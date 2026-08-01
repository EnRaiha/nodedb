// SPDX-License-Identifier: BUSL-1.1

//! Entry point: `copy_from_file`, path validation, and engine-support check.
//!
//! Relocated verbatim from the pgwire `ddl::collection::copy_from::entry`
//! module (now deleted) except for the result type, which is [`DdlResult`] /
//! [`DdlError`] throughout instead of pgwire `Response` / `PgWireResult`. Each
//! error site builds a `DdlError` directly via [`ddl_err`] so the whole call
//! chain speaks one error type.

use nodedb_types::DatabaseId;
use std::path::Path;

use nodedb_sql::ddl_ast::statement::CopyFormat;
use nodedb_types::CollectionType;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::neutral::collection::dml::authorize_write_target;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;

use super::csv_import::{CsvOptions, import_csv};
use super::import_ctx::ImportCtx;
use super::json_import::{import_json_array, import_ndjson};

/// Maximum file size accepted for COPY FROM (16 GiB).
pub(super) const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// COPY FROM format and delimiter options.
#[derive(Clone, Copy, Debug)]
pub struct CopyFromOptions<'a> {
    pub format: Option<&'a CopyFormat>,
    pub delimiter: Option<char>,
    pub header: bool,
}

/// Execute `COPY <collection> FROM '<path>' [WITH (...)]`.
pub async fn copy_from_file(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
    path: &str,
    options: CopyFromOptions<'_>,
    database_id: DatabaseId,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CopyFromOptions {
        format,
        delimiter,
        header,
    } = options;

    authorize_write_target(state, identity, database_id, collection)?;
    validate_path(path)?;

    // Check file size before reading.
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|e| ddl_err("58030", format!("COPY: cannot stat file '{path}': {e}")))?;
    if metadata.len() > MAX_FILE_BYTES {
        return Err(ddl_err(
            "54000",
            format!(
                "COPY: file '{path}' is {} bytes, exceeds limit of {} bytes",
                metadata.len(),
                MAX_FILE_BYTES
            ),
        ));
    }

    // Determine format (caller has already auto-detected from extension; this is a safety net).
    let resolved_format = format.ok_or_else(|| {
        ddl_err(
            "42601",
            format!(
                "COPY: cannot infer format for '{path}'; \
                 add WITH (FORMAT ndjson|json|csv)"
            ),
        )
    })?;

    // Validate engine: reject Timeseries and Spatial.
    check_engine_support(state, identity, database_id, collection)?;

    let tenant_id = identity.tenant_id;

    let import_ctx = ImportCtx {
        state,
        identity,
        tenant_id,
        database_id,
        txn_ctx,
    };

    let row_count = match resolved_format {
        CopyFormat::Ndjson => import_ndjson(&import_ctx, collection, path).await?,
        CopyFormat::JsonArray => import_json_array(&import_ctx, collection, path).await?,
        CopyFormat::Csv => {
            import_csv(
                &import_ctx,
                collection,
                path,
                CsvOptions {
                    delimiter: delimiter.unwrap_or(','),
                    has_header: header,
                },
            )
            .await?
        }
    };

    Ok(vec![DdlResult::Status {
        command: format!("COPY {row_count}"),
        rows_affected: None,
    }])
}

/// Reject paths with `..` segments and non-absolute paths.
fn validate_path(path: &str) -> Result<(), DdlError> {
    if !path.starts_with('/') {
        return Err(ddl_err(
            "42601",
            format!(
                "COPY: path '{path}' is not absolute; \
                 only absolute server-side paths are accepted"
            ),
        ));
    }
    let p = Path::new(path);
    for component in p.components() {
        use std::path::Component;
        if matches!(component, Component::ParentDir) {
            return Err(ddl_err(
                "42501",
                format!(
                    "COPY: path '{path}' contains '..'; \
                     directory traversal is not permitted"
                ),
            ));
        }
    }
    Ok(())
}

/// Verify the collection engine supports COPY FROM.
fn check_engine_support(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
    let tenant_id = identity.tenant_id;
    let catalog = state.credentials.catalog();
    let stored = match catalog.get_collection(database_id, tenant_id.as_u64(), collection) {
        Ok(Some(c)) => c,
        Ok(None) => return Ok(()), // Collection doesn't exist yet — will fail at INSERT.
        Err(e) => {
            return Err(ddl_err(
                "XX000",
                format!("COPY: catalog lookup failed: {e}"),
            ));
        }
    };

    match &stored.collection_type {
        CollectionType::Columnar(profile) => {
            use nodedb_types::ColumnarProfile;
            match profile {
                ColumnarProfile::Plain => Ok(()),
                ColumnarProfile::Timeseries { .. } => Err(ddl_err(
                    "0A000",
                    format!(
                        "COPY: collection '{collection}' uses the timeseries engine; \
                         use ILP or INSERT with explicit time column instead"
                    ),
                )),
                ColumnarProfile::Spatial { .. } => Err(ddl_err(
                    "0A000",
                    format!(
                        "COPY: collection '{collection}' uses the spatial engine; \
                         use INSERT with a WKT/GeoJSON geometry column instead"
                    ),
                )),
            }
        }
        CollectionType::Document(_) => Ok(()),
        CollectionType::KeyValue(_) => Ok(()),
    }
}

/// Wrap a row-level error to include the row number in the message.
///
/// Row-level import helpers (`import_csv`, `import_ndjson`, `import_json_array`)
/// call `plan_and_dispatch`, which returns a protocol-neutral [`DdlError`] (not
/// a pgwire `PgWireError`), so this wraps the same type — only the message is
/// decorated with the row number, matching the original pgwire behavior.
pub(super) fn wrap_row_error(e: DdlError, line_no: usize, fmt: &str) -> DdlError {
    DdlError {
        sqlstate: e.sqlstate,
        message: format!("COPY: {fmt} row {line_no}: {}", e.message),
    }
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}
