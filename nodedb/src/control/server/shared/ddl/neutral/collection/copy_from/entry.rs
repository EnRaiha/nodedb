// SPDX-License-Identifier: BUSL-1.1

//! Entry point: `copy_from_file`, path validation, and engine-support check.
//!
//! Relocated verbatim from the pgwire `ddl::collection::copy_from::entry`
//! module (now deleted) except for the result type, which is [`DdlResult`] /
//! [`DdlError`] throughout instead of pgwire `Response` / `PgWireResult`. The
//! still-imported `pgwire::types::sqlstate_error` builds a `PgWireError` at
//! each error site (shared infra, unchanged), converted immediately to
//! `DdlError` via [`ddl_err`] so the whole call chain speaks one error type.

use nodedb_types::DatabaseId;
use std::path::Path;

use nodedb_sql::ddl_ast::statement::CopyFormat;
use nodedb_types::CollectionType;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::csv_import::{CsvOptions, import_csv};
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
) -> Result<Vec<DdlResult>, DdlError> {
    let CopyFromOptions {
        format,
        delimiter,
        header,
    } = options;
    validate_path(path)?;

    // Check file size before reading.
    let metadata = tokio::fs::metadata(path).await.map_err(|e| {
        ddl_err(sqlstate_error(
            "58030",
            &format!("COPY: cannot stat file '{path}': {e}"),
        ))
    })?;
    if metadata.len() > MAX_FILE_BYTES {
        return Err(ddl_err(sqlstate_error(
            "54000",
            &format!(
                "COPY: file '{path}' is {} bytes, exceeds limit of {} bytes",
                metadata.len(),
                MAX_FILE_BYTES
            ),
        )));
    }

    // Determine format (caller has already auto-detected from extension; this is a safety net).
    let resolved_format = format.ok_or_else(|| {
        ddl_err(sqlstate_error(
            "42601",
            &format!(
                "COPY: cannot infer format for '{path}'; \
                 add WITH (FORMAT ndjson|json|csv)"
            ),
        ))
    })?;

    // Validate engine: reject Timeseries and Spatial.
    check_engine_support(state, identity, collection)?;

    let tenant_id = identity.tenant_id;

    let row_count = match resolved_format {
        CopyFormat::Ndjson => {
            import_ndjson(state, identity, tenant_id, collection, path, database_id).await?
        }
        CopyFormat::JsonArray => {
            import_json_array(state, identity, tenant_id, collection, path, database_id).await?
        }
        CopyFormat::Csv => {
            import_csv(
                state,
                identity,
                tenant_id,
                collection,
                path,
                CsvOptions {
                    delimiter: delimiter.unwrap_or(','),
                    has_header: header,
                },
                database_id,
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
        return Err(ddl_err(sqlstate_error(
            "42601",
            &format!(
                "COPY: path '{path}' is not absolute; \
                 only absolute server-side paths are accepted"
            ),
        )));
    }
    let p = Path::new(path);
    for component in p.components() {
        use std::path::Component;
        if matches!(component, Component::ParentDir) {
            return Err(ddl_err(sqlstate_error(
                "42501",
                &format!(
                    "COPY: path '{path}' contains '..'; \
                     directory traversal is not permitted"
                ),
            )));
        }
    }
    Ok(())
}

/// Verify the collection engine supports COPY FROM.
fn check_engine_support(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
) -> Result<(), DdlError> {
    let tenant_id = identity.tenant_id;
    let catalog = match state.credentials.catalog() {
        Some(c) => c,
        None => return Ok(()), // No catalog means schemaless fallback — allow.
    };
    let stored = match catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), collection) {
        Ok(Some(c)) => c,
        Ok(None) => return Ok(()), // Collection doesn't exist yet — will fail at INSERT.
        Err(e) => {
            return Err(ddl_err(sqlstate_error(
                "XX000",
                &format!("COPY: catalog lookup failed: {e}"),
            )));
        }
    };

    match &stored.collection_type {
        CollectionType::Columnar(profile) => {
            use nodedb_types::ColumnarProfile;
            match profile {
                ColumnarProfile::Plain => Ok(()),
                ColumnarProfile::Timeseries { .. } => Err(ddl_err(sqlstate_error(
                    "0A000",
                    &format!(
                        "COPY: collection '{collection}' uses the timeseries engine; \
                         use ILP or INSERT with explicit time column instead"
                    ),
                ))),
                ColumnarProfile::Spatial { .. } => Err(ddl_err(sqlstate_error(
                    "0A000",
                    &format!(
                        "COPY: collection '{collection}' uses the spatial engine; \
                         use INSERT with a WKT/GeoJSON geometry column instead"
                    ),
                ))),
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

/// Convert a pgwire error (from the still-imported `sqlstate_error` helper)
/// into a protocol-neutral [`DdlError`].
fn ddl_err(err: pgwire::error::PgWireError) -> DdlError {
    match err {
        pgwire::error::PgWireError::UserError(info) => DdlError {
            sqlstate: info.code.clone(),
            message: info.message.clone(),
        },
        other => DdlError {
            sqlstate: "XX000".to_string(),
            message: other.to_string(),
        },
    }
}
