// SPDX-License-Identifier: BUSL-1.1

//! Entry point: `copy_from_file`, path validation, and the engine's bulk-load
//! route, which `EngineRules::plan_bulk_load` answers.
//!
//! The result type is [`DdlResult`] / [`DdlError`] throughout. Each error
//! site builds a `DdlError` directly via [`ddl_err`] so the whole call chain
//! speaks one error type.

use nodedb_types::DatabaseId;
use std::path::Path;
use tokio::io::AsyncReadExt;

use nodedb_sql::SqlError;
use nodedb_sql::ddl_ast::statement::{CopyFormat, CopyStdinFormat};

use crate::control::planner::context::{BulkLoadRoute, QueryContext};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::neutral::collection::dml::authorize_write_target;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;

use crate::control::trigger::statement_txn::{fires_joined_body, in_block, with_statement_txn};
use crate::control::trigger::{DmlEvent, TriggerScope};

use super::array_load::{ArrayLoadCtx, ArrayLoader, LoadInput, TextFormat};
use super::csv_import::{CsvOptions, import_csv};
use super::import_ctx::ImportCtx;
use super::json_import::{import_json_array, import_ndjson};

/// Maximum file size accepted for COPY FROM (16 GiB).
pub(super) const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// How many bytes one read of an array load's file takes.
const FILE_READ_BYTES: usize = 1024 * 1024;

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

    let tenant_id = identity.tenant_id;

    // The engine's rules decide how it loads: row by row, in array cell
    // chunks, or not at all (timeseries, spatial).
    let route = QueryContext::for_state(state)
        .bulk_load_route(collection, tenant_id, database_id)
        .map_err(|e| bulk_load_refusal(e, tenant_id))?;
    if let BulkLoadRoute::ArrayCells(_) = route {
        let ctx = ArrayLoadCtx {
            state,
            identity,
            database_id,
            txn_ctx,
        };
        return copy_array_from_file(&ctx, collection, path, options).await;
    }

    // Each row fires its collection's BEFORE, INSTEAD OF and SYNC AFTER
    // bodies, which join the statement's transaction. Outside a transaction
    // block a COPY into such a collection runs in an implicit one: its rows
    // and the bodies' writes commit together, or none of them do.
    let implicit = !in_block(txn_ctx)
        && fires_joined_body(
            state,
            TriggerScope {
                database_id,
                tenant_id,
            },
            collection,
            DmlEvent::Insert,
        );
    let row_count = with_statement_txn(
        state,
        identity,
        txn_ctx,
        implicit,
        async |txn_ctx: &DmlTxnCtx<'_>| {
            let import_ctx = ImportCtx {
                state,
                identity,
                tenant_id,
                database_id,
                txn_ctx,
            };
            match resolved_format {
                CopyFormat::Ndjson => import_ndjson(&import_ctx, collection, path).await,
                CopyFormat::JsonArray => import_json_array(&import_ctx, collection, path).await,
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
                    .await
                }
            }
        },
    )
    .await?;

    // The count is baked into `command` (not `rows_affected`) because the
    // native and HTTP encoders (`ddl_result_to_native`, `ddl_results_to_json`)
    // read only `command` for a `Status` result and drop `rows_affected` —
    // this is the only shape that surfaces the row count on every protocol.
    // pgwire's `ddl_result_to_response` renders it bare via the `None` arm,
    // which already produces the spec-correct `COPY <rows>` tag text.
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

/// The DDL error of a refused bulk-load route. An engine's refusal keeps its
/// exact text under `0A000`.
pub(super) fn bulk_load_refusal(e: SqlError, tenant_id: nodedb_types::TenantId) -> DdlError {
    match e {
        SqlError::Unsupported { detail } => ddl_err("0A000", detail),
        other => DdlError::from_sql_error(other, tenant_id),
    }
}

/// `COPY <array> FROM '<path>'`: the file's CSV rows stream through the
/// array bulk-load core in reads of [`FILE_READ_BYTES`], as
/// `COPY <array> FROM STDIN` does.
async fn copy_array_from_file(
    ctx: &ArrayLoadCtx<'_>,
    array: &str,
    path: &str,
    options: CopyFromOptions<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CopyFromOptions {
        format,
        delimiter,
        header,
    } = options;
    if format != Some(&CopyFormat::Csv) {
        return Err(ddl_err(
            "0A000",
            format!("COPY: array '{array}' loads CSV rows; use WITH (FORMAT csv)"),
        ));
    }
    let format = TextFormat {
        format: CopyStdinFormat::Csv,
        delimiter,
        header,
    };
    let mut loader = ArrayLoader::open(ctx, array, LoadInput::Text(format))?;
    let read_error =
        |e: std::io::Error| ddl_err("58030", format!("COPY: cannot read '{path}': {e}"));
    let mut file = tokio::fs::File::open(path).await.map_err(read_error)?;
    let mut buf = vec![0u8; FILE_READ_BYTES];
    loop {
        let n = file.read(&mut buf).await.map_err(read_error)?;
        if n == 0 {
            break;
        }
        loader.feed_text(ctx, &buf[..n]).await?;
    }
    let rows = loader.finish(ctx).await?;
    Ok(vec![DdlResult::Status {
        command: format!("COPY {rows}"),
        rows_affected: None,
    }])
}

/// Wrap a row-level error to include the row number in the message.
///
/// Row-level import helpers (`import_csv`, `import_ndjson`, `import_json_array`)
/// call `plan_and_dispatch`, which returns a protocol-neutral [`DdlError`] (not
/// a pgwire `PgWireError`), so this wraps the same type — only the message is
/// decorated with the row number.
pub(super) fn wrap_row_error(e: DdlError, line_no: usize, fmt: &str) -> DdlError {
    DdlError::new(
        e.sqlstate,
        format!("COPY: {fmt} row {line_no}: {}", e.message),
    )
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}
