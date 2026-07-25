// SPDX-License-Identifier: BUSL-1.1

//! Entry point: `copy_to_file`, path validation, scan, and atomic file write.
//!
//! Relocated verbatim from the pgwire `ddl::collection::copy_to::entry`
//! module (now deleted) except for the result type, which is [`DdlResult`] /
//! [`DdlError`] throughout instead of pgwire `Response` / `PgWireResult`.

use nodedb_types::DatabaseId;
use std::path::Path;

use sonic_rs;

use nodedb_sql::ddl_ast::statement::{CopyFormat, CopyToSource};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::format::serialize_rows;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Execute `COPY <source> TO '<path>' [WITH (...)]`.
pub async fn copy_to_file(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    source: &CopyToSource,
    path: &str,
    format: Option<&CopyFormat>,
    delimiter: Option<char>,
    header: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    validate_path(path)?;

    // Resolve format (caller has already auto-detected from extension).
    let resolved_format = format.ok_or_else(|| {
        ddl_err(
            "42601",
            format!(
                "COPY TO: cannot infer format for '{path}'; \
                 add WITH (FORMAT ndjson|json|csv)"
            ),
        )
    })?;

    // Build the SELECT SQL from the source.
    let select_sql = build_select_sql(source)?;

    // Validate collection existence (for table-form sources) and engine support.
    if let CopyToSource::Collection(coll) = source {
        check_collection_exists(state, identity, coll)?;
    }

    // Execute the query and collect all JSON rows.
    let rows = execute_and_collect(state, identity, &select_sql).await?;

    // Serialize to the requested format.
    let bytes = serialize_rows(&rows, resolved_format, delimiter.unwrap_or(','), header)?;

    // Atomic write: temp file → rename.
    let tmp_path = format!("{path}.tmp");
    tokio::fs::write(&tmp_path, &bytes).await.map_err(|e| {
        ddl_err(
            "58030",
            format!("COPY TO: cannot write to '{tmp_path}': {e}"),
        )
    })?;
    tokio::fs::rename(&tmp_path, path).await.map_err(|e| {
        // Clean up the temp file on rename failure.
        let _ = std::fs::remove_file(&tmp_path);
        ddl_err(
            "58030",
            format!("COPY TO: cannot rename '{tmp_path}' to '{path}': {e}"),
        )
    })?;

    let row_count = rows.len();
    Ok(vec![DdlResult::Status {
        command: format!("COPY {row_count}"),
        rows_affected: None,
    }])
}

/// Build a SELECT SQL string from the source.
fn build_select_sql(source: &CopyToSource) -> Result<String, DdlError> {
    match source {
        CopyToSource::Collection(coll) => Ok(format!(
            "SELECT * FROM {}",
            ::nodedb_types::quote_ident(coll)
        )),
        CopyToSource::Query(q) => Ok(q.clone()),
    }
}

/// Verify the named collection exists.
fn check_collection_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    match catalog.get_collection(DatabaseId::DEFAULT, identity.tenant_id.as_u64(), collection) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(ddl_err(
            "42P01",
            format!("COPY TO: collection \"{collection}\" does not exist"),
        )),
        Err(e) => Err(ddl_err(
            "XX000",
            format!("COPY TO: catalog lookup failed: {e}"),
        )),
    }
}

/// Execute the SELECT SQL and collect the results as `serde_json::Value` rows.
async fn execute_and_collect(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    select_sql: &str,
) -> Result<Vec<serde_json::Value>, DdlError> {
    let (tasks, _output_schema) =
        crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql(
            state,
            identity,
            select_sql,
            DatabaseId::DEFAULT,
        )
        .await
        .map_err(|error| DdlError {
            sqlstate: error.sqlstate,
            message: format!("COPY TO: {}", error.message),
        })?;

    let mut all_rows: Vec<serde_json::Value> = Vec::new();

    for task in tasks.into_tasks() {
        let resp = crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
            state,
            task,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| ddl_err("XX000", format!("COPY TO: dispatch failed: {e}")))?;

        if resp.payload.is_empty() {
            continue;
        }

        let json = crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
        extract_json_rows(&json, &mut all_rows)?;
    }

    Ok(all_rows)
}

/// Parse a JSON string (array or single object) and append rows to `out`.
fn extract_json_rows(json: &str, out: &mut Vec<serde_json::Value>) -> Result<(), DdlError> {
    if json.is_empty() {
        return Ok(());
    }
    let parsed: serde_json::Value = sonic_rs::from_str(json).map_err(|e| {
        ddl_err(
            "XX000",
            format!("COPY TO: failed to decode result rows: {e}"),
        )
    })?;
    match parsed {
        serde_json::Value::Array(items) => {
            out.extend(items);
        }
        obj @ serde_json::Value::Object(_) => {
            out.push(obj);
        }
        _ => {} // Scalar or null result — skip.
    }
    Ok(())
}

/// Reject paths with `..` segments and non-absolute paths.
fn validate_path(path: &str) -> Result<(), DdlError> {
    if !path.starts_with('/') {
        return Err(ddl_err(
            "42601",
            format!(
                "COPY TO: path '{path}' is not absolute; \
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
                    "COPY TO: path '{path}' contains '..'; \
                     directory traversal is not permitted"
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_collection_scan_quotes_identifier() {
        let sql = build_select_sql(&CopyToSource::Collection(
            "orders\"; DELETE FROM audit_log; --".to_string(),
        ))
        .expect("collection scan SQL builds");

        assert_eq!(
            sql,
            "SELECT * FROM \"orders\"\"; DELETE FROM audit_log; --\""
        );
    }

    #[test]
    fn query_source_is_not_reconstructed() {
        let query = "SELECT * FROM safe_source WHERE note = 'literal'".to_string();
        assert_eq!(
            build_select_sql(&CopyToSource::Query(query.clone())).expect("query SQL builds"),
            query
        );
    }
}
