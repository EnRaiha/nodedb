// SPDX-License-Identifier: BUSL-1.1

//! NDJSON and JSON array import for `COPY FROM`.
//!
//! Relocated verbatim from the pgwire `ddl::collection::copy_from::json_import`
//! module (now deleted). `plan_and_dispatch` returns the protocol-neutral
//! [`DdlError`] directly (it is the neutral collection-DML helper), so this
//! module's own file-read/parse errors are built as `DdlError` at their call
//! sites to keep one error type end to end.

use crate::control::server::shared::ddl::neutral::collection::dml::{
    fields_to_insert_sql, plan_and_dispatch,
};
use crate::control::server::shared::ddl::result::DdlError;

use super::entry::wrap_row_error;
use super::import_ctx::ImportCtx;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Import from an NDJSON file (one JSON object per non-empty line).
///
/// Parse-then-insert: all rows are parsed before any INSERT is issued so that
/// a JSON parse error on any line prevents rows from being partially written.
pub(super) async fn import_ndjson(
    ctx: &ImportCtx<'_>,
    collection: &str,
    path: &str,
) -> Result<usize, DdlError> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| ddl_err("58030", format!("COPY: cannot read '{path}': {e}")))?;

    let content = std::str::from_utf8(&bytes).map_err(|e| {
        ddl_err(
            "22021",
            format!("COPY: file '{path}' is not valid UTF-8: {e}"),
        )
    })?;

    // Parse phase: collect all rows first.
    let mut parsed: Vec<(
        usize,
        std::collections::HashMap<String, nodedb_types::Value>,
    )> = Vec::new();
    for (line_idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let line_no = line_idx + 1;
        let fields = json_object_to_fields(line, line_no)?;
        parsed.push((line_no, fields));
    }

    // Insert phase: issue all INSERTs.
    for (line_no, fields) in &parsed {
        let sql = fields_to_insert_sql(collection, fields);
        plan_and_dispatch(
            ctx.state,
            ctx.identity,
            ctx.tenant_id,
            ctx.database_id,
            &sql,
            ctx.txn_ctx,
        )
        .await
        .map_err(|e| wrap_row_error(e, *line_no, "NDJSON"))?;
    }

    Ok(parsed.len())
}

/// Import from a JSON array file (`[{...}, {...}, ...]`).
///
/// Parse-then-insert: all rows are deserialized before any INSERT so that
/// a malformed array prevents partial writes.
pub(super) async fn import_json_array(
    ctx: &ImportCtx<'_>,
    collection: &str,
    path: &str,
) -> Result<usize, DdlError> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| ddl_err("58030", format!("COPY: cannot read '{path}': {e}")))?;

    let array: Vec<serde_json::Value> = sonic_rs::from_slice(&bytes).map_err(|e| {
        ddl_err(
            "22P02",
            format!("COPY: file '{path}' is not a valid JSON array: {e}"),
        )
    })?;

    // Parse phase: validate all elements are objects.
    let mut parsed: Vec<std::collections::HashMap<String, nodedb_types::Value>> =
        Vec::with_capacity(array.len());
    for (idx, elem) in array.iter().enumerate() {
        let line_no = idx + 1;
        let obj = elem.as_object().ok_or_else(|| {
            ddl_err(
                "22P02",
                format!("COPY: row {line_no} in '{path}' is not a JSON object"),
            )
        })?;
        let mut fields = std::collections::HashMap::new();
        for (key, val) in obj.iter() {
            fields.insert(key.clone(), serde_json_value_to_nodedb(val));
        }
        parsed.push(fields);
    }

    // Insert phase.
    for (idx, fields) in parsed.iter().enumerate() {
        let line_no = idx + 1;
        let sql = fields_to_insert_sql(collection, fields);
        plan_and_dispatch(
            ctx.state,
            ctx.identity,
            ctx.tenant_id,
            ctx.database_id,
            &sql,
            ctx.txn_ctx,
        )
        .await
        .map_err(|e| wrap_row_error(e, line_no, "JSON array"))?;
    }

    Ok(parsed.len())
}

/// Parse a JSON object string into a field map.
fn json_object_to_fields(
    json: &str,
    line_no: usize,
) -> Result<std::collections::HashMap<String, nodedb_types::Value>, DdlError> {
    let val: serde_json::Value = sonic_rs::from_str(json)
        .map_err(|e| ddl_err("22P02", format!("COPY: line {line_no}: invalid JSON: {e}")))?;

    let obj = val.as_object().ok_or_else(|| {
        ddl_err(
            "22P02",
            format!("COPY: line {line_no}: expected JSON object, got other type"),
        )
    })?;

    let mut fields = std::collections::HashMap::new();
    for (key, v) in obj.iter() {
        fields.insert(key.clone(), serde_json_value_to_nodedb(v));
    }
    Ok(fields)
}

/// Convert a `serde_json::Value` to `nodedb_types::Value`.
pub(super) fn serde_json_value_to_nodedb(val: &serde_json::Value) -> nodedb_types::Value {
    match val {
        serde_json::Value::Null => nodedb_types::Value::Null,
        serde_json::Value::Bool(b) => nodedb_types::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                nodedb_types::Value::Integer(i)
            } else if let Some(u) = n.as_u64() {
                nodedb_types::Value::Integer(u as i64)
            } else if let Some(f) = n.as_f64() {
                nodedb_types::Value::Float(f)
            } else {
                nodedb_types::Value::Null
            }
        }
        serde_json::Value::String(s) => nodedb_types::Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            nodedb_types::Value::Array(arr.iter().map(serde_json_value_to_nodedb).collect())
        }
        serde_json::Value::Object(_) => {
            // Nested object: serialize back to JSON string for storage.
            let s = sonic_rs::to_string(val).unwrap_or_else(|_| "{}".to_string());
            nodedb_types::Value::String(s)
        }
    }
}
