// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the protocol-neutral query-function handlers.
//!
//! Fallible helpers yield a protocol-neutral [`DdlError`] (SQLSTATE +
//! message), and the single-column `result` output builds a
//! [`DdlResult::Rows`] directly.

use nodedb_types::Value;
use serde_json::{Map, Value as JsonValue};

use crate::control::server::response_shape::cell::row_to_wire_json;
use crate::control::server::response_shape::project::push_flat_rows;
use crate::control::server::response_shape::types::ShapedRows;

use super::super::super::result::{DdlError, DdlResult};

/// Construct a protocol-neutral DDL error (SQLSTATE + message).
pub fn err(sqlstate: &str, message: &str) -> DdlError {
    DdlError::new(sqlstate, message)
}

pub fn parse_timestamp_secs(s: &str) -> Result<u64, DdlError> {
    if let Ok(n) = s.parse::<u64>() {
        return Ok(n);
    }
    if let Some(dt) = nodedb_types::NdbDateTime::parse(s) {
        return Ok((dt.micros / 1_000_000) as u64);
    }
    Err(err("22007", &format!("cannot parse '{s}' as timestamp")))
}

/// Convert a JSON value to Decimal. Returns `None` for non-numeric values.
pub fn json_to_decimal(v: &serde_json::Value) -> Option<rust_decimal::Decimal> {
    if let Some(i) = v.as_i64() {
        Some(rust_decimal::Decimal::from(i))
    } else if let Some(f) = v.as_f64() {
        rust_decimal::Decimal::try_from(f).ok()
    } else if let Some(s) = v.as_str() {
        s.parse().ok()
    } else {
        None
    }
}

/// Build the single-row `result` output carrying `value`.
///
/// The output has one text column named `result` with one row holding
/// `value`.
pub fn single_result(value: &str) -> Vec<DdlResult> {
    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String(value.to_string()));
    vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["result".to_string()],
        vec![row],
    ))]
}

/// Unwrap the `DocumentOp::Scan` raw-passthrough envelope (`{"id": ..,
/// "data": {..fields..}}`) off each decoded row, so callers match and read
/// stored fields directly rather than against the wire wrapper.
///
/// Reuses `response_shape::project::push_flat_rows` — the same unwrap the
/// pgwire/HTTP row shaper applies — so there is exactly one definition of
/// "unwrap a scan envelope" in the tree. Each JSON document is lifted to a
/// typed value for the unwrap and its rows rendered back to JSON for the
/// callers, which read fields as JSON. Rows that are not `{id, data}`
/// wrapped (already-flat producers) pass through unchanged.
///
/// The callers read a row's identity under `id`, so a row that lacks `id`
/// gains it here. These rows feed the query functions only and never reach
/// a client as a result set.
pub fn unwrap_scan_docs(docs: Vec<JsonValue>) -> Result<Vec<Map<String, JsonValue>>, DdlError> {
    let mut rows = Vec::with_capacity(docs.len());
    for doc in docs {
        push_flat_rows(
            Value::from(doc),
            nodedb_types::DEFAULT_IDENTITY_COLUMN,
            &mut rows,
        )
        .map_err(|e| DdlError::from_error(&e))?;
    }
    Ok(rows.iter().map(row_to_wire_json).collect())
}

/// Build a zero-row `result` output.
///
/// The output has one text column named `result` and no rows.
pub fn empty_result() -> Vec<DdlResult> {
    vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["result".to_string()],
        Vec::new(),
    ))]
}
