// SPDX-License-Identifier: BUSL-1.1

//! Shared conversion helpers for native protocol dispatch.

use nodedb_types::Value;
use nodedb_types::conversion::json_to_value_display;
use nodedb_types::protocol::NativeResponse;

use crate::control::server::response_shape::types::ShapedRows;

/// Convert a crate-level error into a NativeResponse.
pub(crate) fn error_to_native(seq: u64, e: &crate::Error) -> NativeResponse {
    let (code, message) = match e {
        crate::Error::BadRequest { detail } => ("42601", detail.clone()),
        crate::Error::RejectedAuthz { resource, .. } => ("42501", resource.clone()),
        crate::Error::DeadlineExceeded { .. } => ("57014", "query cancelled due to timeout".into()),
        crate::Error::CollectionNotFound { collection, .. } => {
            ("42P01", format!("collection '{collection}' not found"))
        }
        other => ("XX000", format!("{other}")),
    };
    NativeResponse::error(seq, code, message)
}

/// Convert a `NodeDbError` produced while shaping a response into a
/// NativeResponse error frame.
pub(crate) fn shape_error_to_native(seq: u64, e: &nodedb_types::NodeDbError) -> NativeResponse {
    NativeResponse::error(seq, "XX000", e.message().to_string())
}

/// Convert protocol-neutral `ShapedRows` (produced by
/// `response_shape::compose::shape_response_materialized`) into native wire
/// columns/rows: each JSON scalar cell becomes a typed `Value` via
/// `json_to_value_display`; a column absent from a given row's map becomes
/// `Value::Null`.
pub(crate) fn to_native_columns_rows(shaped: &ShapedRows) -> (Vec<String>, Vec<Vec<Value>>) {
    let rows = shaped
        .rows
        .iter()
        .map(|row| {
            shaped
                .columns
                .iter()
                .map(|col| {
                    row.get(col.as_str())
                        .map(json_to_value_display)
                        .unwrap_or(Value::Null)
                })
                .collect()
        })
        .collect();
    (shaped.columns.clone(), rows)
}
