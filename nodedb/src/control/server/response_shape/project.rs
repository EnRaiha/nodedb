// SPDX-License-Identifier: BUSL-1.1

//! Pure JSON/plan projection and flattening helpers for SELECT responses.
//!
//! These operate purely on parsed SQL and `serde_json::Value` — no pgwire
//! wire types — so they are shared across any protocol-specific response
//! shaper. Protocol-specific encode glue that turns these into wire rows
//! (e.g. pgwire's `DataRow`) lives in each protocol's own handler code.

/// Convert a JSON scalar value to its PostgreSQL text-format string.
///
/// - `String` values are returned as-is (no extra quoting).
/// - `Bool` uses PostgreSQL text format: `t` for true, `f` for false.
/// - All other scalars (`Number`, `Array`, `Object`) use their JSON
///   `Display` representation; arrays/objects should not normally appear
///   as individual cell values but are rendered faithfully.
pub fn json_value_to_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        // PostgreSQL text format for boolean is `t`/`f`.
        serde_json::Value::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        other => other.to_string(),
    }
}

/// Flatten a parsed JSON value into row objects.
pub fn push_flat_rows(
    value: serde_json::Value,
    out: &mut Vec<serde_json::Map<String, serde_json::Value>>,
) {
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                push_flat_rows(item, out);
            }
        }
        serde_json::Value::Object(mut map) => {
            if is_scan_wrapper(&map)
                && let Some(serde_json::Value::Object(inner)) = map.remove("data")
            {
                out.push(inner);
                return;
            }
            out.push(map);
        }
        _ => {}
    }
}

/// The Data Plane's raw document-scan codec emits objects with exactly
/// the keys `id` (string) and `data` (object). This is the wire shape
/// we unwrap before column projection.
pub fn is_scan_wrapper(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.len() == 2
        && matches!(map.get("id"), Some(serde_json::Value::String(_)))
        && matches!(map.get("data"), Some(serde_json::Value::Object(_)))
}
