// SPDX-License-Identifier: BUSL-1.1

//! KV point-get / batch-get response shaping: inject the primary key(s)
//! into the stored value(s) before the protocol layer turns them into
//! SQL rows.

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::response_codec::decode_payload_to_json;
use nodedb_physical::physical_plan::KvOp;
use nodedb_query::msgpack_scan;

/// When `plan` is a KV point-get or batch-get, turn the engine's stored
/// bytes into row-shaped msgpack.
///
/// The single-key rule is `msgpack_scan::kv_row_msgpack` — the same function
/// the Data Plane's scan and `RETURNING` paths call, deliberately not a
/// second copy here. Two copies of it existed once and diverged, and every
/// scan path served `value: 118` for a stored `"v1"` until they were merged.
///
/// `KvOp::BatchGet` gets its own arm: `execute_kv_batch_get` (Data Plane)
/// emits a bare msgpack array of per-key results (base64 `value` string,
/// or `null` for a missing key) positionally parallel to the plan's
/// `keys` list. That array of scalars has no `key` attached, so the
/// generic row-flattener (`push_flat_rows`) would silently drop every
/// scalar element (its catch-all only forwards objects/arrays). Zip the
/// results with `keys` here and wrap each pair into the same `{key,
/// value}` row shape the single-key `Get` arm above produces, with a
/// missing key represented as `value: null` (matching how
/// `execute_kv_batch_get` already encodes a miss).
///
/// For every other plan, return the payload unchanged.
pub fn apply_kv_wrap(plan: &PhysicalPlan, payload: &[u8]) -> Vec<u8> {
    if payload.is_empty() {
        return payload.to_vec();
    }
    match plan {
        PhysicalPlan::Kv(KvOp::Get { key, .. }) => {
            msgpack_scan::kv_row_msgpack(&String::from_utf8_lossy(key), payload)
        }
        PhysicalPlan::Kv(KvOp::BatchGet { keys, .. }) => wrap_batch_get(keys, payload),
        _ => payload.to_vec(),
    }
}

/// Zip `KvOp::BatchGet`'s `keys` with the Data Plane's positional
/// `[value_or_null, ...]` array and wrap each pair into a `{key, value}`
/// row, msgpack-encoded so the rest of the shaping pipeline
/// (`decode_payload_to_json` -> `push_flat_rows`) treats it exactly like
/// any other row-array payload.
///
/// Falls back to the raw payload (rather than panicking) if the Data
/// Plane payload is not the expected JSON/msgpack array — a malformed
/// upstream payload degrades to the pre-fix (empty-looking) shape instead
/// of taking down the connection.
fn wrap_batch_get(keys: &[Vec<u8>], payload: &[u8]) -> Vec<u8> {
    let decoded = decode_payload_to_json(payload);
    let Ok(JsonValue::Array(values)) = sonic_rs::from_str::<JsonValue>(&decoded) else {
        return payload.to_vec();
    };

    let rows: Vec<JsonValue> = keys
        .iter()
        .zip(values)
        .map(|(key, value)| {
            let mut row = Map::new();
            row.insert(
                "key".to_string(),
                JsonValue::String(String::from_utf8_lossy(key).into_owned()),
            );
            row.insert("value".to_string(), value);
            JsonValue::Object(row)
        })
        .collect();

    nodedb_types::json_to_msgpack(&JsonValue::Array(rows)).unwrap_or_else(|_| payload.to_vec())
}
