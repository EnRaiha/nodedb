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
/// bytes into row-shaped msgpack. Storage is shape-neutral by design: the
/// two legal single-value shapes are structurally disjoint via their
/// msgpack type byte, so the wrap rule is a tagged union, not a fallback.
///
/// - **Msgpack map** (first byte in `0x80..=0x8f` / `0xde` / `0xdf`) —
///   typed KV entry (`CREATE ... (key ..., col1 ..., col2 ...)`). Inject
///   the primary key under `key` and pass through.
/// - **Anything else** — single-`value` form (from `INSERT ... VALUES
///   (key, value)` or RESP `SET`). Wrap as `{key: <key>, value: <bytes>}`.
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
        PhysicalPlan::Kv(KvOp::Get { key, .. }) => wrap_single_get(key, payload),
        PhysicalPlan::Kv(KvOp::BatchGet { keys, .. }) => wrap_batch_get(keys, payload),
        _ => payload.to_vec(),
    }
}

/// Wrap a single-key `Get` payload into a `{key, value}` (or injected
/// `key`-into-map) row. Extracted verbatim from the pre-`BatchGet` version
/// of `apply_kv_wrap` so both callers share one wrapping helper.
fn wrap_single_get(key: &[u8], payload: &[u8]) -> Vec<u8> {
    let key_str = String::from_utf8_lossy(key);
    if msgpack_scan::map_header(payload, 0).is_some() {
        msgpack_scan::inject_str_field(payload, "key", &key_str)
    } else {
        let mut buf = Vec::with_capacity(payload.len() + key_str.len() + 16);
        msgpack_scan::write_map_header(&mut buf, 2);
        msgpack_scan::write_str(&mut buf, "key");
        msgpack_scan::write_str(&mut buf, &key_str);
        msgpack_scan::write_str(&mut buf, "value");
        // `write_str` takes `&str` — for arbitrary bytes coming from
        // raw-value storage (possibly non-UTF-8 for RESP SET writes),
        // take the lossy UTF-8 view. SQL SELECT on RESP-written binary
        // values is already degraded by the pgwire text protocol; this
        // keeps the representation well-formed msgpack.
        msgpack_scan::write_str(&mut buf, &String::from_utf8_lossy(payload));
        buf
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
