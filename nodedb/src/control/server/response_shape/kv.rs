// SPDX-License-Identifier: BUSL-1.1

//! KV point-get response shaping: inject the primary key into the
//! stored value map before the protocol layer turns it into a SQL row.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::KvOp;
use nodedb_query::msgpack_scan;

/// When `plan` is a KV point-get, turn the engine's stored bytes into
/// a row-shaped msgpack map. Storage is shape-neutral by design: the
/// two legal shapes are structurally disjoint via their msgpack type
/// byte, so the wrap rule is a tagged union, not a fallback.
///
/// - **Msgpack map** (first byte in `0x80..=0x8f` / `0xde` / `0xdf`) —
///   typed KV entry (`CREATE ... (key ..., col1 ..., col2 ...)`). Inject
///   the primary key under `key` and pass through.
/// - **Anything else** — single-`value` form (from `INSERT ... VALUES
///   (key, value)` or RESP `SET`). Wrap as `{key: <key>, value: <bytes>}`.
///
/// For every non-KV-point-get plan, return the payload unchanged.
pub fn apply_kv_wrap(plan: &PhysicalPlan, payload: &[u8]) -> Vec<u8> {
    if payload.is_empty() {
        return payload.to_vec();
    }
    let PhysicalPlan::Kv(KvOp::Get { key, .. }) = plan else {
        return payload.to_vec();
    };
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
