// SPDX-License-Identifier: BUSL-1.1

//! KV-specific command-tag derivation for staged in-transaction KV point
//! writes, split out of `plan.rs` to keep it under the file-size limit.
//!
//! `InsertOnConflictUpdate` is the one staged KV op whose tag (INSERT vs
//! UPDATE) cannot be decided from the plan shape alone -- the stage handler
//! resolves the current value against BASE ∪ OVERLAY and signals the
//! outcome back via an `"op"` field in the response payload (`"insert"` or
//! `"update"`), alongside the usual `"affected"` row count.

use pgwire::api::results::Tag;

use nodedb_physical::physical_plan::KvOp;

/// Extract the `"op"` field a staged `InsertOnConflictUpdate` response
/// payload carries (`"insert"` or `"update"`). `None` for any other payload
/// shape (including every other staged KV write, which carries no `"op"`
/// field at all).
pub(super) fn extract_kv_conflict_op(payload: &[u8]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    let v: serde_json::Value = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice(payload).ok())?;
    v.get("op").and_then(|n| n.as_str()).map(str::to_string)
}

/// Synthesise the `CommandComplete` tag for a staged `KvOp` write.
///
/// Caller invariant: `op` must be one of the five stageable KV writes --
/// `Put`, `Insert`, `InsertIfAbsent`, `InsertOnConflictUpdate`, `Delete` --
/// i.e. the enclosing plan already passed `is_stageable_write`. Every other
/// `KvOp` variant is unreachable here because the staging dispatch never
/// routes them through this path.
pub(super) fn kv_write_tag(op: &KvOp, rows: usize, payload: &[u8]) -> Tag {
    match op {
        KvOp::Put { .. } | KvOp::Insert { .. } | KvOp::InsertIfAbsent { .. } => {
            Tag::new("INSERT").with_rows(rows)
        }
        KvOp::InsertOnConflictUpdate { .. } => {
            if extract_kv_conflict_op(payload).as_deref() == Some("update") {
                Tag::new("UPDATE").with_rows(rows)
            } else {
                Tag::new("INSERT").with_rows(rows)
            }
        }
        KvOp::Delete { .. } => Tag::new("DELETE").with_rows(rows),
        KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::Expire { .. }
        | KvOp::Persist { .. }
        | KvOp::BatchGet { .. }
        | KvOp::BatchPut { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldGet { .. }
        | KvOp::FieldSet { .. }
        | KvOp::GetTtl { .. }
        | KvOp::Truncate { .. }
        | KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::Transfer { .. }
        | KvOp::TransferItem { .. }
        | KvOp::MaterializeScan { .. } => unreachable!(
            "kv_write_tag called on a non-stageable KvOp; \
             is_stageable_write invariant broken: {op:?}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_kv_conflict_op_reads_op_field() {
        let payload =
            nodedb_types::json_to_msgpack(&serde_json::json!({"affected": 1, "op": "update"}))
                .unwrap();
        assert_eq!(extract_kv_conflict_op(&payload).as_deref(), Some("update"));
    }

    #[test]
    fn extract_kv_conflict_op_none_when_absent() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({"affected": 1})).unwrap();
        assert_eq!(extract_kv_conflict_op(&payload), None);
    }
}
