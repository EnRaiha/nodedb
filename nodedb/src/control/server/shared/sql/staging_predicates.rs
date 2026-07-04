// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral predicates for the in-transaction write-staging gate.
//!
//! Moved verbatim (decision logic only) from the pgwire handler's
//! `plan.rs` / `plan_kv.rs` so the same staging decisions can be reused by
//! any protocol's dispatch loop (pgwire SQL today; native and DSL/UPSERT in
//! later units). No pgwire types are imported here.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{DocumentOp, KvOp};

/// Allow-list of the plans the in-transaction path stages at statement time:
/// point writes, predicate `BulkUpdate` / `BulkDelete` (bulk predicate DML,
/// no RETURNING), and `InsertSelect` (`INSERT ... SELECT`, which has no
/// RETURNING variant so it is always stageable). Explicit match on the exact
/// staged variants — KV point writes and any RETURNING variant stay on the
/// buffer path. Named for the point-write case historically; also covers
/// bulk predicate DML and `InsertSelect` now that `stage_bulk_update` /
/// `stage_bulk_delete` / `stage_insert_select` stage the matched rows the
/// same way.
pub fn is_point_write(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(
            DocumentOp::PointPut { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::PointDelete {
                    returning: None,
                    ..
                }
                | DocumentOp::PointUpdate {
                    returning: None,
                    ..
                }
                | DocumentOp::BulkUpdate {
                    returning: None,
                    ..
                }
                | DocumentOp::BulkDelete {
                    returning: None,
                    ..
                }
                | DocumentOp::InsertSelect { .. }
        )
    )
}

/// Allow-list of plans the in-transaction path stages at statement time via
/// `MetaOp::StageWrite`: everything [`is_point_write`] accepts (Document
/// point writes, predicate `BulkUpdate` / `BulkDelete`, `InsertSelect`),
/// plus the five stageable KV point writes -- `KvOp::Put`, `KvOp::Insert`,
/// `KvOp::InsertIfAbsent`, `KvOp::InsertOnConflictUpdate`, `KvOp::Delete`.
/// KV is the first non-Document engine to stage at statement time; this
/// predicate is the shared gate later engine units extend the same way.
/// Every other `KvOp` (Incr, Cas, FieldSet, BatchPut, Expire, Transfer, the
/// sorted-index family, etc.) stays on the pre-existing buffer + "OK"
/// deferral, same as any other non-stageable write.
pub fn is_stageable_write(plan: &PhysicalPlan) -> bool {
    is_point_write(plan)
        || matches!(
            plan,
            PhysicalPlan::Kv(
                KvOp::Put { .. }
                    | KvOp::Insert { .. }
                    | KvOp::InsertIfAbsent { .. }
                    | KvOp::InsertOnConflictUpdate { .. }
                    | KvOp::Delete { .. }
            )
        )
}

/// Extract affected row count from a JSON or MessagePack payload.
///
/// Looks for `"affected"`, `"truncated"`, `"inserted"`, or `"accepted"` fields.
pub fn extract_affected_count(payload: &[u8]) -> Option<u64> {
    if payload.is_empty() {
        return None;
    }
    let v: serde_json::Value = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice(payload).ok())?;
    v.get("affected")
        .or_else(|| v.get("truncated"))
        .or_else(|| v.get("inserted"))
        .or_else(|| v.get("accepted"))
        .and_then(|n| n.as_u64())
}

/// Extract the `"op"` field a staged `KvOp::InsertOnConflictUpdate` response
/// payload carries (`"insert"` or `"update"`). `None` for any other payload
/// shape (including every other staged KV write, which carries no `"op"`
/// field at all).
pub fn extract_kv_conflict_op(payload: &[u8]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    let v: serde_json::Value = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice(payload).ok())?;
    v.get("op").and_then(|n| n.as_str()).map(str::to_string)
}

/// Neutral classification of the command a staged write resolved to, used to
/// render a protocol-specific "command complete" tag (pgwire `Tag::new(..)`,
/// or a native-protocol equivalent).
///
/// `KvUpsert` carries whether `KvOp::InsertOnConflictUpdate` resolved to an
/// update (`true`) or an insert (`false`) -- the one staged write whose
/// outcome cannot be decided from the plan shape alone; the stage handler
/// signals it back via the `"op"` field in the response payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedTagKind {
    Insert,
    Update,
    Delete,
    KvUpsert { updated: bool },
}

/// Decide the [`StagedTagKind`] for a staged write, given the plan and the
/// stage handler's raw response payload.
///
/// Caller invariant: `plan` must have passed [`is_stageable_write`].
pub fn staged_tag_kind(plan: &PhysicalPlan, payload: &[u8]) -> StagedTagKind {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut { .. } | DocumentOp::PointInsert { .. }) => {
            StagedTagKind::Insert
        }
        PhysicalPlan::Document(
            DocumentOp::PointUpdate {
                returning: None, ..
            }
            | DocumentOp::BulkUpdate {
                returning: None, ..
            },
        ) => StagedTagKind::Update,
        PhysicalPlan::Document(
            DocumentOp::PointDelete {
                returning: None, ..
            }
            | DocumentOp::BulkDelete {
                returning: None, ..
            },
        ) => StagedTagKind::Delete,
        PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) => StagedTagKind::Insert,
        PhysicalPlan::Kv(op) => staged_kv_tag_kind(op, payload),
        other => unreachable!(
            "staged_tag_kind called on a non-stageable-write plan; \
             is_stageable_write invariant broken: {other:?}"
        ),
    }
}

/// Decide the [`StagedTagKind`] for a staged `KvOp` write.
///
/// Caller invariant: `op` must be one of the five stageable KV writes --
/// `Put`, `Insert`, `InsertIfAbsent`, `InsertOnConflictUpdate`, `Delete` --
/// i.e. the enclosing plan already passed [`is_stageable_write`]. Every
/// other `KvOp` variant is unreachable here because the staging dispatch
/// never routes them through this path.
fn staged_kv_tag_kind(op: &KvOp, payload: &[u8]) -> StagedTagKind {
    match op {
        KvOp::Put { .. } | KvOp::Insert { .. } | KvOp::InsertIfAbsent { .. } => {
            StagedTagKind::Insert
        }
        KvOp::InsertOnConflictUpdate { .. } => StagedTagKind::KvUpsert {
            updated: extract_kv_conflict_op(payload).as_deref() == Some("update"),
        },
        KvOp::Delete { .. } => StagedTagKind::Delete,
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
            "staged_kv_tag_kind called on a non-stageable KvOp; \
             is_stageable_write invariant broken: {op:?}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_affected_count_reads_msgpack_payload() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": 3 })).unwrap();
        assert_eq!(extract_affected_count(&payload), Some(3));
    }

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
