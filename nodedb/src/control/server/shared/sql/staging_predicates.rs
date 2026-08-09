// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral predicates for the in-transaction write-staging gate.
//!
//! Moved verbatim (decision logic only) from the pgwire handler's
//! `plan.rs` / `plan_kv.rs` so the same staging decisions can be reused by
//! any protocol's dispatch loop (pgwire SQL today; native and DSL/UPSERT in
//! later units). No pgwire types are imported here.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, GraphOp, KvOp, SpatialOp, TimeseriesOp,
};

/// Allow-list of the plans the in-transaction path stages at statement time:
/// point writes, predicate `BulkUpdate` / `BulkDelete` (bulk predicate DML),
/// and `Upsert` (`UPSERT INTO`). Explicit match on the exact staged variants —
/// KV point writes stay on the buffer path. Named for the point-write case
/// historically; also covers bulk predicate DML and `Upsert` now that
/// `stage_bulk_update` / `stage_bulk_delete` / `stage_document_upsert` stage the
/// matched rows the same way.
///
/// `InsertSelect` (`INSERT ... SELECT`) is deliberately NOT here: an
/// in-transaction `INSERT ... SELECT` is resolved + staged at STATEMENT time by
/// [`resolve_and_emit_insert_select_ops`](crate::control::insert_select::
/// resolve_and_emit_insert_select_ops) (driven from
/// `session::expander_stage`), which emits concrete fresh-surrogate
/// `PointInsert` ops that flow through this gate on their own — the raw
/// `InsertSelect` never reaches the staging path.
///
/// A `RETURNING` clause does NOT change stageability. The `stage_*` handlers
/// stage the matched rows' resolved post-images identically whether or not a
/// `RETURNING` projection is attached; the clause only governs the client
/// response shape (rows vs. a command tag). Inside a transaction the staged
/// path renders an affected-count tag and the `RETURNING` rows are not
/// projected back to the client — the same rows the pre-existing buffer path
/// discarded — so staging a `RETURNING` DML op is strictly an upgrade of the
/// response tag (`OK` -> `UPDATE n`), never a regression.
pub fn is_point_write(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(
            DocumentOp::PointPut { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::PointDelete { .. }
                | DocumentOp::PointUpdate { .. }
                | DocumentOp::BulkUpdate { .. }
                | DocumentOp::BulkDelete { .. }
                | DocumentOp::Upsert { .. }
        )
    )
}

/// Allow-list of plans the in-transaction path stages at statement time via
/// `MetaOp::StageWrite`: everything [`is_point_write`] accepts (Document
/// point writes, predicate `BulkUpdate` / `BulkDelete`, `Upsert`),
/// plus the eleven stageable KV point writes -- `KvOp::Put`, `KvOp::Insert`,
/// `KvOp::InsertIfAbsent`, `KvOp::InsertOnConflictUpdate`, `KvOp::Delete`,
/// `KvOp::BatchPut`, `KvOp::Incr`, `KvOp::IncrFloat`, `KvOp::Cas`,
/// `KvOp::GetSet`, `KvOp::Expire`, `KvOp::Persist`. KV is the first
/// non-Document engine to stage at statement time; this predicate is the
/// shared gate later engine units extend the same way. `Incr` / `IncrFloat`
/// / `Cas` / `GetSet` / `BatchPut` stage only the computed VALUE bytes into
/// the overlay -- a TTL carried on `Incr` / `BatchPut` is ALSO staged into
/// the overlay's KV TTL delta map (`StagedTtl`, sibling to `Staged`, never
/// consulted by non-KV engines) so a same-transaction `GetTtl` observes it.
/// `Expire` / `Persist` stage directly into that same TTL delta map.
/// `FieldSet` / `Transfer` / `TransferItem` are also stageable -- like the
/// atomics they stage only the computed value bytes (`stage_kv_transfer.rs`).
/// Every other `KvOp` (the sorted-index family, etc.) stays on the
/// pre-existing buffer + "OK" deferral, same as any other non-stageable
/// write.
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
                    | KvOp::BatchPut { .. }
                    | KvOp::Incr { .. }
                    | KvOp::IncrFloat { .. }
                    | KvOp::Cas { .. }
                    | KvOp::GetSet { .. }
                    | KvOp::FieldSet { .. }
                    | KvOp::Transfer { .. }
                    | KvOp::TransferItem { .. }
                    | KvOp::Expire { .. }
                    | KvOp::Persist { .. }
            )
        )
        || matches!(
            plan,
            PhysicalPlan::Columnar(
                ColumnarOp::Insert { .. } | ColumnarOp::Update { .. } | ColumnarOp::Delete { .. }
            )
        )
        || matches!(plan, PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. }))
        || matches!(
            plan,
            PhysicalPlan::Spatial(SpatialOp::Insert { .. } | SpatialOp::Delete { .. })
        )
        || matches!(
            plan,
            PhysicalPlan::Graph(
                GraphOp::EdgePut { .. }
                    | GraphOp::EdgeDelete { .. }
                    | GraphOp::EdgePutBatch { .. }
                    | GraphOp::EdgeDeleteBatch { .. }
                    | GraphOp::SetNodeLabels { .. }
                    | GraphOp::RemoveNodeLabels { .. }
            )
        )
}

/// Extract affected row count from a JSON or MessagePack payload.
///
/// Looks for `"affected"`, `"truncated"`, `"inserted"`, `"accepted"`, or
/// `"deleted"` fields. The alias list exists because several count-bearing
/// payloads are also read directly by non-SQL entry points under their own name
/// (RESP `DEL` reads `"deleted"`, ingest paths read `"accepted"`); the count is
/// the same number under a different key, so every name a write actually emits
/// MUST appear here. A key that emits a count this function cannot see is
/// indistinguishable from a write that reported no count at all.
///
/// `None` means "this payload carries no count" — it is NEVER a licence to
/// substitute one. Callers rendering a DML command tag go through
/// [`require_affected_count`] instead, which turns the absence into a typed
/// error, because for a count-bearing plan the absence can only mean a handler
/// stopped reporting.
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
        .or_else(|| v.get("deleted"))
        .and_then(|n| n.as_u64())
}

/// The affected-row count a DML response MUST carry, or a typed error.
///
/// Use this wherever an affected count is about to be shown to a client. The
/// count is a property of the mutation that ran, so a plan classified as
/// count-bearing whose response has no count is a broken invariant in the
/// handler that produced it — surfacing that loudly is the only way it stays
/// fixed. Defaulting to `1` (or to a dispatcher's per-statement estimate) is
/// what let a delete against an absent row report a removed row.
pub fn require_affected_count(payload: &[u8]) -> crate::Result<u64> {
    extract_affected_count(payload).ok_or_else(|| crate::Error::Internal {
        detail: "write response carried no affected-row count; the handler for this plan must \
                 report one (see CoreLoop::response_affected)"
            .to_owned(),
    })
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
///
/// `DocUpsert` is `DocumentOp::Upsert`'s counterpart: like the autocommit
/// handler (`handlers/upsert.rs`), the pgwire tag for `UPSERT INTO` is
/// always the literal `UPSERT` command regardless of insert-vs-update
/// outcome, so unlike `KvUpsert` it carries no outcome flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedTagKind {
    Insert,
    Update,
    Delete,
    KvUpsert {
        updated: bool,
    },
    DocUpsert,
    /// A statement-time in-transaction `MERGE`, staged as concrete
    /// `PointInsert` / `PointPut` / `PointDelete` ops by the MERGE expander
    /// (`session::expander_stage`). `affected` is the total across all arms;
    /// pgwire renders the Postgres `MERGE <n>` command tag.
    Merge,
    /// A statement-time in-transaction `UPDATE ... FROM <source>`, staged as
    /// concrete `PointPut` ops by the UPDATE-FROM expander
    /// (`session::expander_stage`). `affected` is the total matched target rows;
    /// pgwire renders the `UPDATE <n>` command tag.
    UpdateFromJoin,
    /// The staged handler computed a value rather than an affected-row count
    /// (`KvOp::Incr` / `IncrFloat` / `Cas` / `GetSet`). The caller forwards
    /// [`StagedWriteOutcome::payload`](super::super::session::staging_gate::
    /// StagedWriteOutcome::payload) to the client verbatim instead of
    /// rendering a tag from `affected`.
    RawPayload,
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
        PhysicalPlan::Document(DocumentOp::PointUpdate { .. } | DocumentOp::BulkUpdate { .. }) => {
            StagedTagKind::Update
        }
        PhysicalPlan::Document(DocumentOp::PointDelete { .. } | DocumentOp::BulkDelete { .. }) => {
            StagedTagKind::Delete
        }
        PhysicalPlan::Document(DocumentOp::Upsert { .. }) => StagedTagKind::DocUpsert,
        PhysicalPlan::Kv(op) => staged_kv_tag_kind(op, payload),
        PhysicalPlan::Columnar(ColumnarOp::Insert { .. }) => StagedTagKind::Insert,
        // Predicate `UPDATE ... WHERE` / `DELETE ... WHERE` on a columnar
        // collection: same Update/Delete command tags the Document bulk
        // predicate-DML arms above resolve to.
        PhysicalPlan::Columnar(ColumnarOp::Update { .. }) => StagedTagKind::Update,
        PhysicalPlan::Columnar(ColumnarOp::Delete { .. }) => StagedTagKind::Delete,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. }) => StagedTagKind::Insert,
        PhysicalPlan::Spatial(SpatialOp::Insert { .. }) => StagedTagKind::Insert,
        PhysicalPlan::Spatial(SpatialOp::Delete { .. }) => StagedTagKind::Delete,
        // GraphOp::EdgePut / EdgePutBatch add a new edge tuple -- Insert,
        // matching the autocommit `execute_edge_put` path (which has no
        // distinct update outcome; an edge either exists or it doesn't).
        PhysicalPlan::Graph(GraphOp::EdgePut { .. } | GraphOp::EdgePutBatch { .. }) => {
            StagedTagKind::Insert
        }
        PhysicalPlan::Graph(GraphOp::EdgeDelete { .. } | GraphOp::EdgeDeleteBatch { .. }) => {
            StagedTagKind::Delete
        }
        // SetNodeLabels / RemoveNodeLabels mutate an existing node's label
        // bitset in place -- Update, not Insert/Delete of a row.
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. }) => {
            StagedTagKind::Update
        }
        other => unreachable!(
            "staged_tag_kind called on a non-stageable-write plan; \
             is_stageable_write invariant broken: {other:?}"
        ),
    }
}

/// Decide the [`StagedTagKind`] for a staged `KvOp` write.
///
/// Caller invariant: `op` must be one of the fourteen stageable KV writes --
/// `Put`, `Insert`, `InsertIfAbsent`, `InsertOnConflictUpdate`, `Delete`,
/// `BatchPut`, `Incr`, `IncrFloat`, `Cas`, `GetSet`, `FieldSet`, `Transfer`,
/// `TransferItem`, `Expire`, `Persist` -- i.e. the enclosing plan already
/// passed [`is_stageable_write`]. Every other `KvOp` variant is unreachable
/// here because the staging dispatch never routes them through this path.
fn staged_kv_tag_kind(op: &KvOp, payload: &[u8]) -> StagedTagKind {
    match op {
        KvOp::Put { .. } | KvOp::Insert { .. } | KvOp::InsertIfAbsent { .. } => {
            StagedTagKind::Insert
        }
        KvOp::InsertOnConflictUpdate { .. } => StagedTagKind::KvUpsert {
            updated: extract_kv_conflict_op(payload).as_deref() == Some("update"),
        },
        KvOp::Delete { .. } => StagedTagKind::Delete,
        // `BatchPut` reports an affected/inserted count, same shape as the
        // other insert-style ops.
        KvOp::BatchPut { .. } => StagedTagKind::Insert,
        // `Incr` / `IncrFloat` / `Cas` / `GetSet` return a computed value
        // (`{"value": ..}` / `{"success": .., "current_value": ..}` /
        // `{"old_value": ..}`), not a row count -- forward the payload
        // verbatim. `FieldSet` (`{"fields_added": ..}`), `Transfer`
        // (`{"source_key": .., "dest_key": .., "source_balance": ..,
        // "dest_balance": ..}`), and `TransferItem` (`{"item_key": ..,
        // "dest_key": .., ..}`) are the same shape of "computed result, not
        // a row count" and forward the same way.
        KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::FieldSet { .. }
        | KvOp::Transfer { .. }
        | KvOp::TransferItem { .. } => StagedTagKind::RawPayload,
        // `Expire` / `Persist` mutate an existing row's TTL metadata in
        // place -- Update, not Insert/Delete of the row itself.
        KvOp::Expire { .. } | KvOp::Persist { .. } => StagedTagKind::Update,
        KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::BatchGet { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldGet { .. }
        | KvOp::GetTtl { .. }
        | KvOp::Truncate { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
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

    fn kv_plan(op: KvOp) -> PhysicalPlan {
        PhysicalPlan::Kv(op)
    }

    #[test]
    fn returning_document_writes_are_stageable_and_tagged_by_command() {
        use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};
        let ret = || {
            Some(ReturningSpec {
                columns: ReturningColumns::Star,
            })
        };

        // A RETURNING clause no longer forces the buffer + "OK" path: these
        // stage like any other point/bulk write and render an affected-count
        // command tag (UPDATE n / DELETE n), never `unreachable!`.
        let point_update = PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: "c".into(),
            document_id: "d".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            updates: Vec::new(),
            returning: ret(),
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(is_point_write(&point_update));
        assert!(is_stageable_write(&point_update));
        assert_eq!(staged_tag_kind(&point_update, &[]), StagedTagKind::Update);

        let point_delete = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: "c".into(),
            document_id: "d".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: ret(),
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(is_stageable_write(&point_delete));
        assert_eq!(staged_tag_kind(&point_delete, &[]), StagedTagKind::Delete);

        let bulk_update = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "c".into(),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: ret(),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(is_stageable_write(&bulk_update));
        assert_eq!(staged_tag_kind(&bulk_update, &[]), StagedTagKind::Update);

        let bulk_delete = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: "c".into(),
            filters: Vec::new(),
            returning: ret(),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(is_stageable_write(&bulk_delete));
        assert_eq!(staged_tag_kind(&bulk_delete, &[]), StagedTagKind::Delete);
    }

    #[test]
    fn is_stageable_write_accepts_the_kv_atomics_and_batch_put() {
        assert!(is_stageable_write(&kv_plan(KvOp::Incr {
            collection: "c".into(),
            key: b"k".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: Vec::new(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::IncrFloat {
            collection: "c".into(),
            key: b"k".to_vec(),
            delta: 1.0,
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: Vec::new(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::Cas {
            collection: "c".into(),
            key: b"k".to_vec(),
            expected: vec![],
            new_value: b"v".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: Vec::new(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::GetSet {
            collection: "c".into(),
            key: b"k".to_vec(),
            new_value: b"v".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::BatchPut {
            collection: "c".into(),
            entries: vec![(b"k".to_vec(), b"v".to_vec())],
            ttl_ms: 0,
            surrogates: vec![nodedb_types::Surrogate::ZERO],
            returning: None,
            rls_filters: Vec::new(),
        })));
    }

    #[test]
    fn staged_kv_tag_kind_atomics_forward_raw_payload() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "value": 5 })).unwrap();
        for op in [
            KvOp::Incr {
                collection: "c".into(),
                key: b"k".to_vec(),
                delta: 1,
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: Vec::new(),
            },
            KvOp::IncrFloat {
                collection: "c".into(),
                key: b"k".to_vec(),
                delta: 1.0,
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: Vec::new(),
            },
            KvOp::Cas {
                collection: "c".into(),
                key: b"k".to_vec(),
                expected: vec![],
                new_value: b"v".to_vec(),
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: Vec::new(),
            },
            KvOp::GetSet {
                collection: "c".into(),
                key: b"k".to_vec(),
                new_value: b"v".to_vec(),
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_filters: Vec::new(),
                rls_write_check: Vec::new(),
            },
        ] {
            assert_eq!(
                staged_kv_tag_kind(&op, &payload),
                StagedTagKind::RawPayload,
                "{op:?} must classify as RawPayload"
            );
        }
    }

    #[test]
    fn staged_kv_tag_kind_batch_put_is_insert() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": 2 })).unwrap();
        let op = KvOp::BatchPut {
            collection: "c".into(),
            entries: vec![(b"k".to_vec(), b"v".to_vec())],
            ttl_ms: 0,
            surrogates: vec![nodedb_types::Surrogate::ZERO],
            returning: None,
            rls_filters: Vec::new(),
        };
        assert_eq!(staged_kv_tag_kind(&op, &payload), StagedTagKind::Insert);
    }

    #[test]
    fn is_stageable_write_accepts_expire_and_persist() {
        assert!(is_stageable_write(&kv_plan(KvOp::Expire {
            collection: "c".into(),
            key: b"k".to_vec(),
            ttl_ms: 1_000,
            rls_write_check: Vec::new(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::Persist {
            collection: "c".into(),
            key: b"k".to_vec(),
            rls_write_check: Vec::new(),
        })));
    }

    #[test]
    fn staged_kv_tag_kind_expire_and_persist_are_update() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({})).unwrap();
        let expire = KvOp::Expire {
            collection: "c".into(),
            key: b"k".to_vec(),
            ttl_ms: 1_000,
            rls_write_check: Vec::new(),
        };
        let persist = KvOp::Persist {
            collection: "c".into(),
            key: b"k".to_vec(),
            rls_write_check: Vec::new(),
        };
        assert_eq!(staged_kv_tag_kind(&expire, &payload), StagedTagKind::Update);
        assert_eq!(
            staged_kv_tag_kind(&persist, &payload),
            StagedTagKind::Update
        );
    }
}
