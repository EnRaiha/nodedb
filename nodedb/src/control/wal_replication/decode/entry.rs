// SPDX-License-Identifier: BUSL-1.1

//! Entry point: decode a committed `ReplicatedEntry` into a `PhysicalPlan`.
//!
//! `to_physical_plan` is a thin top-level dispatcher: it groups the
//! `ReplicatedWrite` variants by the `PhysicalPlan` family they produce and
//! delegates each group to a per-engine `decode_arm` (mirroring how `encode/`
//! splits its per-engine classification into `entry_*` siblings). The
//! top-level match is exhaustive over every `ReplicatedWrite` variant — a new
//! variant is a compile error here, never a silent omission.
//!
//! The shared `DecodeCtx` + surrogate-binding helpers used across the
//! per-engine decode submodules live in [`super::ctx`].

use super::super::types::{ReplicatedEntry, ReplicatedWrite};
use super::ctx::DecodeCtx;
use super::{
    entry_array, entry_columnar_family, entry_crdt, entry_document, entry_graph, entry_kv, vector,
};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::surrogate::SurrogateAssigner;
use crate::types::{DatabaseId, TenantId, VShardId};

/// Decoded `(tenant, vshard, plan, resolved_now_ms)` for a committed entry.
///
/// `resolved_now_ms` is the wall-clock instant the proposing node resolved
/// for a TTL-bearing KV write (see `ReplicatedWrite::KvPut::resolved_now_ms`),
/// `None` for every non-TTL write and for writes with no TTL. The caller
/// stamps it onto the dispatched `Request::resolved_now_ms` so every replica
/// installs the identical `expire_at_ms` instead of reading its own wall
/// clock at apply time.
pub type DecodedEntry = (TenantId, VShardId, PhysicalPlan, Option<u64>);

/// Returns `None` if the data is not a valid ReplicatedEntry (e.g., ConfChange or no-op).
///
/// `assigner`, when `Some`, drives follower-local surrogate binding.
/// Single-row writers (documents, KV, vector, graph edges) carry the
/// leader-assigned surrogate verbatim on the wire and call
/// `assigner.bind(...)` to install that exact identity in the local catalog
/// (+ `SurrogateBind` WAL record) — they never re-allocate, so the same key
/// resolves to the same surrogate on every node. CRDT variants still
/// re-derive via `assign`. When `None`, surrogate fields fall back to the
/// carried value / `Surrogate::ZERO` without catalog writes (used by tests
/// that exercise the decoder without `SharedState`).
pub fn from_replicated_entry(
    data: &[u8],
    assigner: Option<&SurrogateAssigner>,
) -> crate::Result<Option<DecodedEntry>> {
    let entry = match ReplicatedEntry::from_bytes(data) {
        Some(e) => e,
        None => return Ok(None),
    };
    // Array CRDT variants are handled by the distributed applier before this
    // function is called. Return None so the applier skips the generic dispatch
    // path for them.
    match &entry.write {
        ReplicatedWrite::ArrayOp { .. } | ReplicatedWrite::ArraySchema { .. } => {
            return Ok(None);
        }
        _ => {}
    }
    let tenant_id = TenantId::new(entry.tenant_id);
    // `0` decodes to `DatabaseId::DEFAULT` — the same convention used for
    // entries that pre-date the field (see `LegacyReplicatedEntry`).
    let database_id = DatabaseId::new(entry.database_id);
    let ctx = DecodeCtx {
        assigner,
        database_id,
        tenant_id,
    };
    let (plan, resolved_now_ms) = to_physical_plan(&entry.write, &ctx)?;
    Ok(Some((
        tenant_id,
        VShardId::new(entry.vshard_id),
        plan,
        resolved_now_ms,
    )))
}

/// Convert a ReplicatedWrite back into a PhysicalPlan for Data Plane
/// execution, alongside the TTL-bearing KV writes' `resolved_now_ms` (see
/// [`DecodedEntry`]). `resolved_now_ms` stays `None` for every group except
/// the KV group, whose TTL-bearing arms stamp it from the wire field.
fn to_physical_plan(
    write: &ReplicatedWrite,
    ctx: &DecodeCtx,
) -> crate::Result<(PhysicalPlan, Option<u64>)> {
    match write {
        // Document family (`PhysicalPlan::Document`).
        ReplicatedWrite::PointPut { .. }
        | ReplicatedWrite::PointInsert { .. }
        | ReplicatedWrite::PointDelete { .. }
        | ReplicatedWrite::PointUpdate { .. }
        | ReplicatedWrite::DocUpsert { .. }
        | ReplicatedWrite::DocBatchInsert { .. }
        | ReplicatedWrite::DocTruncate { .. }
        | ReplicatedWrite::BulkDml { .. }
        | ReplicatedWrite::InsertSelect { .. }
        | ReplicatedWrite::ApplyBalanceDelta { .. } => {
            Ok((entry_document::decode_arm(ctx, write)?, None))
        }
        // The full `Vector*` variant family (original four write shapes plus
        // the sparse / multi-vector / direct-upsert / delete-by-surrogate
        // additions) is dispatched to a single helper — see
        // `vector::decode_arm`'s doc.
        ReplicatedWrite::VectorInsert { .. }
        | ReplicatedWrite::VectorBatchInsert { .. }
        | ReplicatedWrite::VectorDelete { .. }
        | ReplicatedWrite::SetVectorParams { .. }
        | ReplicatedWrite::DropVectorIndex { .. }
        | ReplicatedWrite::SparseInsert { .. }
        | ReplicatedWrite::SparseDelete { .. }
        | ReplicatedWrite::MultiVectorInsert { .. }
        | ReplicatedWrite::MultiVectorDelete { .. }
        | ReplicatedWrite::DeleteBySurrogate { .. }
        | ReplicatedWrite::DirectUpsert { .. } => Ok((vector::decode_arm(ctx, write)?, None)),
        // CRDT family (`PhysicalPlan::Crdt`).
        ReplicatedWrite::CrdtApply { .. }
        | ReplicatedWrite::CrdtApplyFenced { .. }
        | ReplicatedWrite::CrdtApplyAuthenticated { .. }
        | ReplicatedWrite::CrdtImportCollection { .. }
        | ReplicatedWrite::CrdtListInsert { .. }
        | ReplicatedWrite::CrdtListDelete { .. }
        | ReplicatedWrite::CrdtListMove { .. }
        | ReplicatedWrite::CrdtDocUpsert { .. }
        | ReplicatedWrite::CrdtDocDelete { .. }
        | ReplicatedWrite::ConstraintChange { .. } => {
            Ok((entry_crdt::decode_arm(ctx, write)?, None))
        }
        // Graph family (`PhysicalPlan::Graph`).
        ReplicatedWrite::EdgePut { .. }
        | ReplicatedWrite::EdgeDelete { .. }
        | ReplicatedWrite::SetNodeLabels { .. }
        | ReplicatedWrite::RemoveNodeLabels { .. }
        | ReplicatedWrite::EdgePutBatch { .. }
        | ReplicatedWrite::EdgeDeleteBatch { .. } => {
            Ok((entry_graph::decode_arm(ctx, write)?, None))
        }
        // KV family (`PhysicalPlan::Kv`) — the only group that carries
        // `resolved_now_ms` (TTL-bearing arms stamp it from the wire field).
        ReplicatedWrite::KvTruncate { .. }
        | ReplicatedWrite::KvPut { .. }
        | ReplicatedWrite::KvDelete { .. }
        | ReplicatedWrite::KvInsert { .. }
        | ReplicatedWrite::KvInsertIfAbsent { .. }
        | ReplicatedWrite::KvInsertOnConflictUpdate { .. }
        | ReplicatedWrite::KvBatchPut { .. }
        | ReplicatedWrite::KvExpire { .. }
        | ReplicatedWrite::KvPersist { .. }
        | ReplicatedWrite::KvIncr { .. }
        | ReplicatedWrite::KvIncrFloat { .. }
        | ReplicatedWrite::KvCas { .. }
        | ReplicatedWrite::KvGetSet { .. }
        | ReplicatedWrite::KvRegisterSortedIndex { .. }
        | ReplicatedWrite::KvDropSortedIndex { .. }
        | ReplicatedWrite::KvRegisterIndex { .. }
        | ReplicatedWrite::KvDropIndex { .. }
        | ReplicatedWrite::KvFieldSet { .. }
        | ReplicatedWrite::KvTransfer { .. }
        | ReplicatedWrite::KvTransferItem { .. } => entry_kv::decode_arm(ctx, write),
        // Columnar-storage family + overlay sync engines
        // (`PhysicalPlan::Columnar` / `Timeseries` / `Text` / `Spatial`).
        ReplicatedWrite::ColumnarIngest { .. }
        | ReplicatedWrite::TimeseriesIngest { .. }
        | ReplicatedWrite::FtsIndex { .. }
        | ReplicatedWrite::FtsDelete { .. }
        | ReplicatedWrite::SpatialInsert { .. }
        | ReplicatedWrite::SpatialDelete { .. }
        | ReplicatedWrite::ColumnarBulkDml { .. } => {
            Ok((entry_columnar_family::decode_arm(write)?, None))
        }
        // Raft-native array cell writes (`PhysicalPlan::Array`) — the cluster
        // SQL DML array path. Distinct from the Lite-sync `ArrayOp` CRDT variant
        // below, which is intercepted by the distributed applier and never
        // reaches this dispatcher. The applier routes the plan these produce
        // through the array-open bootstrap rather than the plain dispatch path.
        ReplicatedWrite::ArrayCellPut { .. } | ReplicatedWrite::ArrayCellDelete { .. } => {
            Ok((entry_array::decode_arm(ctx, write)?, None))
        }
        // The following variants are intercepted upstream (Array CRDT ops by
        // `from_replicated_entry`, CalvinReadResult by the apply loop) and never
        // dispatched through the generic Data Plane path. These arms exist only
        // to keep the match exhaustive.
        ReplicatedWrite::ArrayOp { .. } => Err(crate::Error::Internal {
            detail: "ArrayOp reached to_physical_plan (should have been intercepted)".into(),
        }),
        ReplicatedWrite::ArraySchema { .. } => Err(crate::Error::Internal {
            detail: "ArraySchema reached to_physical_plan (should have been intercepted)".into(),
        }),
        ReplicatedWrite::CalvinReadResult { .. } => Err(crate::Error::Internal {
            detail: "CalvinReadResult reached to_physical_plan (should have been intercepted)"
                .into(),
        }),
    }
}
