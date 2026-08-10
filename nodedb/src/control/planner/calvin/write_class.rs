// SPDX-License-Identifier: BUSL-1.1

//! Exhaustive, compile-enforced classification: does a `PhysicalPlan` mutate
//! base state in a way that needs a Calvin write-key / lock?
//!
//! This is the single chokepoint `dispatch::classify_dispatch` and
//! `tx_class::build_static_tx_class` use to decide whether a plan
//! participates in Calvin's write-key set. Mirrors the technique used by
//! `plan_vshard` (`control/cluster/calvin/scheduler/driver/core/routing.rs`):
//! every op variant inside the eight write-capable engine families (Document,
//! Kv, Vector, Graph, Timeseries, Columnar, Crdt, Array) is matched
//! explicitly as `true` or `false` — no wildcard arm — so a newly added op
//! variant is a compile error here, not a silently-`false` write
//! classification. Text, Spatial, Query, and Meta plans never carry a
//! Calvin-lockable write in `plan_vshard` (each is a single blanket
//! `NotAWrite` there), so they stay a single blanket `false` arm each at the
//! `PhysicalPlan` level — that match is still exhaustive over every
//! `PhysicalPlan` variant, it just doesn't drill into their payload.
//!
//! # Why this does NOT delegate to `plan_is_write`
//!
//! `write_admission::predicate::plan_is_write` answers a different question
//! — "does this plan require `Permission::Write`?", derived from the
//! security tier's exhaustive `required_permission` oracle. Several op
//! variants answer yes there while carrying no vshard to lock on in
//! `plan_vshard`, so naively deriving from `Permission::Write` would
//! misclassify them as Calvin writes:
//!
//! - `VectorOp::{SetParams, Seal, CompactIndex, Rebuild}` are
//!   `Permission::Alter`, not `Write` at all.
//! - `KvOp::{RegisterIndex, DropIndex, RegisterSortedIndex,
//!   DropSortedIndex}` are `Permission::Write` but `NotAWrite` in
//!   `plan_vshard` (index metadata, not key-value state).
//! - `DocumentOp::Merge` and `KvOp::TransferItem` are `Permission::Write` but
//!   `Unroutable` in `plan_vshard` (cross-collection writes with no
//!   enforced co-location).
//! - `TextOp::{FtsIndexDoc, FtsDeleteDoc}` and `SpatialOp::{Insert, Delete}`
//!   are `Permission::Write` but their whole families are blanket
//!   `NotAWrite` in `plan_vshard`.
//! - Most `MetaOp` writes (`WalAppend`, `TransactionBatch`,
//!   `CalvinExecute*`, ...) are `Permission::Write` but the whole `Meta`
//!   family is blanket `NotAWrite` in `plan_vshard` — these are Calvin's own
//!   internal dispatch/commit plans, not user writes to classify.
//!
//! Widening `is_write_plan` for any of the above would turn a routing gap
//! into an aborted transaction (a write key with no vshard to lock).

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};

fn document_is_write(op: &DocumentOp) -> bool {
    match op {
        DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        // Pre-existing: `UpdateFromJoin` is `Unroutable` in `plan_vshard`
        // (source/target co-location is not enforced) yet is already
        // classified `true` here upstream of this change; left as-is, out
        // of this task's scope, since `plan_vshard` is not being changed.
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Truncate { .. }
        // A balance write mutates a row like any other document write, and it
        // is precisely the task that has to enter the write-key set: without it
        // the pair it belongs to classifies as single-shard and the source
        // write commits without the balance.
        | DocumentOp::ApplyBalanceDelta { .. } => true,
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        // `Merge` is `Permission::Write` but `Unroutable` in `plan_vshard`
        // (cross-collection write, no enforced co-location) — kept `false`
        // here so it never enters Calvin's write-key set with no vshard to
        // lock on.
        | DocumentOp::Merge { .. } => false,
    }
}

/// Whether `plan` is a DERIVED side effect rather than the user's own write.
///
/// Two shapes qualify, and they are the same shape: a cross-collection write
/// the Control Plane appends alongside a statement it did not appear in.
///
/// * a `GraphOp` edge write mirroring a document that carries `_from`/`_to`;
/// * a [`DocumentOp::ApplyBalanceDelta`] moving a materialized-sum balance
///   whose target does not share the source's vShard.
///
/// Both are real writes and both must enter Calvin's write-key set — they are
/// what makes the pair multi-shard and commit atomically. What they must NOT do
/// is answer the client. A statement's `CommandComplete` tag is shaped from ONE
/// applied response, and a derived participant's response describes a row the
/// user's statement never named: shaping `INSERT` from it reports a count that
/// belongs to a different write.
///
/// The distinction is named here, once, rather than spelled as an inline
/// negation at the place the responses are folded. It was an inline
/// `!matches!(plan, PhysicalPlan::Graph(_))` in
/// `plans_have_primary_write`, so the balance write — added later, and
/// deliberately modelled on the implicit graph edge — never inherited it and
/// raced the source write to deposit the statement's applied response.
pub fn is_derived_side_effect(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Graph(_) => true,
        PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { .. }) => true,
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => false,
    }
}

fn kv_is_write(op: &KvOp) -> bool {
    match op {
        KvOp::Put { .. }
        | KvOp::Insert { .. }
        | KvOp::InsertIfAbsent { .. }
        | KvOp::InsertOnConflictUpdate { .. }
        | KvOp::Delete { .. }
        | KvOp::BatchPut { .. }
        | KvOp::Expire { .. }
        | KvOp::Persist { .. }
        | KvOp::FieldSet { .. }
        | KvOp::Truncate { .. }
        | KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::Transfer { .. } => true,
        KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::FieldGet { .. }
        | KvOp::MaterializeScan { .. }
        // `SortedIndexRank`/`TopK`/`Range`/`Count`/`Score` are `Permission::Read`
        // (query-only) despite the `SortedIndex*` naming.
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        // `Permission::Write` but `NotAWrite` in `plan_vshard` — index
        // metadata registration, not key-value state; no vshard to lock on.
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        // `Permission::Write` but `Unroutable` in `plan_vshard`
        // (cross-collection write, no enforced co-location).
        | KvOp::TransferItem { .. } => false,
    }
}

fn vector_is_write(op: &VectorOp) -> bool {
    match op {
        VectorOp::Insert { .. }
        | VectorOp::BatchInsert { .. }
        | VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::DirectUpsert { .. } => true,
        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        // `SetParams`/`DropIndex`/`Seal`/`CompactIndex`/`Rebuild` are
        // `Permission::Alter` (not `Write` at all) and `NotAWrite` in
        // `plan_vshard`.
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. } => false,
    }
}

fn graph_is_write(op: &GraphOp) -> bool {
    match op {
        GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDelete { .. }
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. } => true,
        GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::Stats { .. } => false,
    }
}

fn timeseries_is_write(op: &TimeseriesOp) -> bool {
    match op {
        TimeseriesOp::Ingest { .. } => true,
        TimeseriesOp::Scan { .. } => false,
    }
}

fn columnar_is_write(op: &ColumnarOp) -> bool {
    match op {
        ColumnarOp::Insert { .. } | ColumnarOp::Update { .. } | ColumnarOp::Delete { .. } => true,
        ColumnarOp::Scan { .. } | ColumnarOp::MaterializeScan { .. } => false,
    }
}

fn crdt_is_write(op: &CrdtOp) -> bool {
    match op {
        CrdtOp::Apply { .. } | CrdtOp::ApplyAuthenticated { .. }
        | CrdtOp::ListInsert { .. }
        | CrdtOp::ListDelete { .. }
        | CrdtOp::ListMove { .. }
        | CrdtOp::DocUpsert { .. }
        | CrdtOp::DocDelete { .. }
        | CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::RestoreToVersion { .. }
        | CrdtOp::ImportSnapshot { .. } => true,
        CrdtOp::Read { .. }
        | CrdtOp::PreviewApply { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        // `SetPolicy`/`CompactAtVersion` are `Permission::Alter` (not `Write`
        // at all), same pattern as `VectorOp::SetParams`/`Seal` above.
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::CompactAtVersion { .. } => false,
    }
}

fn array_is_write(op: &ArrayOp) -> bool {
    match op {
        // Pre-existing: `Put`/`Delete`/`Flush` are `Unroutable` in
        // `plan_vshard` (tile->vshard needs catalog tile_extents not present
        // on the plan) yet are already classified `true` here upstream of
        // this change; left as-is, out of this task's scope.
        ArrayOp::Put { .. } | ArrayOp::Delete { .. } | ArrayOp::Flush { .. } => true,
        ArrayOp::OpenArray { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RestoreArrayDrop { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => false,
    }
}

/// Returns `true` if the plan is a write operation that must be classified
/// into Calvin's write-key set.
///
/// Centralizing this avoids scattered `match` arms when new write variants
/// are added. Reads, scans, and query operators return `false`.
pub fn is_write_plan(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Document(op) => document_is_write(op),
        PhysicalPlan::Kv(op) => kv_is_write(op),
        PhysicalPlan::Vector(op) => vector_is_write(op),
        PhysicalPlan::Graph(op) => graph_is_write(op),
        PhysicalPlan::Timeseries(op) => timeseries_is_write(op),
        PhysicalPlan::Columnar(op) => columnar_is_write(op),
        PhysicalPlan::Crdt(op) => crdt_is_write(op),
        PhysicalPlan::Array(op) => array_is_write(op),
        // Reads, scans, queries, meta, spatial, text: none of these
        // families carry a Calvin-lockable write in `plan_vshard`.
        PhysicalPlan::Spatial(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => false,
    }
}

#[cfg(test)]
#[path = "write_class_tests.rs"]
mod tests;
