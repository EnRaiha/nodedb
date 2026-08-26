// SPDX-License-Identifier: BUSL-1.1

//! Exhaustive, compile-enforced classification: does a `PhysicalPlan` mutate
//! base state in a way that needs a Calvin write-key / lock?
//!
//! The chokepoint `classify_dispatch` and `build_static_tx_class` use to
//! decide write-key-set membership. Mirrors `plan_vshard`: every op in the
//! eight write-capable engines is matched explicitly `true`/`false` (no
//! wildcard), so a new op variant is a compile error here. Text/Spatial/
//! Query/Meta stay one blanket `false` arm each (`NotAWrite` in
//! `plan_vshard`), still exhaustive over `PhysicalPlan`.
//!
//! Does NOT delegate to `plan_is_write` (`Permission::Write`): several
//! `Permission::Write` variants carry no vshard to lock in `plan_vshard`
//! (index-metadata ops, cross-collection writes like `Merge`/`TransferItem`,
//! Text/Spatial write ops, most `MetaOp` writes) and would misclassify as
//! Calvin writes, turning a routing gap into an aborted transaction.

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
        // `UpdateFromJoin` is `Unroutable` in `plan_vshard` (no enforced
        // co-location) but is already classified `true` here.
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Truncate { .. }
        // Without this in the write-key set, the pair classifies as
        // single-shard and the source write commits without the balance.
        | DocumentOp::ApplyBalanceDelta { .. }
        // Mutates the rows its mutation list names, like any other write.
        | DocumentOp::ResolvedWrite { .. } => true,
        // Read-only: reports what the wrapped write would apply, mutates nothing.
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
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
        // (no enforced co-location) — kept `false` so it never enters the
        // write-key set with no vshard to lock on.
        | DocumentOp::Merge { .. } => false,
    }
}

/// Whether `plan` is a DERIVED side effect (a `GraphOp` edge write mirroring
/// a document, or an [`DocumentOp::ApplyBalanceDelta`] whose target differs
/// from the source's vShard) rather than the user's own write.
///
/// Both are real writes that must enter Calvin's write-key set — what they
/// must NOT do is answer the client: a derived participant's response
/// describes a row the statement never named, so shaping `CommandComplete`
/// from it would report the wrong count. Named once here rather than as an
/// inline negation, after the balance write (modelled on the implicit graph
/// edge) failed to inherit an ad hoc `!matches!` check and raced the source
/// write to deposit the statement's response.
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
        | KvOp::Transfer { .. }
        // Predicate DML mutates the rows a scan selects, homed on the one
        // collection it names — a single-vshard write like `Truncate`.
        | KvOp::PredicateUpdate { .. }
        | KvOp::PredicateDelete { .. } => true,
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
        // Read-only: reports what a governed write would apply, mutates
        // nothing, and is `NotAWrite` in `plan_vshard`.
        | KvOp::ResolveWrite(_)
        // `Permission::Write` but `NotAWrite` in `plan_vshard` — index
        // metadata registration, not key-value state; no vshard to lock on.
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        // `Permission::Write` but `Unroutable` in `plan_vshard`
        // (cross-collection write, no enforced co-location).
        | KvOp::TransferItem { .. }
        // `Permission::Write` but `Unroutable` in `plan_vshard` — its
        // mutations may span two collections, so no single vshard to lock on.
        | KvOp::ResolvedWrite { .. } => false,
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
        // The resolve pass writes nothing; the delete it decides is proposed
        // separately by the write-resolve orchestrator.
        GraphOp::ResolveEdgeDelete(_)
        | GraphOp::Hop { .. }
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
        // The resolve pass writes nothing; the ingest it reports is proposed
        // separately by the write-resolve orchestrator.
        TimeseriesOp::ResolveIngest(_) | TimeseriesOp::Scan { .. } => false,
    }
}

fn columnar_is_write(op: &ColumnarOp) -> bool {
    match op {
        ColumnarOp::Insert { .. }
        | ColumnarOp::Update { .. }
        | ColumnarOp::Delete { .. }
        | ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. } => true,
        // Read-only: decides the write policy but mutates nothing, so no
        // vshard lock to take.
        ColumnarOp::Scan { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::ResolveDml { .. } => false,
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
        // `Put`/`Delete`/`Flush` are `Unroutable` in `plan_vshard` (tile→vshard
        // needs catalog tile_extents not present on the plan) but are
        // already classified `true` here.
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
