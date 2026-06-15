// SPDX-License-Identifier: Apache-2.0

//! Physical plan types dispatched from Control Plane to Data Plane.
//!
//! The top-level [`PhysicalPlan`] enum delegates to per-engine sub-enums,
//! each defined in its own module. This keeps each engine's operations
//! isolated.

pub mod array;
pub mod cluster_array;
pub mod columnar;
pub mod crdt;
pub mod document;
pub mod exchange;
pub mod graph;
pub mod kv;
pub mod meta;
pub mod query;
pub mod routing;
pub mod spatial;
pub mod text;
pub mod timeseries;
pub mod vector;
pub mod wire;

pub use array::{ArrayBinaryOp, ArrayOp, ArrayReducer};
pub use cluster_array::ClusterArrayOp;
pub use columnar::{ColumnarInsertIntent, ColumnarOp};
pub use crdt::CrdtOp;
pub use document::{
    BalancedDef, DocumentOp, EnforcementOptions, GeneratedColumnSpec, MaterializedSumBinding,
    PeriodLockConfig, RegisteredIndex, RegisteredIndexState, ReturningColumns, ReturningItem,
    ReturningSpec, StorageMode, UpdateValue,
};
pub use exchange::{ExchangeMode, ExchangeOp};
pub use graph::{BatchEdge, GraphOp};
pub use kv::KvOp;
pub use meta::MetaOp;
pub use query::{AggregateSpec, JoinProjection, QueryOp};
pub use routing::plan_contains_cluster_partitioned_leaf;
pub use spatial::{SpatialOp, SpatialPredicate};
pub use text::TextOp;
pub use timeseries::TimeseriesOp;
pub use vector::VectorOp;
pub use wire::{decode, encode};

/// Physical plan dispatched to the Data Plane.
///
/// Each variant wraps a per-engine operation enum. The Data Plane dispatcher
/// matches on the top-level variant, then delegates to engine-specific handlers.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum PhysicalPlan {
    /// Vector engine: HNSW search, insert, delete, params.
    Vector(VectorOp),
    /// Graph engine: edges, traversal, algorithms, pattern matching.
    Graph(GraphOp),
    /// Document engine: point CRUD, scans, indexes, bulk DML.
    Document(DocumentOp),
    /// KV engine: hash-indexed point ops, TTL, batch ops.
    Kv(KvOp),
    /// Full-text search: BM25, hybrid vector+text.
    Text(TextOp),
    /// Columnar engine (base): scan + insert for plain columnar collections.
    Columnar(ColumnarOp),
    /// Timeseries profile: extends columnar with time-range + bucketing.
    Timeseries(TimeseriesOp),
    /// Spatial profile: extends columnar with R-tree + OGC predicates.
    Spatial(SpatialOp),
    /// CRDT engine: read, apply delta, set policy.
    Crdt(CrdtOp),
    /// Query operations: joins, aggregates, and the coordinator-resolved
    /// `Exchange` data-movement node (see `QueryOp::Exchange`).
    Query(QueryOp),
    /// Meta / maintenance: WAL, cancel, snapshot, compact, checkpoint.
    Meta(MetaOp),
    /// Array engine: ND-array query operators + put/delete/flush/compact.
    Array(ArrayOp),
    /// Cluster-mode array operations executed by the coordinator on the
    /// Control Plane. Never sent to the Data Plane.
    ClusterArray(ClusterArrayOp),
}

impl PhysicalPlan {
    /// Whether this plan is a sharded-source operation that the converter must
    /// wrap in `Exchange{Gather}`. Identifies reads and joins that are
    /// distributed across all Data Plane cores and whose results must be
    /// gathered and merged on the coordinator.
    pub fn is_sharded_source(&self) -> bool {
        // Aggregate and HashJoin are sharded ONLY when at least one of their
        // leaves reads a real per-shard collection. A pure-catalog plan (whose
        // only leaves are coordinator-materialized `ProviderScan` nodes) is
        // coordinator-local: it must run EXACTLY once. Broadcasting a
        // pure-catalog COUNT(*) to N cores would N×-overcount, and a
        // pure-catalog join would duplicate every row N times.
        match self {
            // Aggregate over a sub-plan (catalog): inherit the child's
            // sharded-ness. Aggregate with no sub-plan: legacy per-shard scan
            // of the named collection → always sharded.
            PhysicalPlan::Query(QueryOp::Aggregate { input, .. }) => match input {
                Some(child) => child.is_sharded_source(),
                None => true,
            },
            // HashJoin is sharded iff at least one side reads a real
            // per-shard collection. Catalog⋈catalog → false (coordinator-local);
            // any side touching a real collection → true.
            PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
                ..
            }) => {
                hash_join_side_is_sharded(left_collection, left_input, left_bitmap)
                    || hash_join_side_is_sharded(right_collection, right_input, right_bitmap)
            }
            _ => self.is_sharded_source_leaf(),
        }
    }

    /// Leaf / non-recursive sharded-source check for all plan variants other
    /// than `Aggregate` and `HashJoin` (handled structurally in
    /// `is_sharded_source`).
    fn is_sharded_source_leaf(&self) -> bool {
        matches!(
            self,
            PhysicalPlan::Document(DocumentOp::Scan { .. })
                | PhysicalPlan::Columnar(ColumnarOp::Scan { .. })
                | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
                | PhysicalPlan::Graph(GraphOp::Hop { .. })
                | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
                | PhysicalPlan::Graph(GraphOp::NeighborsMulti { .. })
                | PhysicalPlan::Graph(GraphOp::Path { .. })
                | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
                | PhysicalPlan::Graph(GraphOp::RagFusion { .. })
                | PhysicalPlan::Graph(GraphOp::Match { .. })
                | PhysicalPlan::Graph(GraphOp::TemporalNeighbors { .. })
                | PhysicalPlan::Graph(GraphOp::TemporalAlgorithm { .. })
                | PhysicalPlan::Graph(GraphOp::Stats { .. })
                | PhysicalPlan::Vector(VectorOp::Search { .. })
                | PhysicalPlan::Text(TextOp::Search { .. })
                | PhysicalPlan::Text(TextOp::HybridSearch { .. })
                | PhysicalPlan::Text(TextOp::HybridSearchTriple { .. })
                | PhysicalPlan::Text(TextOp::BM25ScoreScan { .. })
        )
    }
}

/// Whether one side of a `HashJoin` reads a real per-shard collection (and so
/// makes the join a sharded source that must be fanned to all cores).
///
/// Side shapes, per the converter:
/// - `input: Some(child)` — a resolved sub-plan. A catalog side lowers to a
///   `ProviderScan` (coordinator-local → not sharded); an Exchange-wrapped or
///   otherwise-sharded child propagates its sharded-ness. Recurse.
/// - `input: None` + non-empty `collection` — a real collection scanned
///   locally by name → sharded.
/// - `input: None` + empty `collection` — no real read on this side.
/// - `bitmap: Some(..)` — a bitmap producer over a real collection
///   (`IndexedFetch`) → that side touches a real collection → sharded.
fn hash_join_side_is_sharded(
    collection: &str,
    input: &Option<Box<PhysicalPlan>>,
    bitmap: &Option<Box<PhysicalPlan>>,
) -> bool {
    if let Some(child) = input {
        // An Exchange wrapper always carries a sharded child; otherwise defer
        // to the child's own structural classification.
        return matches!(**child, PhysicalPlan::Query(QueryOp::Exchange(_)))
            || child.is_sharded_source();
    }
    if bitmap.is_some() {
        return true;
    }
    !collection.is_empty()
}
