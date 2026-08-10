// SPDX-License-Identifier: Apache-2.0

//! Physical plan types dispatched from Control Plane to Data Plane.
//!
//! The top-level [`PhysicalPlan`] enum delegates to per-engine sub-enums,
//! each defined in its own module. This keeps each engine's operations
//! isolated.

pub mod array;
pub mod cluster_array;
pub mod cluster_event;
pub mod columnar;
pub mod crdt;
pub mod document;
pub mod exchange;
pub mod graph;
pub mod kv;
pub mod meta;
pub mod meta_calvin;
pub mod query;
pub mod routing;
pub mod sort_key;
pub mod spatial;
pub mod text;
pub mod timeseries;
pub mod vector;
pub mod wire;

pub use array::{ArrayBinaryOp, ArrayOp, ArrayReducer};
pub use cluster_array::ClusterArrayOp;
pub use cluster_event::{ClusterEventOp, MAX_REMOTE_CDC_COMMITTED_OFFSETS};
pub use columnar::{ColumnarInsertIntent, ColumnarOp};
pub use crdt::CrdtOp;
pub use document::{
    BalancedDef, DocumentOp, EnforcementOptions, GeneratedColumnSpec, MaterializedSumBinding,
    OllpPredictedEdge, PeriodLockConfig, RegisteredIndex, RegisteredIndexState, ResolvedSumTarget,
    ReturningColumns, ReturningItem, ReturningSpec, StorageMode, SumTargetKey, TimeseriesSchema,
    UpdateValue, resolved_sum_surrogate,
};
pub use exchange::{ExchangeMode, ExchangeOp};
pub use graph::{
    BatchEdge, BspSuperstepPlan, BspSuperstepResult, GraphOp, WccSuperstepPlan, WccSuperstepResult,
};
pub use kv::KvOp;
pub use meta::MetaOp;
pub use query::{AggregateSpec, GroupKeySpec, JoinProjection, QueryOp};
pub use routing::plan_contains_cluster_partitioned_leaf;
pub use sort_key::SortKeySpec;
pub use spatial::{SpatialOp, SpatialPredicate};
pub use text::TextOp;
pub use timeseries::{TimeseriesOp, UNBOUNDED_TIME_RANGE};
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
    /// Event-Plane operations executed by a receiving Control Plane.
    /// Never sent to the Data Plane.
    ClusterEvent(ClusterEventOp),
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

    /// Whether a fanned-out plan can be streamed to the client as an unordered
    /// union of per-source row batches, rather than fully materialized and
    /// merged on the coordinator first.
    ///
    /// Returns `true` ONLY for a plain full-collection scan that imposes no
    /// global ordering, distinctness, offset, or aggregation across the union —
    /// i.e. one whose rows from any source can be emitted in any interleaving
    /// without changing the result:
    ///
    /// - `Document::Scan` — empty `sort_keys`, `distinct == false`, `offset == 0`,
    ///   and no window functions (a window over the union needs the full set).
    /// - `Kv::Scan` — empty `sort_keys`.
    /// - `Columnar::Scan` — empty `sort_keys`.
    /// - `Timeseries::Scan` — empty `sort_keys` (none today), and no aggregation:
    ///   empty `group_by`, empty `aggregates`, `bucket_interval_ms == 0`.
    /// - `ProviderScan` — empty `sort_keys`, `distinct == false`, `offset == 0`.
    ///
    /// Everything else — aggregates, joins, recursive / lateral / facet ops,
    /// vector / text / graph / array / spatial reads, and any sorted, distinct,
    /// offset, windowed, or aggregating scan — returns `false`. The match is
    /// exhaustive so a new plan variant forces an explicit streamability
    /// decision.
    ///
    /// `limit` is intentionally NOT a disqualifier: a global take-N is applied
    /// by the streaming coordinator (it stops pulling once `limit` rows have
    /// been emitted), so a limited scan still streams correctly.
    pub fn is_streamable_unordered_scan(&self) -> bool {
        match self {
            PhysicalPlan::Document(document::DocumentOp::Scan {
                sort_keys,
                distinct,
                offset,
                window_functions,
                ..
            }) => sort_keys.is_empty() && !*distinct && *offset == 0 && window_functions.is_empty(),

            PhysicalPlan::Kv(kv::KvOp::Scan { sort_keys, .. }) => sort_keys.is_empty(),

            PhysicalPlan::Columnar(columnar::ColumnarOp::Scan { sort_keys, .. }) => {
                sort_keys.is_empty()
            }

            PhysicalPlan::Timeseries(timeseries::TimeseriesOp::Scan {
                group_by,
                aggregates,
                bucket_interval_ms,
                ..
            }) => group_by.is_empty() && aggregates.is_empty() && *bucket_interval_ms == 0,

            PhysicalPlan::Query(query::QueryOp::ProviderScan {
                sort_keys,
                offset,
                distinct,
                ..
            }) => sort_keys.is_empty() && *offset == 0 && !*distinct,

            // Every other Document / Kv / Columnar / Timeseries op, plus all
            // other engines and query ops, are not unordered-streamable.
            PhysicalPlan::Document(_)
            | PhysicalPlan::Kv(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_)
            | PhysicalPlan::Query(_) => false,
        }
    }

    /// Global take-N to apply when streaming an unordered scan.
    ///
    /// Returns the scan's row cap (`usize::MAX` for an effectively-unlimited
    /// scan) so the streaming coordinator can stop after that many rows across
    /// the union. Returns `usize::MAX` for any plan that is not a streamable
    /// scan — callers gate on [`PhysicalPlan::is_streamable_unordered_scan`]
    /// first, so the fallthrough is never the deciding factor.
    pub fn streamable_scan_limit(&self) -> usize {
        match self {
            PhysicalPlan::Document(document::DocumentOp::Scan { limit, .. }) => *limit,
            PhysicalPlan::Kv(kv::KvOp::Scan { count, .. }) => *count,
            PhysicalPlan::Columnar(columnar::ColumnarOp::Scan { limit, .. }) => *limit,
            PhysicalPlan::Timeseries(timeseries::TimeseriesOp::Scan { limit, .. }) => *limit,
            PhysicalPlan::Query(query::QueryOp::ProviderScan { limit, .. }) => {
                limit.unwrap_or(usize::MAX)
            }
            _ => usize::MAX,
        }
    }

    /// Primary read/target collection this plan touches, if it maps to exactly
    /// one user collection.
    ///
    /// This is the plane-neutral twin of the Control-Plane
    /// `crate::control::server::shared::plan_util::extract_collection`
    /// (in the `nodedb` core crate). The two MUST stay in sync: the selection
    /// logic below mirrors that function's arms exactly so both planes agree on
    /// which collection a plan reads. `extract_collection` lives in the core
    /// crate (which depends on `nodedb-physical`, not the reverse), so its logic
    /// is replicated here rather than called.
    pub fn collection(&self) -> Option<&str> {
        match self {
            PhysicalPlan::Document(DocumentOp::PointGet { collection, .. })
            | PhysicalPlan::Vector(VectorOp::Search { collection, .. })
            | PhysicalPlan::Document(DocumentOp::RangeScan { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::Read { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::PreviewApply { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::Apply { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::ImportSnapshot { collection, .. })
            | PhysicalPlan::Vector(VectorOp::Insert { collection, .. })
            | PhysicalPlan::Vector(VectorOp::BatchInsert { collection, .. })
            | PhysicalPlan::Vector(VectorOp::MultiSearch { collection, .. })
            // A vector-primary collection stores its row HERE and nowhere else,
            // so this op is the only source a RETURNING projection over it can
            // name. Reporting `None` left those rows with no collection to key
            // a redaction policy on, and the masking pass ran inert.
            | PhysicalPlan::Vector(VectorOp::DirectUpsert { collection, .. })
            | PhysicalPlan::Vector(VectorOp::Delete { collection, .. })
            | PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. })
            | PhysicalPlan::Document(DocumentOp::PointPut { collection, .. })
            | PhysicalPlan::Document(DocumentOp::PointInsert { collection, .. })
            | PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. })
            | PhysicalPlan::Document(DocumentOp::PointUpdate { collection, .. })
            | PhysicalPlan::Document(DocumentOp::Scan { collection, .. })
            | PhysicalPlan::Query(QueryOp::Aggregate { collection, .. })
            | PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection: collection,
                ..
            })
            | PhysicalPlan::Query(QueryOp::NestedLoopJoin {
                left_collection: collection,
                ..
            })
            | PhysicalPlan::Graph(GraphOp::RagFusion { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::SetPolicy { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::GetPolicy { collection, .. })
            | PhysicalPlan::Vector(VectorOp::SetParams { collection, .. })
            | PhysicalPlan::Text(TextOp::Search { collection, .. })
            | PhysicalPlan::Text(TextOp::PhraseSearch { collection, .. })
            | PhysicalPlan::Text(TextOp::HybridSearch { collection, .. })
            | PhysicalPlan::Text(TextOp::HybridSearchTriple { collection, .. })
            | PhysicalPlan::Text(TextOp::BM25ScoreScan { collection, .. })
            | PhysicalPlan::Text(TextOp::FtsIndexDoc { collection, .. })
            | PhysicalPlan::Text(TextOp::FtsDeleteDoc { collection, .. })
            | PhysicalPlan::Text(TextOp::SetTextConfig { collection, .. })
            | PhysicalPlan::Query(QueryOp::PartialAggregate { collection, .. })
            | PhysicalPlan::Query(QueryOp::FacetCounts { collection, .. })
            | PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. })
            | PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. })
            | PhysicalPlan::Document(DocumentOp::Upsert { collection, .. })
            | PhysicalPlan::Document(DocumentOp::InsertSelect {
                target_collection: collection,
                ..
            })
            | PhysicalPlan::Document(DocumentOp::Truncate { collection, .. })
            | PhysicalPlan::Document(DocumentOp::EstimateCount { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::Scan { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::Insert { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::Update { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::Delete { collection, .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Scan { collection, .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. })
            | PhysicalPlan::Spatial(SpatialOp::Scan { collection, .. })
            | PhysicalPlan::Document(DocumentOp::Register { collection, .. })
            | PhysicalPlan::Document(DocumentOp::IndexLookup { collection, .. })
            | PhysicalPlan::Document(DocumentOp::IndexedFetch { collection, .. })
            | PhysicalPlan::Document(DocumentOp::DropIndex { collection, .. }) => {
                Some(collection.as_str())
            }
            PhysicalPlan::Graph(GraphOp::EdgePut { .. })
            | PhysicalPlan::Graph(GraphOp::EdgeDelete { .. })
            | PhysicalPlan::Graph(GraphOp::Hop { .. })
            | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
            | PhysicalPlan::Graph(GraphOp::Path { .. })
            | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
            | PhysicalPlan::Meta(MetaOp::WalAppend { .. })
            | PhysicalPlan::Meta(MetaOp::Cancel { .. })
            | PhysicalPlan::Meta(MetaOp::TransactionBatch { .. })
            | PhysicalPlan::Meta(MetaOp::CreateSnapshot)
            | PhysicalPlan::Meta(MetaOp::Compact)
            | PhysicalPlan::Meta(MetaOp::Checkpoint)
            | PhysicalPlan::Graph(GraphOp::Algo { .. })
            | PhysicalPlan::Graph(GraphOp::Match { .. })
            | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
            | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
            | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
            | PhysicalPlan::Graph(GraphOp::WccSuperstep(_)) => None,
            // Exchange: recurse into the child plan to extract the collection.
            PhysicalPlan::Query(QueryOp::Exchange(op)) => op.child.collection(),
            // PostProcess: recurse into the materialized input plan.
            PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => input.collection(),
            // ProviderScan is a catalog/constant source — no user collection.
            PhysicalPlan::Query(QueryOp::ProviderScan { .. }) => None,
            // KV ops carry their own collection (sorted-index-only ops → None).
            PhysicalPlan::Kv(op) => op.collection(),
            // All remaining ops carry no extractable user collection here: the
            // specific arms above take precedence; these inner wildcards catch
            // the unmatched ops of each engine plus the engines with no arms at
            // all (Array, ClusterArray). Exhaustive so a new variant forces a
            // decision rather than silently returning None.
            PhysicalPlan::Document(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => None,
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
                | PhysicalPlan::Query(QueryOp::PartialAggregateState { .. })
                | PhysicalPlan::Graph(GraphOp::Hop { .. })
                | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
                | PhysicalPlan::Graph(GraphOp::NeighborsMulti { .. })
                | PhysicalPlan::Graph(GraphOp::Path { .. })
                | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
                | PhysicalPlan::Graph(GraphOp::RagFusion { .. })
                | PhysicalPlan::Graph(GraphOp::Match { .. })
                | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
                | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
                | PhysicalPlan::Graph(GraphOp::TemporalNeighbors { .. })
                | PhysicalPlan::Graph(GraphOp::TemporalAlgorithm { .. })
                | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
                | PhysicalPlan::Graph(GraphOp::WccSuperstep(_))
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
