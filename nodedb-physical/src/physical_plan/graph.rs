// SPDX-License-Identifier: Apache-2.0

//! Graph engine operations dispatched to the Data Plane.

use nodedb_graph::{AlgoParams, Direction, GraphAlgorithm, GraphTraversalOptions};
use nodedb_types::{Surrogate, SurrogateBitmap, SystemTimeScope};

/// One edge in an `EdgePutBatch` / `EdgeDeleteBatch`.
///
/// `src_surrogate` / `dst_surrogate` carry the global row identity for the
/// edge endpoints (resolved at construction time via the surrogate assigner).
/// `Surrogate::ZERO` is used in test fixtures and on in-memory paths where
/// no catalog is wired; production paths always populate real surrogates.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BatchEdge {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub src_surrogate: Surrogate,
    pub dst_surrogate: Surrogate,
}

/// Graph engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum GraphOp {
    /// Insert a graph edge with properties.
    ///
    /// `src_surrogate` / `dst_surrogate` carry the global row identity for
    /// the two endpoints, resolved at construction time. The string `src_id`
    /// / `dst_id` remain user-visible identifiers (used by the CSR partition
    /// for label interning and by the edge store for keying), while the
    /// surrogates are the cross-engine join currency.
    EdgePut {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
        src_surrogate: Surrogate,
        dst_surrogate: Surrogate,
    },

    /// Batched edge insert: many `(collection, src, label, dst)` tuples.
    /// Every edge in the batch must target the same collection — the
    /// batch is a unit of work, not a cross-collection scatter.
    EdgePutBatch { edges: Vec<BatchEdge> },

    /// Delete a graph edge.
    ///
    /// Carries `src_surrogate` / `dst_surrogate` mirroring `EdgePut` so a
    /// cross-shard delete can be dual-homed atomically via Calvin: the
    /// surrogate pair gives the static-tx class its participant shards
    /// (`from_key(src)` / `from_key(dst)`) AND the lock identity that
    /// conflict-serializes against a concurrent `EdgePut` of the same edge.
    EdgeDelete {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        src_surrogate: Surrogate,
        dst_surrogate: Surrogate,
    },

    /// Batched edge delete: used to revert a partial `EdgePutBatch` on
    /// failure so the DDL leaves no stranded edges.
    EdgeDeleteBatch { edges: Vec<BatchEdge> },

    /// Graph hop traversal: BFS from start nodes via label, bounded by depth.
    Hop {
        start_nodes: Vec<String>,
        edge_label: Option<String>,
        direction: Direction,
        depth: usize,
        options: GraphTraversalOptions,
        /// RLS filters applied to traversed nodes before returning.
        rls_filters: Vec<u8>,
        /// Optional surrogate prefilter restricting which frontier nodes are
        /// eligible as traversal targets. `None` = no restriction.
        frontier_bitmap: Option<SurrogateBitmap>,
    },

    /// Immediate 1-hop neighbors lookup.
    Neighbors {
        node_id: String,
        edge_label: Option<String>,
        direction: Direction,
        /// RLS filters applied to neighbor nodes before returning.
        rls_filters: Vec<u8>,
    },

    /// Batched 1-hop neighbors lookup: one RPC per hop of a BFS frontier
    /// instead of one RPC per frontier node. Returns
    /// `[{ src, label, node }, ...]` so the caller can attribute each
    /// neighbor to its origin (needed for shortest-path parent pointers).
    ///
    /// `max_results` is the per-RPC cap: the Data Plane handler stops
    /// emitting entries once the batch reaches this size so a single
    /// wide hop cannot allocate past the caller's budget. `0` means
    /// unbounded (use with care).
    NeighborsMulti {
        node_ids: Vec<String>,
        edge_label: Option<String>,
        direction: Direction,
        max_results: u32,
        /// RLS filters applied to neighbor nodes before returning.
        rls_filters: Vec<u8>,
    },

    /// Shortest path between two nodes.
    Path {
        src: String,
        dst: String,
        edge_label: Option<String>,
        max_depth: usize,
        options: GraphTraversalOptions,
        /// RLS filters applied to path nodes before returning.
        rls_filters: Vec<u8>,
        /// Optional surrogate prefilter restricting which nodes may appear
        /// on the path. `None` = no restriction.
        frontier_bitmap: Option<SurrogateBitmap>,
    },

    /// Materialize a subgraph as edge tuples.
    Subgraph {
        start_nodes: Vec<String>,
        edge_label: Option<String>,
        depth: usize,
        options: GraphTraversalOptions,
        /// RLS filters applied to subgraph nodes/edges before returning.
        rls_filters: Vec<u8>,
    },

    /// GraphRAG fusion: vector search → graph expansion → RRF ranking.
    ///
    /// Two-source form: vector + graph (backwards-compatible; `bm25_query` is `None`).
    /// Three-source form: vector + BM25 text + graph; activated when `bm25_query` is set.
    RagFusion {
        collection: String,
        query_vector: Vec<f32>,
        vector_top_k: usize,
        edge_label: Option<String>,
        direction: Direction,
        expansion_depth: usize,
        final_top_k: usize,
        /// Two-source RRF k constants: (vector_k, graph_k).
        /// Used when `bm25_query` is absent (backwards-compatible two-source form).
        rrf_k: (f64, f64),
        /// Three-source RRF k constants: (vector_k, text_k, graph_k).
        /// Set when the FUSION DSL carries a `BM25 '...' ON '...'` clause.
        rrf_k_triple: Option<(f64, f64, f64)>,
        /// Vector index field name. Empty string selects the raw (field-less)
        /// index created via `VectorOp::Insert`; a non-empty value selects
        /// the field-backed index created when documents are inserted with an
        /// embedded vector column (e.g. `INSERT INTO col (id, embedding) VALUES …`).
        vector_field: String,
        options: GraphTraversalOptions,
        /// BM25 query string for the text leg of three-source fusion. `None` = two-source.
        bm25_query: Option<String>,
        /// Document field on which BM25 scoring is applied. Required when `bm25_query` is set.
        bm25_field: Option<String>,
    },

    /// Graph algorithm execution (PageRank, WCC, SSSP, etc.).
    Algo {
        algorithm: GraphAlgorithm,
        params: AlgoParams,
    },

    /// Graph pattern matching (MATCH clause execution).
    Match {
        /// Serialized `MatchQuery` (MessagePack).
        query: Vec<u8>,
        /// Optional surrogate prefilter restricting which nodes are eligible
        /// as pattern anchors. `None` = no restriction.
        frontier_bitmap: Option<SurrogateBitmap>,
        /// When `true`, the Data Plane emits every bound zero-degree source as
        /// a cross-shard frontier candidate (it has no routing knowledge, so
        /// the Control Plane filters them precisely in B2). When `false` (the
        /// single-node default) no frontier is emitted and the unwrapped rows
        /// payload is byte-identical to a non-cluster MATCH. B2 sets this true
        /// for cluster orchestration.
        cluster_mode: bool,
    },

    /// Cross-shard MATCH continuation (resume a pattern on this shard).
    ///
    /// Dispatched to the shard that owns `source_node` after another shard
    /// emitted an `UnresolvedExpansion` for it. The receiving shard resumes
    /// the SAME (already-optimized) pattern from `resume_triple_idx`, seeded
    /// with `partial_row` plus `source_binding -> source_node`. The query is
    /// carried already-optimized and MUST NOT be re-optimized on resume —
    /// `resume_triple_idx` indexes the originating shard's triple order.
    ///
    /// Phase A returns ROWS ONLY — identical response format to `Match`.
    MatchContinuation {
        /// Serialized (already-optimized) `MatchQuery` (MessagePack).
        query: Vec<u8>,
        /// Within-chain triple index to resume from (originating shard's order).
        resume_triple_idx: usize,
        /// Serialized `HashMap<String, String>` of accumulated bindings (MessagePack).
        partial_row: Vec<u8>,
        /// The node name on THIS shard to resume expansion from.
        source_node: String,
        /// The binding variable bound to `source_node`.
        source_binding: String,
    },

    /// One distributed-PageRank BSP superstep on this shard's local CSR.
    ///
    /// Phase A primitive: the Control-Plane coordinator (Phase B) round-trips
    /// this op once per superstep, threading the per-shard rank vector back in
    /// via `rank_vec` and routing cross-shard contributions to the owning shard
    /// via `incoming_contributions`. The handler is stateless across calls —
    /// all per-superstep state lives in this variant and `BspSuperstepResult`.
    ///
    /// Boxed because the payload (params + three vectors) is large and
    /// `PhysicalPlan` is cloned/moved across the SPSC bridge on every request;
    /// keeping the common variants small avoids bloating the whole enum.
    BspSuperstep(Box<BspSuperstepPlan>),

    /// Set node labels (bitset-based, up to 64 distinct labels).
    SetNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },

    /// Remove node labels.
    RemoveNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },

    /// Bitemporal 1-hop neighbors lookup.
    ///
    /// Resolves edges whose latest version with `system_from <= system_as_of_ms`
    /// (converted to HLC ordinal) is not a sentinel, optionally also filtering
    /// by `valid_from_ms <= valid_at_ms < valid_until_ms`. The handler calls
    /// `ceiling_resolve_edge` per candidate base edge.
    TemporalNeighbors {
        /// Edge store is collection-scoped; current-state `Neighbors` reads
        /// the tenant-wide CSR, but the versioned key layout is
        /// `{collection}\x00...`, so the bitemporal path must name the
        /// collection explicitly.
        collection: String,
        node_id: String,
        edge_label: Option<String>,
        direction: Direction,
        /// System-time selection. `Current` falls back to current-state
        /// semantics identical to `Neighbors`; `AsOf(ms)` is point-in-time.
        /// `AllVersions` returns a typed NotSupported error on graph.
        system_time: SystemTimeScope,
        /// Optional valid-time point. Skipped when `None`.
        valid_at_ms: Option<i64>,
        rls_filters: Vec<u8>,
    },

    /// Bitemporal graph algorithm execution.
    ///
    /// Identical to `Algo` but builds its CSR snapshot via
    /// `CsrSnapshot::from_edge_store_as_of` at the given system-time cutoff
    /// before running the algorithm.
    TemporalAlgorithm {
        algorithm: GraphAlgorithm,
        params: AlgoParams,
        /// System-time selection. `Current` means current state (equivalent to
        /// plain `Algo`); `AsOf(ms)` builds a snapshot at that cutoff.
        /// `AllVersions` returns a typed NotSupported error on graph.
        system_time: SystemTimeScope,
    },

    /// Read persistent graph-stats counters from the edge store.
    ///
    /// `collection = Some` returns stats for one `(tenant, collection)` pair.
    /// `collection = None` returns stats for every collection that has
    /// edges (or had any, per cold-start rebuild) for this tenant.
    ///
    /// `as_of = None` is the O(1) live-snapshot path (reads the cached
    /// summary row). `as_of = Some(ms)` falls back to a historical scan.
    Stats {
        collection: Option<String>,
        as_of: Option<i64>,
    },
}

/// Boxed payload of [`GraphOp::BspSuperstep`] — all per-superstep inputs.
///
/// Kept out-of-line (the variant holds a `Box`) so the large param + vector
/// fields don't bloat `PhysicalPlan`, which is cloned/moved on every request.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BspSuperstepPlan {
    /// Algorithm selector. Only `PageRank` is supported in Phase A; other
    /// variants surface a typed `Unsupported` error from the handler.
    pub algorithm: GraphAlgorithm,
    /// Algorithm parameters. Carries the target `collection` (mirroring `Algo`)
    /// plus `damping`.
    pub params: AlgoParams,
    /// Zero-based superstep index. `0` triggers `1/global_n` initialization.
    pub superstep: u32,
    /// Total OWNED nodes across all shards (Control-Plane computed). Used as the
    /// PageRank `n` in the teleport / dangling redistribution terms.
    ///
    /// `global_n == 0` is the COUNT-ONLY sentinel: the coordinator dispatches one
    /// superstep with `global_n = 0` (and empty `rank_seed` / `incoming_contributions`)
    /// to every shard BEFORE superstep 0 so it can sum each shard's owned
    /// `vertex_count` into the real `global_n`. On that sentinel the handler
    /// short-circuits after building the owned-node set and runs NO superstep —
    /// it returns only `vertex_count` + `node_names`. Every real superstep
    /// (`superstep >= 0` of the actual run) passes `global_n > 0`.
    pub global_n: usize,
    /// The vShards this shard owns (Control-Plane supplied). A destination node
    /// whose `VShardId::from_key(name)` is not in this set is a ghost
    /// (cross-shard) edge target and its contribution is emitted in `outbound`
    /// rather than scattered locally.
    pub owned_vshards: Vec<u32>,
    /// Cross-shard contributions routed to THIS shard's owned nodes for this
    /// superstep: `(dst_node_name, contribution)`.
    pub incoming_contributions: Vec<(String, f64)>,
    /// Round-tripped per-shard rank seed as `(node_name, rank)` pairs (name-keyed,
    /// NOT positional) so the same plan can be fanned across a node's cores and
    /// each core self-filters to its owned nodes by name. EMPTY on superstep 0 →
    /// the handler initializes every owned node to `1/global_n`. A node absent from
    /// the seed also falls back to `1/global_n`.
    pub rank_seed: Vec<(String, f64)>,
}

/// Result of one [`GraphOp::BspSuperstep`] on a single shard.
///
/// `rank_vec` and `node_names` are positionally aligned: `rank_vec[i]` is the
/// post-superstep PageRank of the owned node `node_names[i]`. The Control-Plane
/// coordinator (Phase B) round-trips `rank_vec` back into the next superstep's
/// `GraphOp::BspSuperstep::rank_vec` and uses `node_names` to map indices back
/// to node identities for final assembly and for routing `outbound`
/// contributions to the owning shard. `node_names` is returned on every
/// superstep (it is cheap and keeps the op stateless).
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BspSuperstepResult {
    /// Sum of `|rank_old - rank_new|` over this shard's owned nodes — the
    /// shard's contribution to the global convergence delta.
    pub local_delta: f64,
    /// Cross-shard contributions to scatter to other shards next superstep:
    /// `(target_vshard, dst_node_name, contribution)`.
    pub outbound: Vec<(u32, String, f64)>,
    /// Post-superstep rank vector over this shard's owned nodes, aligned with
    /// `node_names`.
    pub rank_vec: Vec<f64>,
    /// Number of owned nodes on this shard (== `rank_vec.len()`).
    pub vertex_count: usize,
    /// Owned-node names, positionally aligned with `rank_vec`.
    pub node_names: Vec<String>,
}
