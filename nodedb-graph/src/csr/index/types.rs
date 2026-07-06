// SPDX-License-Identifier: Apache-2.0

//! `CsrIndex` struct definition and constructor.
//!
//! Memory layout at scale (1B edges):
//! - Old: `Vec<Vec<(String, u32)>>` ≈ 60 GB (heap String per edge)
//! - New: contiguous `Vec<u32>` offsets + targets + `Vec<u32>` labels ≈ 12 GB
//!
//! Writes accumulate in a mutable buffer (`buffer_out`/`buffer_in`).
//! Reads check both the dense CSR arrays and the mutable buffer.
//! `compact()` merges the buffer into the dense arrays (double-buffered swap).
//!
//! ## Edge Weights
//!
//! Optional `f64` weight per edge stored in parallel arrays. `None` when the
//! graph is entirely unweighted (zero memory overhead). Populated from the
//! `"weight"` edge property at insertion time. Unweighted edges default to 1.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nodedb_mem::MemoryGovernor;

use crate::csr::dense_array::DenseArray;

// Re-export shared Direction from nodedb-types.
pub use nodedb_types::graph::Direction;

/// Dense integer CSR adjacency index with interned node IDs and labels.
pub struct CsrIndex {
    // ── Node interning ──
    pub(crate) node_to_id: HashMap<String, u32>,
    pub(crate) id_to_node: Vec<String>,

    // ── Label interning ──
    pub(crate) label_to_id: HashMap<String, u32>,
    pub(crate) id_to_label: Vec<String>,

    // ── Collection interning ──
    //
    // Every edge carries the id of the collection it was inserted under.
    // Nodes (and their label bitsets / surrogates) are shared across
    // collections within a `(database, tenant)` partition — only edges are
    // collection-scoped — so the collection axis lives on edges, not on the
    // partition key. `""` (the empty string) is the reserved "unscoped"
    // collection used by throwaway CSRs built for algorithms / tests via the
    // collection-less `add_edge` / `add_edge_weighted` entry points.
    pub(crate) collection_to_id: HashMap<String, u32>,
    pub(crate) id_to_collection: Vec<String>,

    // ── Dense CSR (read-only between compactions) ──
    //
    // Offsets are `Vec<u32>` (mutable — extended on node creation).
    // Targets/labels/weights use `DenseArray<T>` for zero-copy mmap support:
    // after cold start from rkyv checkpoint, these point directly into the
    // archived buffer with no deserialization. Compaction replaces them with
    // owned Vecs.
    /// `out_offsets[i]..out_offsets[i+1]` = range in `out_targets`/`out_labels`.
    /// Length: `num_nodes + 1`.
    pub(crate) out_offsets: Vec<u32>,
    pub(crate) out_targets: DenseArray<u32>,
    pub(crate) out_labels: DenseArray<u32>,
    /// Parallel outbound edge collection-id array. Same length/order as
    /// `out_targets` / `out_labels`: `out_collections[i]` is the collection id
    /// of the edge whose target/label live at index `i`. Plain `Vec` (not a
    /// zero-copy `DenseArray`) because it is small relative to targets and is
    /// only read on the collection-scoped MATCH / RAG paths.
    pub(crate) out_collections: Vec<u32>,
    /// Parallel edge weight array. `None` if graph has no weighted edges.
    pub(crate) out_weights: Option<DenseArray<f64>>,

    pub(crate) in_offsets: Vec<u32>,
    pub(crate) in_targets: DenseArray<u32>,
    pub(crate) in_labels: DenseArray<u32>,
    /// Parallel inbound edge collection-id array (see `out_collections`).
    pub(crate) in_collections: Vec<u32>,
    /// Parallel inbound edge weight array. `None` if graph has no weighted edges.
    pub(crate) in_weights: Option<DenseArray<f64>>,

    // ── Mutable write buffer ──
    /// Per-node outbound buffer: `buffer_out[node_id]` = `[(label_id, dst_id)]`.
    pub(crate) buffer_out: Vec<Vec<(u32, u32)>>,
    pub(crate) buffer_in: Vec<Vec<(u32, u32)>>,
    /// Per-node outbound weight buffer (parallel to `buffer_out`).
    /// Only populated when `has_weights` is true.
    pub(crate) buffer_out_weights: Vec<Vec<f64>>,
    /// Per-node inbound weight buffer (parallel to `buffer_in`).
    pub(crate) buffer_in_weights: Vec<Vec<f64>>,
    /// Per-node outbound collection-id buffer (parallel to `buffer_out`):
    /// `buffer_out_collections[node][k]` is the collection id of the edge
    /// `buffer_out[node][k]`. Always maintained (unlike weights, which are
    /// gated on `has_weights`).
    pub(crate) buffer_out_collections: Vec<Vec<u32>>,
    /// Per-node inbound collection-id buffer (parallel to `buffer_in`).
    pub(crate) buffer_in_collections: Vec<Vec<u32>>,

    /// Edges deleted since last compaction, keyed by full edge identity
    /// `(src, label, dst, collection)`. The collection is part of the key so
    /// the SAME `(src, label, dst)` triple inserted under two collections
    /// forms two DISTINCT edges: deleting collection A's copy leaves
    /// collection B's copy live.
    pub(crate) deleted_edges: HashSet<(u32, u32, u32, u32)>,

    /// Whether any edge has a non-default weight. When false, weight arrays
    /// are `None` and weight buffers are empty — zero overhead for unweighted graphs.
    pub(crate) has_weights: bool,

    // ── Node labels (bitset) ──
    //
    // Each node has a `u64` bitset where bit `i` corresponds to label ID `i`.
    // Supports up to 64 distinct node labels. Labels are interned in
    // `node_label_to_id` / `node_label_names` (separate from edge labels).
    // Used by MATCH pattern `(a:Person)` — filters nodes by label membership.
    pub(crate) node_label_bits: Vec<u64>,
    pub(crate) node_label_to_id: HashMap<String, u8>,
    pub(crate) node_label_names: Vec<String>,

    // ── Surrogate storage ──
    /// Per-node surrogate: `node_surrogates[local_id]` = global `Surrogate.as_u32()`.
    ///
    /// `0` (Surrogate::ZERO) is the unset sentinel — populated at `EdgePut` time
    /// from the surrogates resolved by the Control Plane. A node whose surrogate
    /// is zero was inserted without surrogate plumbing (e.g. legacy paths or tests)
    /// and is treated as "not in any prefilter bitmap" when a bitmap is active.
    pub(crate) node_surrogates: Vec<u32>,
    /// Reverse map: `Surrogate.as_u32()` → CSR-local node id. Maintained
    /// in step with `node_surrogates` by `set_node_surrogate`. Excludes the
    /// zero sentinel. Used by cross-engine fusion (graph RAG) to resolve a
    /// vector-side surrogate to the corresponding graph node name.
    pub(crate) surrogate_to_local: HashMap<u32, u32>,

    // ── Hot/cold access tracking ──
    /// Per-node access counter: incremented on each neighbor/BFS/path query.
    /// Uses `Cell<u32>` so access can be tracked through `&self` references
    /// (traversal methods are `&self` for shared read access).
    pub(crate) access_counts: Vec<std::cell::Cell<u32>>,
    /// Total queries served since last access counter reset.
    pub(crate) query_epoch: u64,

    /// Unique partition tag assigned at construction. Embedded into
    /// every `LocalNodeId` this index produces; cross-partition use is
    /// caught by comparing tags at API boundaries.
    pub(crate) partition_tag: u32,

    /// Optional memory governor for budget tracking.
    ///
    /// When `None`, all memory operations proceed without budget enforcement
    /// (the behavior for NodeDB-Lite / WASM deployments that have no governor).
    /// When `Some`, `compact()`, `checkpoint_to_bytes()`, and `compute_statistics()`
    /// reserve bytes against `EngineId::Graph` before allocating and release them
    /// on drop via `BudgetGuard`.
    pub(crate) governor: Option<Arc<MemoryGovernor>>,
}

impl Default for CsrIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl CsrIndex {
    pub fn new() -> Self {
        Self {
            node_to_id: HashMap::new(),
            id_to_node: Vec::new(),
            label_to_id: HashMap::new(),
            id_to_label: Vec::new(),
            collection_to_id: HashMap::new(),
            id_to_collection: Vec::new(),
            out_offsets: vec![0],
            out_targets: DenseArray::default(),
            out_labels: DenseArray::default(),
            out_collections: Vec::new(),
            out_weights: None,
            in_offsets: vec![0],
            in_targets: DenseArray::default(),
            in_labels: DenseArray::default(),
            in_collections: Vec::new(),
            in_weights: None,
            buffer_out: Vec::new(),
            buffer_in: Vec::new(),
            buffer_out_weights: Vec::new(),
            buffer_in_weights: Vec::new(),
            buffer_out_collections: Vec::new(),
            buffer_in_collections: Vec::new(),
            deleted_edges: HashSet::new(),
            has_weights: false,
            node_label_bits: Vec::new(),
            node_label_to_id: HashMap::new(),
            node_label_names: Vec::new(),
            node_surrogates: Vec::new(),
            surrogate_to_local: HashMap::new(),
            access_counts: Vec::new(),
            query_epoch: 0,
            partition_tag: crate::csr::local_node_id::next_partition_tag(),
            governor: None,
        }
    }

    /// Create a new `CsrIndex` wired to a memory governor.
    ///
    /// Subsequent calls to `compact()`, `checkpoint_to_bytes()`, and
    /// `compute_statistics()` will reserve bytes against `EngineId::Graph`
    /// before allocating and return `Err(GraphError::MemoryBudget(_))` if
    /// the budget is exhausted.
    ///
    /// Use `CsrIndex::new()` when deploying without a governor (NodeDB-Lite,
    /// WASM, or tests that do not need budget enforcement).
    pub fn with_governor(governor: Arc<MemoryGovernor>) -> Self {
        Self {
            governor: Some(governor),
            ..Self::new()
        }
    }

    /// Attach a memory governor to an existing `CsrIndex`.
    ///
    /// Used by the REINDEX path: a partition is rebuilt on a background thread
    /// (without a governor since `MemoryGovernor` is `Arc<...>` but the thread
    /// is independent), and on cutover the Data Plane installs the governor.
    pub fn with_governor_attached(mut self, governor: Arc<MemoryGovernor>) -> Self {
        self.governor = Some(governor);
        self
    }

    /// Whether any edge in this index has a non-default (1.0) weight.
    pub fn has_weighted_edges(&self) -> bool {
        self.has_weights
    }
}
