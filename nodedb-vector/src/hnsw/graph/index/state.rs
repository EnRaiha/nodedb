// SPDX-License-Identifier: Apache-2.0

//! The `HnswIndex` value itself: its fields, its constructors, and the
//! read-only properties that describe how it was built.

use std::cell::RefCell;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use nodedb_types::hnsw::HnswParams;
use nodedb_types::vector_dtype::VectorStorageDtype;

use crate::hnsw::arena::BeamSearchArena;

use super::super::ARENA_INITIAL_CAPACITY;
use super::super::types::{Node, Xorshift64};

/// Hierarchical Navigable Small World graph index.
///
/// - FP32 construction for structural integrity
/// - Heuristic neighbor selection (Algorithm 4)
/// - Beam search with configurable ef parameter
pub struct HnswIndex {
    pub(crate) params: HnswParams,
    pub(crate) dim: usize,
    pub(crate) nodes: Vec<Node>,
    pub(crate) entry_point: Option<u32>,
    pub(crate) max_layer: usize,
    pub(crate) rng: Xorshift64,
    /// Flat neighbor storage for zero-copy access after checkpoint restore.
    /// When present, `neighbors_at()` reads from here instead of per-node Vecs.
    /// Cleared on first mutation (insert/delete).
    pub(crate) flat_neighbors: Option<crate::hnsw::flat_neighbors::FlatNeighborStore>,
    /// Optional backing store for vector data.
    ///
    /// When set (graph-checkpoint-only restore path), per-node vector storage
    /// is left empty and `dist_to_node` falls through to the backing.  Origin
    /// never sets this field; it is only used by Lite's pagedb segment path.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) backing: Option<Arc<dyn crate::segment_backing::VectorSegmentBacking>>,
    /// Per-invocation scratch arena for beam-search heaps.
    ///
    /// Wrapped in `RefCell` so search methods keep `&self` receivers without
    /// forcing `&mut self` across all call sites.  The borrow is taken at the
    /// start of `search_layer` and released before returning.  The arena must
    /// never be borrowed twice simultaneously — it is a per-call scratch buffer
    /// owned exclusively by one Data Plane core.
    pub(crate) arena: RefCell<BeamSearchArena>,
}

impl HnswIndex {
    /// Create a new empty HNSW index.
    pub fn new(dim: usize, params: HnswParams) -> Self {
        Self::seeded(dim, params, 42)
    }

    /// Create with a specific RNG seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, seed: u64) -> Self {
        Self::seeded(dim, params, seed)
    }

    fn seeded(dim: usize, params: HnswParams, seed: u64) -> Self {
        let initial_capacity = params.ef_construction.max(ARENA_INITIAL_CAPACITY);
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(seed),
            flat_neighbors: None,
            arena: RefCell::new(BeamSearchArena::new(initial_capacity)),
            params,
            #[cfg(not(target_arch = "wasm32"))]
            backing: None,
        }
    }

    /// The distance metric this index was built with. Search-time metric
    /// overrides must match this; differing metrics require either rebuilding
    /// the index or a metric-aware re-rank pass.
    pub fn metric(&self) -> crate::distance::DistanceMetric {
        self.params.metric
    }

    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Storage dtype this index was constructed with.
    pub fn dtype(&self) -> VectorStorageDtype {
        self.params.dtype
    }

    pub fn entry_point(&self) -> Option<u32> {
        self.entry_point
    }

    pub fn max_layer(&self) -> usize {
        self.max_layer
    }

    /// Current RNG state (for snapshot reproducibility).
    pub fn rng_state(&self) -> u64 {
        self.rng.0
    }

    /// Approximate memory usage in bytes (vector data + neighbor lists).
    pub fn memory_usage_bytes(&self) -> usize {
        let vector_bytes = self.nodes.len() * self.params.dtype.bytes_for_dim(self.dim);
        let neighbor_bytes: usize = self
            .nodes
            .iter()
            .map(|n| {
                n.neighbors
                    .iter()
                    .map(|layer| layer.len() * 4)
                    .sum::<usize>()
            })
            .sum();
        let node_overhead = self.nodes.len() * std::mem::size_of::<Node>();
        vector_bytes + neighbor_bytes + node_overhead
    }
}
