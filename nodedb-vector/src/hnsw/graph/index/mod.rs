// SPDX-License-Identifier: Apache-2.0

mod compact;
mod distance_ops;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use crate::hnsw::arena::BeamSearchArena;
use nodedb_types::vector_dtype::VectorStorageDtype;

use super::ARENA_INITIAL_CAPACITY;
use super::types::{Node, NodeStorage, Xorshift64};
pub use nodedb_types::hnsw::HnswParams;

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
    /// Get neighbors of a node at a specific layer.
    /// Uses flat zero-copy storage if available, otherwise per-node Vec.
    #[inline]
    pub(crate) fn neighbors_at(&self, node_id: u32, layer: usize) -> &[u32] {
        if let Some(ref flat) = self.flat_neighbors {
            return flat.neighbors_at(node_id, layer);
        }
        let node = &self.nodes[node_id as usize];
        if layer < node.neighbors.len() {
            &node.neighbors[layer]
        } else {
            &[]
        }
    }

    /// Number of layers a node participates in.
    #[inline]
    pub(crate) fn node_num_layers(&self, node_id: u32) -> usize {
        if let Some(ref flat) = self.flat_neighbors {
            return flat.num_layers(node_id);
        }
        self.nodes[node_id as usize].neighbors.len()
    }

    /// Ensure mutable per-node neighbor Vecs are available.
    /// Materializes flat storage back to per-node Vecs if needed.
    pub(crate) fn ensure_mutable_neighbors(&mut self) {
        if let Some(flat) = self.flat_neighbors.take() {
            let nested = flat.to_nested(self.nodes.len());
            for (i, layers) in nested.into_iter().enumerate() {
                self.nodes[i].neighbors = layers;
            }
        }
    }
}

impl HnswIndex {
    /// The distance metric this index was built with. Search-time metric
    /// overrides must match this; differing metrics require either rebuilding
    /// the index or a metric-aware re-rank pass.
    pub fn metric(&self) -> crate::distance::DistanceMetric {
        self.params.metric
    }

    /// Create a new empty HNSW index.
    pub fn new(dim: usize, params: HnswParams) -> Self {
        let initial_capacity = params.ef_construction.max(ARENA_INITIAL_CAPACITY);
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(42),
            flat_neighbors: None,
            arena: RefCell::new(BeamSearchArena::new(initial_capacity)),
            params,
            #[cfg(not(target_arch = "wasm32"))]
            backing: None,
        }
    }

    /// Create with a specific RNG seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, seed: u64) -> Self {
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

    /// Attach a [`VectorSegmentBacking`] to this index.
    ///
    /// After calling this, `dist_to_node` will fall back to the backing whenever
    /// a node's local vector storage is empty.  This is used by Lite's
    /// graph-checkpoint-only restore path: the graph topology is loaded from the
    /// B+ tree blob, but vector data lives in a pagedb segment.
    ///
    /// Origin never calls this — its node arenas are always populated.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_backing(
        &mut self,
        b: Arc<dyn crate::segment_backing::VectorSegmentBacking>,
    ) -> &mut Self {
        self.backing = Some(b);
        self
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn live_count(&self) -> usize {
        self.nodes.len() - self.tombstone_count()
    }

    pub fn tombstone_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.deleted).count()
    }

    /// Tombstone ratio: fraction of nodes that are deleted.
    pub fn tombstone_ratio(&self) -> f64 {
        if self.nodes.is_empty() {
            0.0
        } else {
            self.tombstone_count() as f64 / self.nodes.len() as f64
        }
    }

    pub fn is_empty(&self) -> bool {
        self.live_count() == 0
    }

    /// Soft-delete a vector by internal node ID.
    pub fn delete(&mut self, id: u32) -> bool {
        if let Some(node) = self.nodes.get_mut(id as usize) {
            if node.deleted {
                return false;
            }
            node.deleted = true;
            true
        } else {
            false
        }
    }

    pub fn is_deleted(&self, id: u32) -> bool {
        self.nodes.get(id as usize).is_none_or(|n| n.deleted)
    }

    pub fn undelete(&mut self, id: u32) -> bool {
        if let Some(node) = self.nodes.get_mut(id as usize)
            && node.deleted
        {
            node.deleted = false;
            return true;
        }
        false
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Storage dtype this index was constructed with.
    pub fn dtype(&self) -> VectorStorageDtype {
        self.params.dtype
    }

    /// Returns a `&[f32]` view of the stored vector for node `id`.
    ///
    /// Returns `Some` only when the index dtype is `F32`. For `F16` or `BF16`
    /// indexes this method returns `None` — use [`Self::get_vector_bytes`]
    /// instead and decode via [`crate::dtype::cast_to_f32`] if an f32 view is
    /// needed.
    ///
    /// In debug builds, calling this on a non-F32 index triggers a
    /// `debug_assert!` to flag misuse early. In release builds the
    /// `debug_assert!` is a no-op and `None` is returned silently.
    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        debug_assert!(
            self.params.dtype == VectorStorageDtype::F32,
            "get_vector: called on non-F32 index (dtype={}); use get_vector_bytes instead",
            self.params.dtype,
        );
        self.nodes
            .get(id as usize)
            .and_then(|n| n.storage.as_f32_slice())
    }

    /// Dtype-agnostic byte view of the stored vector for node `id`.
    ///
    /// Returns `None` if `id` is out of range. Pair the returned slice with
    /// [`Self::dtype`] to interpret the encoding.
    pub fn get_vector_bytes(&self, id: u32) -> Option<&[u8]> {
        self.nodes.get(id as usize).map(|n| n.storage.as_bytes())
    }

    /// Returns a `&[f32]` view of the stored vector for node `id`, consulting
    /// the pagedb segment backing when the node's local storage is empty.
    ///
    /// This is the rerank-safe variant for Lite's graph-checkpoint-only restore
    /// path: after `from_checkpoint` + `with_backing`, per-node vectors are
    /// empty placeholders and must be fetched through the backing.
    ///
    /// Returns `None` when `id` is out of range, the node has no local vector
    /// and no backing is set, or the backing does not contain `id`.
    ///
    /// Only available on non-WASM targets (the backing type requires mmap).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_vector_or_backing(&self, id: u32) -> Option<&[f32]> {
        let node = self.nodes.get(id as usize)?;
        let local = node.storage.as_f32_slice();
        // If local storage is non-empty, return it directly.
        if let Some(v) = local
            && !v.is_empty()
        {
            return Some(v);
        }
        // Local storage is empty — try the segment backing.
        if let Some(ref b) = self.backing {
            return b.get_vector(id);
        }
        // No backing and empty local storage: caller gets None.
        None
    }

    /// Extract all node vectors as owned F32 vecs for segment serialization.
    ///
    /// Non-F32 nodes are decoded to F32 via byte-level reinterpretation or
    /// dtype conversion.  Nodes whose storage is empty (graph-checkpoint-only
    /// restore) produce an empty vec for that slot.
    ///
    /// The second tuple element is always empty — `HnswIndex` has no surrogate
    /// map.  Surrogates live at the `VectorCollection` layer in Origin.  Lite
    /// passes an empty slice so `write_vector_segment` writes no surrogate block.
    pub fn extract_vectors_and_surrogates(&self) -> (Vec<Vec<f32>>, Vec<u64>) {
        let vectors = self
            .nodes
            .iter()
            .map(|node| match &node.storage {
                super::types::NodeStorage::F32(v) => v.clone(),
                super::types::NodeStorage::Bytes { bytes, dtype } => {
                    crate::dtype::cast_to_f32(bytes, *dtype, self.dim).unwrap_or_default()
                }
            })
            .collect();
        (vectors, Vec::new())
    }

    pub fn params(&self) -> &HnswParams {
        &self.params
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

    /// Export all vectors as F32 for snapshot transfer.
    ///
    /// For F32 indexes this is a clone. For F16/BF16 indexes each vector is
    /// decoded to F32 on the fly.
    pub fn export_vectors(&self) -> Vec<Vec<f32>> {
        self.nodes
            .iter()
            .map(|n| match &n.storage {
                NodeStorage::F32(v) => v.clone(),
                NodeStorage::Bytes { dtype, bytes } => {
                    crate::dtype::cast_to_f32(bytes, *dtype, self.dim)
                        .expect("export_vectors: byte-length invariant violated")
                }
            })
            .collect()
    }

    /// Export all neighbor lists for snapshot transfer.
    pub fn export_neighbors(&self) -> Vec<Vec<Vec<u32>>> {
        self.nodes.iter().map(|n| n.neighbors.clone()).collect()
    }
}
