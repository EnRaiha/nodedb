// SPDX-License-Identifier: Apache-2.0

//! Layer assignment and distance computation for [`HnswIndex`].

use nodedb_types::vector_dtype::VectorStorageDtype;

use super::HnswIndex;
use crate::distance::dispatch::distance_typed;
use crate::distance::distance;
use crate::dtype::cast_from_f32;
use crate::hnsw::graph::MAX_LAYER_CAP;

impl HnswIndex {
    /// Assign a random layer using the exponential distribution.
    ///
    /// Capped at `MAX_LAYER_CAP` to prevent pathological RNG draws from
    /// promoting the index's `max_layer` to hundreds or thousands, which
    /// would make every search's Phase-1 greedy descent O(max_layer).
    pub(crate) fn random_layer(&mut self) -> usize {
        let ml = 1.0 / (self.params.m as f64).ln();
        let r = self.rng.next_f64().max(f64::MIN_POSITIVE);
        let layer = (-r.ln() * ml).floor() as usize;
        layer.min(MAX_LAYER_CAP)
    }

    /// Compute distance between a pre-encoded query and a stored node.
    ///
    /// `query_bytes` must already be encoded in `self.params.dtype`; callers
    /// encode once at the top of search/insert and pass the same buffer to
    /// every `dist_to_node` call within that operation.
    ///
    /// When the node's local vector storage is empty (graph-checkpoint-only
    /// restore) and a `backing` is attached, the vector bytes are fetched from
    /// the backing.  This is the Lite cold-load path; Origin never hits it.
    ///
    /// A node whose vector cannot be resolved scores [`f32::INFINITY`] — search
    /// then never selects it — rather than panicking. `with_backing` rejects a
    /// backing that cannot serve the index, so reaching that branch means a node
    /// has no vector source at all; aborting a search worker (and poisoning the
    /// locks it held) is strictly worse than ranking one node last.
    pub(crate) fn dist_to_node(&self, query_bytes: &[u8], node_id: u32) -> f32 {
        let node_bytes = self.nodes[node_id as usize].storage.as_bytes();
        #[cfg(not(target_arch = "wasm32"))]
        let node_bytes: &[u8] = if node_bytes.is_empty() {
            if let Some(ref b) = self.backing {
                // Backing stores F32 only; convert slice to bytes for distance_typed.
                if let Some(v) = b.get_vector(node_id) {
                    // SAFETY: &[f32] → &[u8] cast via bytemuck is always safe.
                    bytemuck::cast_slice(v)
                } else {
                    node_bytes
                }
            } else {
                node_bytes
            }
        } else {
            node_bytes
        };
        distance_typed(
            self.params.metric,
            self.params.dtype,
            query_bytes,
            node_bytes,
            self.dim,
        )
        .unwrap_or(f32::INFINITY)
    }

    /// Compute distance between a query given as `&[f32]` and a stored node.
    ///
    /// For F32 indexes this is a direct call to `distance`. For F16/BF16
    /// indexes the query is encoded to the storage dtype on each call, which
    /// is an allocation. Prefer pre-encoding the query once via
    /// [`crate::dtype::cast_from_f32`] and calling [`Self::dist_to_node`]
    /// for hot-path code such as search.
    #[allow(dead_code)]
    pub(crate) fn dist_to_node_f32(&self, query: &[f32], node_id: u32) -> f32 {
        match self.params.dtype {
            VectorStorageDtype::F32 => distance(
                query,
                self.nodes[node_id as usize]
                    .storage
                    .as_f32_slice()
                    .expect("F32 dtype must have F32 storage"),
                self.params.metric,
            ),
            _ => {
                let query_bytes = cast_from_f32(query, self.params.dtype);
                self.dist_to_node(&query_bytes, node_id)
            }
        }
    }

    /// Max neighbors allowed at a given layer.
    pub(crate) fn max_neighbors(&self, layer: usize) -> usize {
        if layer == 0 {
            self.params.m0
        } else {
            self.params.m
        }
    }
}
