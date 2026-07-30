// SPDX-License-Identifier: Apache-2.0

//! Attaching an external vector segment to a graph-only index.
//!
//! Lite's restore path loads the graph topology from a B+ tree blob while the
//! vectors stay in a pagedb segment, so the index arrives with empty per-node
//! storage and has to be pointed at that segment before it can answer anything.
//!
//! The attachment is where a bad segment has to be caught. Once attached, an
//! unserviceable segment is indistinguishable from a healthy one until a query
//! scores a node that has no vector — by which time the collection is already
//! published and serving.

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use crate::error::VectorError;

use super::state::HnswIndex;

impl HnswIndex {
    /// Attach a [`crate::segment_backing::VectorSegmentBacking`] to this index.
    ///
    /// After this succeeds, `dist_to_node` falls back to the backing whenever a
    /// node's local vector storage is empty.  This is used by Lite's
    /// graph-checkpoint-only restore path: the graph topology is loaded from the
    /// B+ tree blob, but vector data lives in a pagedb segment.
    ///
    /// Origin never calls this — its node arenas are always populated.
    ///
    /// # Errors
    ///
    /// The backing is validated against this index BEFORE being attached, and is
    /// not attached at all when it cannot serve every node whose local storage
    /// is empty. An attached-but-unserviceable backing is the worst outcome
    /// available: the graph looks healthy, so search proceeds and then finds no
    /// vector for a node it must score. Callers should treat an error as "this
    /// segment is unusable — rebuild the index from the authoritative vectors,
    /// or leave the collection unloaded", never as something to ignore.
    ///
    /// - [`VectorError::DimensionMismatch`] if the backing's `dim()` is not this
    ///   index's `dim`.
    /// - [`VectorError::VectorUnavailable`] if the backing holds fewer vectors
    ///   than the index has nodes, or if a node that needs the backing has no
    ///   correctly-sized vector in it.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_backing(
        &mut self,
        b: Arc<dyn crate::segment_backing::VectorSegmentBacking>,
    ) -> Result<&mut Self, VectorError> {
        if b.dim() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: b.dim(),
            });
        }
        if b.len() < self.nodes.len() {
            // A segment written from an index that had no vectors to give
            // declares its real count in the header but carries no payload;
            // this is where that lie becomes visible. `b.len()` is also the id
            // of the first node the backing cannot serve.
            return Err(VectorError::VectorUnavailable { id: b.len() as u32 });
        }
        // Every node that will actually read through the backing must resolve to
        // a correctly-sized vector. Nodes with local storage are unaffected.
        for (id, node) in self.nodes.iter().enumerate() {
            if !node.storage.as_bytes().is_empty() {
                continue;
            }
            let id = id as u32;
            match b.get_vector(id) {
                Some(v) if v.len() == self.dim => {}
                _ => return Err(VectorError::VectorUnavailable { id }),
            }
        }
        self.backing = Some(b);
        Ok(self)
    }

    /// Fetch node `id`'s vector from the attached segment backing.
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn backing_vector(&self, id: u32) -> Result<Vec<f32>, VectorError> {
        self.backing
            .as_ref()
            .and_then(|b| b.get_vector(id))
            .map(<[f32]>::to_vec)
            .ok_or(VectorError::VectorUnavailable { id })
    }

    /// WASM targets have no segment backing (it requires mmap).
    #[cfg(target_arch = "wasm32")]
    pub(super) fn backing_vector(&self, id: u32) -> Result<Vec<f32>, VectorError> {
        Err(VectorError::VectorUnavailable { id })
    }
}
