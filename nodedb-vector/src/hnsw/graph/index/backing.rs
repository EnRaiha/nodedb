// SPDX-License-Identifier: Apache-2.0

//! Attaching an external vector segment to a graph-only index.
//!
//! Lite's restore path loads the graph topology from a B+ tree blob while the
//! vectors stay in a pagedb segment, so the index arrives with empty per-node
//! storage and has to be pointed at that segment before it can answer anything.

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use crate::error::VectorError;

use super::state::HnswIndex;

impl HnswIndex {
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
