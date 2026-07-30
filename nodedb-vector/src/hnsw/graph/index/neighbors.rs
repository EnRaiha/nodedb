// SPDX-License-Identifier: Apache-2.0

//! Neighbor-list access.
//!
//! After a checkpoint restore the lists live in one flat, zero-copy store; a
//! mutation materializes them back into per-node `Vec`s. Both readers below
//! consult the flat store first so callers never have to know which
//! representation is currently in use.

use super::state::HnswIndex;

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

    /// Export all neighbor lists for snapshot transfer.
    pub fn export_neighbors(&self) -> Vec<Vec<Vec<u32>>> {
        self.nodes.iter().map(|n| n.neighbors.clone()).collect()
    }
}
