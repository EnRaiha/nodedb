// SPDX-License-Identifier: Apache-2.0

//! Tombstone compaction for [`HnswIndex`]: drops deleted nodes and remaps ids.

use super::HnswIndex;
use crate::hnsw::graph::types::Node;

impl HnswIndex {
    /// Compact the index by removing all tombstoned nodes.
    ///
    /// Returns the number of removed nodes. See `compact_with_map` for the
    /// variant that also returns the old→new id remapping.
    pub fn compact(&mut self) -> usize {
        self.compact_with_map().0
    }

    /// Compact and return both the removed count and the old→new id map.
    ///
    /// `id_map[old_local]` = new_local, or `u32::MAX` if the node was
    /// tombstoned (removed).
    pub fn compact_with_map(&mut self) -> (usize, Vec<u32>) {
        let tombstone_count = self.tombstone_count();
        if tombstone_count == 0 {
            let identity: Vec<u32> = (0..self.nodes.len() as u32).collect();
            return (0, identity);
        }
        self.ensure_mutable_neighbors();

        let mut id_map: Vec<u32> = Vec::with_capacity(self.nodes.len());
        let mut new_id = 0u32;
        for node in &self.nodes {
            if node.deleted {
                id_map.push(u32::MAX);
            } else {
                id_map.push(new_id);
                new_id += 1;
            }
        }

        let mut new_nodes: Vec<Node> = Vec::with_capacity(new_id as usize);
        for node in self.nodes.drain(..) {
            if node.deleted {
                continue;
            }
            let remapped_neighbors: Vec<Vec<u32>> = node
                .neighbors
                .into_iter()
                .map(|layer_neighbors| {
                    layer_neighbors
                        .into_iter()
                        .filter_map(|old_nid| {
                            let new_nid = id_map[old_nid as usize];
                            if new_nid == u32::MAX {
                                None
                            } else {
                                Some(new_nid)
                            }
                        })
                        .collect()
                })
                .collect();
            new_nodes.push(Node {
                storage: node.storage,
                neighbors: remapped_neighbors,
                deleted: false,
            });
        }

        self.entry_point = if let Some(old_ep) = self.entry_point {
            let new_ep = id_map[old_ep as usize];
            if new_ep == u32::MAX {
                new_nodes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, n)| n.neighbors.len())
                    .map(|(i, _)| i as u32)
            } else {
                Some(new_ep)
            }
        } else {
            None
        };

        self.max_layer = new_nodes
            .iter()
            .map(|n| n.neighbors.len().saturating_sub(1))
            .max()
            .unwrap_or(0);

        self.nodes = new_nodes;
        (tombstone_count, id_map)
    }
}
