// SPDX-License-Identifier: Apache-2.0

//! CSR compaction: merges the mutable write buffer into dense arrays.

use super::index::CsrIndex;
use crate::GraphError;

impl CsrIndex {
    /// Merge the mutable buffer into the dense CSR arrays.
    ///
    /// Called during idle periods. Rebuilds the contiguous offset/target/label
    /// (and weight) arrays from scratch (buffer + surviving dense edges).
    /// The old arrays are dropped, freeing memory. O(E) where E = total edges.
    ///
    /// # Errors
    ///
    /// Returns [`GraphError::MemoryBudget`] if a memory governor is installed
    /// and the dense-array allocation would exceed the `Graph` engine budget.
    pub fn compact(&mut self) -> Result<(), GraphError> {
        let n = self.id_to_node.len();
        let mut new_out_edges: Vec<Vec<(u32, u32)>> = vec![Vec::new(); n];
        let mut new_in_edges: Vec<Vec<(u32, u32)>> = vec![Vec::new(); n];
        // Collection ids parallel to `new_out_edges` / `new_in_edges`.
        let mut new_out_collections: Vec<Vec<u32>> = vec![Vec::new(); n];
        let mut new_in_collections: Vec<Vec<u32>> = vec![Vec::new(); n];
        let mut new_out_weights: Vec<Vec<f64>> = if self.has_weights {
            vec![Vec::new(); n]
        } else {
            Vec::new()
        };
        let mut new_in_weights: Vec<Vec<f64>> = if self.has_weights {
            vec![Vec::new(); n]
        } else {
            Vec::new()
        };

        // Collect surviving dense edges.
        for node in 0..n {
            let node_id = node as u32;
            let idx = node_id as usize;

            // Outbound dense edges.
            if idx + 1 < self.out_offsets.len() {
                let start = self.out_offsets[idx] as usize;
                let end = self.out_offsets[idx + 1] as usize;
                for i in start..end {
                    let lid = self.out_labels[i];
                    let dst = self.out_targets[i];
                    let coll = self.out_collections.get(i).copied().unwrap_or(0);
                    if !self.deleted_edges.contains(&(node_id, lid, dst, coll)) {
                        new_out_edges[node].push((lid, dst));
                        new_out_collections[node].push(coll);
                        if self.has_weights {
                            let w = self
                                .out_weights
                                .as_ref()
                                .map_or(1.0, |ws| ws.get(i).copied().unwrap_or(1.0));
                            new_out_weights[node].push(w);
                        }
                    }
                }
            }

            // Inbound dense edges.
            if idx + 1 < self.in_offsets.len() {
                let start = self.in_offsets[idx] as usize;
                let end = self.in_offsets[idx + 1] as usize;
                for i in start..end {
                    let lid = self.in_labels[i];
                    let src = self.in_targets[i];
                    let coll = self.in_collections.get(i).copied().unwrap_or(0);
                    if !self.deleted_edges.contains(&(src, lid, node_id, coll)) {
                        new_in_edges[node].push((lid, src));
                        new_in_collections[node].push(coll);
                        if self.has_weights {
                            let w = self
                                .in_weights
                                .as_ref()
                                .map_or(1.0, |ws| ws.get(i).copied().unwrap_or(1.0));
                            new_in_weights[node].push(w);
                        }
                    }
                }
            }
        }

        // Merge buffer edges. Dedup is collection-aware: an identical
        // `(label, dst)` under a different collection is a distinct edge and
        // must survive alongside the existing one.
        for node in 0..n {
            for (buf_idx, &(lid, dst)) in self.buffer_out[node].iter().enumerate() {
                let coll = self.buffer_out_collections[node]
                    .get(buf_idx)
                    .copied()
                    .unwrap_or(0);
                if !new_out_edges[node]
                    .iter()
                    .zip(new_out_collections[node].iter())
                    .any(|(&(l, d), &c)| l == lid && d == dst && c == coll)
                {
                    new_out_edges[node].push((lid, dst));
                    new_out_collections[node].push(coll);
                    if self.has_weights {
                        let w = self.buffer_out_weights[node]
                            .get(buf_idx)
                            .copied()
                            .unwrap_or(1.0);
                        new_out_weights[node].push(w);
                    }
                }
            }
            for (buf_idx, &(lid, src)) in self.buffer_in[node].iter().enumerate() {
                let coll = self.buffer_in_collections[node]
                    .get(buf_idx)
                    .copied()
                    .unwrap_or(0);
                if !new_in_edges[node]
                    .iter()
                    .zip(new_in_collections[node].iter())
                    .any(|(&(l, s), &c)| l == lid && s == src && c == coll)
                {
                    new_in_edges[node].push((lid, src));
                    new_in_collections[node].push(coll);
                    if self.has_weights {
                        let w = self.buffer_in_weights[node]
                            .get(buf_idx)
                            .copied()
                            .unwrap_or(1.0);
                        new_in_weights[node].push(w);
                    }
                }
            }
        }

        // Build new dense arrays.
        let governor = self.governor.as_ref();
        let out = Self::build_dense(&new_out_edges, &new_out_collections, governor)?;
        let in_ = Self::build_dense(&new_in_edges, &new_in_collections, governor)?;

        self.out_offsets = out.offsets;
        self.out_targets = out.targets.into();
        self.out_labels = out.labels.into();
        self.out_collections = out.collections;
        self.in_offsets = in_.offsets;
        self.in_targets = in_.targets.into();
        self.in_labels = in_.labels.into();
        self.in_collections = in_.collections;

        // Build weight arrays (flatten per-node vecs into contiguous array).
        if self.has_weights {
            self.out_weights = Some(
                new_out_weights
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .into(),
            );
            self.in_weights = Some(
                new_in_weights
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .into(),
            );
        }

        // Clear buffer and deleted set.
        for buf in &mut self.buffer_out {
            buf.clear();
        }
        for buf in &mut self.buffer_in {
            buf.clear();
        }
        for buf in &mut self.buffer_out_collections {
            buf.clear();
        }
        for buf in &mut self.buffer_in_collections {
            buf.clear();
        }
        if self.has_weights {
            for buf in &mut self.buffer_out_weights {
                buf.clear();
            }
            for buf in &mut self.buffer_in_weights {
                buf.clear();
            }
        }
        self.deleted_edges.clear();
        Ok(())
    }
}
