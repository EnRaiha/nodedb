// SPDX-License-Identifier: Apache-2.0

//! CSR compaction: merges the mutable write buffer into dense arrays.

use super::index::CsrIndex;
use crate::GraphError;

impl CsrIndex {
    /// Compact a freshly built index directly from its mutation buffers.
    ///
    /// Initial rebuilds have no dense edges or deletions, so the general
    /// merge path would allocate and populate a second graph-sized set of
    /// per-node vectors only to flatten it immediately. This path flattens the
    /// already deduplicated insertion buffers directly. If the index is not
    /// fresh, it falls back to [`Self::compact`] to preserve mutation semantics.
    pub fn compact_initial_build(&mut self) -> Result<(), GraphError> {
        if !self.out_targets.is_empty()
            || !self.in_targets.is_empty()
            || !self.deleted_edges.is_empty()
        {
            return self.compact();
        }

        let n = self.id_to_node.len();
        let governor = self.governor.as_ref();
        let out = Self::build_dense(&self.buffer_out, &self.buffer_out_collections, governor)?;
        let in_ = Self::build_dense(&self.buffer_in, &self.buffer_in_collections, governor)?;
        let out_weights = self.has_weights.then(|| {
            let mut weights = Vec::with_capacity(out.targets.len());
            for (node, edges) in self.buffer_out.iter().enumerate() {
                for edge in 0..edges.len() {
                    weights.push(
                        self.buffer_out_weights
                            .get(node)
                            .and_then(|node_weights| node_weights.get(edge))
                            .copied()
                            .unwrap_or(1.0),
                    );
                }
            }
            weights.into()
        });
        let in_weights = self.has_weights.then(|| {
            let mut weights = Vec::with_capacity(in_.targets.len());
            for (node, edges) in self.buffer_in.iter().enumerate() {
                for edge in 0..edges.len() {
                    weights.push(
                        self.buffer_in_weights
                            .get(node)
                            .and_then(|node_weights| node_weights.get(edge))
                            .copied()
                            .unwrap_or(1.0),
                    );
                }
            }
            weights.into()
        });

        self.out_offsets = out.offsets;
        self.out_targets = out.targets.into();
        self.out_labels = out.labels.into();
        self.out_collections = out.collections;
        self.in_offsets = in_.offsets;
        self.in_targets = in_.targets.into();
        self.in_labels = in_.labels.into();
        self.in_collections = in_.collections;
        self.out_weights = out_weights;
        self.in_weights = in_weights;

        // Release the graph-sized per-node buffer allocations. Keep one empty
        // slot per node so subsequent incremental mutations remain valid.
        self.buffer_out = vec![Vec::new(); n];
        self.buffer_in = vec![Vec::new(); n];
        self.buffer_out_collections = vec![Vec::new(); n];
        self.buffer_in_collections = vec![Vec::new(); n];
        self.buffer_out_weights = vec![Vec::new(); n];
        self.buffer_in_weights = vec![Vec::new(); n];
        Ok(())
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn buffered_index() -> CsrIndex {
        let mut csr = CsrIndex::new();
        csr.add_edge_in_collection("a", "L", "b", "one").unwrap();
        csr.add_edge_weighted_in_collection("a", "W", "c", "two", 2.5)
            .unwrap();
        csr.add_edge_weighted_in_collection("b", "W", "c", "one", 3.5)
            .unwrap();
        csr.add_node("isolated").unwrap();
        csr
    }

    #[test]
    fn initial_build_compaction_matches_general_compaction() {
        let mut fast = buffered_index();
        let mut general = buffered_index();
        fast.compact_initial_build().unwrap();
        general.compact().unwrap();

        assert_eq!(&*fast.out_offsets, &*general.out_offsets);
        assert_eq!(&*fast.out_targets, &*general.out_targets);
        assert_eq!(&*fast.out_labels, &*general.out_labels);
        assert_eq!(fast.out_collections, general.out_collections);
        assert_eq!(&*fast.in_offsets, &*general.in_offsets);
        assert_eq!(&*fast.in_targets, &*general.in_targets);
        assert_eq!(&*fast.in_labels, &*general.in_labels);
        assert_eq!(fast.in_collections, general.in_collections);
        assert_eq!(fast.out_weights.as_deref(), general.out_weights.as_deref());
        assert_eq!(fast.in_weights.as_deref(), general.in_weights.as_deref());
        assert_eq!(fast.node_count(), 4);
    }

    #[test]
    fn initial_build_compaction_defaults_missing_weight_buffers() {
        let mut fast = buffered_index();
        let mut general = buffered_index();
        for weights in &mut fast.buffer_out_weights {
            weights.clear();
        }
        for weights in &mut fast.buffer_in_weights {
            weights.clear();
        }
        for weights in &mut general.buffer_out_weights {
            weights.clear();
        }
        for weights in &mut general.buffer_in_weights {
            weights.clear();
        }
        fast.compact_initial_build().unwrap();
        general.compact().unwrap();
        assert_eq!(fast.out_weights.as_deref(), general.out_weights.as_deref());
        assert_eq!(fast.in_weights.as_deref(), general.in_weights.as_deref());
        assert_eq!(
            fast.out_weights.as_deref().unwrap().len(),
            fast.edge_count()
        );
    }

    #[test]
    fn initial_build_compaction_allows_incremental_mutation() {
        let mut csr = buffered_index();
        csr.compact_initial_build().unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        assert_eq!(csr.edge_count(), 4);
        csr.compact().unwrap();
        assert_eq!(csr.edge_count(), 4);
    }

    #[test]
    fn initial_build_compaction_falls_back_after_dense_mutation() {
        let mut csr = buffered_index();
        csr.compact().unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        csr.compact_initial_build().unwrap();
        assert_eq!(csr.edge_count(), 4);
        assert!(csr.compacted_out_adjacency_raw().is_some());
    }
}
