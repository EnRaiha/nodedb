// SPDX-License-Identifier: Apache-2.0

//! Sort-Tile-Recursive (STR) bulk loading for R-tree.

use nodedb_mem::ScopedMemory;

use super::node::{ChildRef, INTERNAL_CAPACITY, LEAF_CAPACITY, Node, NodeKind, RTreeEntry};
use super::tree::RTree;

impl RTree {
    /// Bulk load entries using Sort-Tile-Recursive packing.
    ///
    /// More efficient than repeated single inserts for large datasets.
    /// Produces better packing (less overlap between nodes).
    pub fn bulk_load(entries: Vec<RTreeEntry>) -> Self {
        Self::bulk_load_inner(entries, None)
    }

    /// Bulk load with scoped memory for budget accounting.
    ///
    /// The scoped memory handle is stored on the returned tree and used for
    /// subsequent batch operations (full-scan, checkpoint serialization).
    pub fn bulk_load_with_memory(entries: Vec<RTreeEntry>, memory: ScopedMemory) -> Self {
        Self::bulk_load_inner(entries, Some(memory))
    }

    fn bulk_load_inner(entries: Vec<RTreeEntry>, memory: Option<ScopedMemory>) -> Self {
        if entries.is_empty() {
            let mut tree = Self::new();
            if let Some(mem) = memory {
                tree.memory = Some(mem);
            }
            return tree;
        }
        let len = entries.len();

        // Reserve budget for nodes vec (approximately len / LEAF_CAPACITY nodes,
        // each holding LEAF_CAPACITY RTreeEntry slots). Best-effort: budget
        // pressure is a backpressure signal, not a hard gate on bulk load.
        let _guard = memory.as_ref().and_then(|mem| {
            let node_count = len.div_ceil(LEAF_CAPACITY) * 2; // leaves + internals
            let bytes = node_count
                * (std::mem::size_of::<Node>() + LEAF_CAPACITY * std::mem::size_of::<RTreeEntry>());
            mem.reserve(bytes).ok()
        });

        let mut tree = Self {
            nodes: Vec::new(),
            root: 0,
            len,
            memory,
        };
        tree.root = str_pack(&mut tree.nodes, entries);
        tree
    }
}

/// Recursively pack entries into leaf nodes, then group into internal nodes.
fn str_pack(nodes: &mut Vec<Node>, mut entries: Vec<RTreeEntry>) -> usize {
    if entries.len() <= LEAF_CAPACITY {
        let mut node = Node::new_leaf();
        if let NodeKind::Leaf {
            entries: ref mut leaf,
        } = node.kind
        {
            *leaf = entries;
        }
        node.recompute_bbox();
        let idx = nodes.len();
        nodes.push(node);
        return idx;
    }

    let num_leaves = entries.len().div_ceil(LEAF_CAPACITY);
    let num_slices = (num_leaves as f64).sqrt().ceil() as usize;
    let slice_size = entries.len().div_ceil(num_slices);

    // Sort by longitude (X).
    entries.sort_by(|a, b| {
        let ca = (a.bbox.min_lng + a.bbox.max_lng) / 2.0;
        let cb = (b.bbox.min_lng + b.bbox.max_lng) / 2.0;
        ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut child_nodes: Vec<usize> = Vec::new();

    for slice in entries.chunks(slice_size) {
        let mut slice_vec: Vec<RTreeEntry> = slice.to_vec();
        // Sort each slice by latitude (Y).
        slice_vec.sort_by(|a, b| {
            let ca = (a.bbox.min_lat + a.bbox.max_lat) / 2.0;
            let cb = (b.bbox.min_lat + b.bbox.max_lat) / 2.0;
            ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
        });

        for chunk in slice_vec.chunks(LEAF_CAPACITY) {
            let child_idx = str_pack(nodes, chunk.to_vec());
            child_nodes.push(child_idx);
        }
    }

    pack_internal(nodes, child_nodes)
}

/// Recursively group child node indices into internal nodes.
fn pack_internal(nodes: &mut Vec<Node>, child_indices: Vec<usize>) -> usize {
    if child_indices.len() <= INTERNAL_CAPACITY {
        let level = nodes[child_indices[0]].level + 1;
        let mut node = Node::new_internal(level);
        if let NodeKind::Internal { children } = &mut node.kind {
            for idx in child_indices {
                children.push(ChildRef {
                    bbox: nodes[idx].bbox,
                    node_idx: idx,
                });
            }
        }
        node.recompute_bbox();
        let idx = nodes.len();
        nodes.push(node);
        return idx;
    }

    let mut new_children = Vec::new();
    for chunk in child_indices.chunks(INTERNAL_CAPACITY) {
        let level = nodes[chunk[0]].level + 1;
        let mut node = Node::new_internal(level);
        if let NodeKind::Internal { children } = &mut node.kind {
            for &idx in chunk {
                children.push(ChildRef {
                    bbox: nodes[idx].bbox,
                    node_idx: idx,
                });
            }
        }
        node.recompute_bbox();
        let idx = nodes.len();
        nodes.push(node);
        new_children.push(idx);
    }

    pack_internal(nodes, new_children)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::BoundingBox;

    fn make_entry(id: u64, lng: f64, lat: f64) -> RTreeEntry {
        RTreeEntry {
            id,
            bbox: BoundingBox::from_point(lng, lat),
        }
    }

    #[test]
    fn bulk_load_500() {
        let entries: Vec<RTreeEntry> = (0..500)
            .map(|i| make_entry(i, (i as f64) * 0.1 - 25.0, (i as f64) * 0.06 - 15.0))
            .collect();
        let tree = RTree::bulk_load(entries);
        assert_eq!(tree.len(), 500);

        let all = tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), 500);

        let subset = tree.search(&BoundingBox::new(-5.0, -5.0, 5.0, 5.0));
        assert!(!subset.is_empty());
    }

    #[test]
    fn bulk_load_small() {
        let entries = vec![make_entry(1, 0.0, 0.0), make_entry(2, 1.0, 1.0)];
        let tree = RTree::bulk_load(entries);
        assert_eq!(tree.len(), 2);
    }

    #[test]
    fn bulk_load_empty() {
        let tree = RTree::bulk_load(Vec::new());
        assert!(tree.is_empty());
    }
}
