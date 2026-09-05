// SPDX-License-Identifier: Apache-2.0

//! R*-tree public API and core structure.

use nodedb_types::BoundingBox;

use super::node::{Node, NodeKind, RTreeEntry};
use super::search::NnResult;
use nodedb_mem::ScopedMemory;

/// R*-tree spatial index.
///
/// Supports insert, delete, range search (bbox intersection), and
/// incremental nearest-neighbor queries. Array-backed nodes for cache
/// friendliness.
///
/// References:
/// - Beckmann et al., "The R*-tree" (1990)
/// - Hjaltason & Samet, "Distance Browsing in Spatial Databases" (1999)
pub struct RTree {
    pub(crate) nodes: Vec<Node>,
    pub(crate) root: usize,
    pub(crate) len: usize,
    /// Enforces database, tenant, and engine memory budgets on large batch
    /// allocations (bulk load, full-scan serialization, range search results).
    pub(crate) memory: ScopedMemory,
}

impl RTree {
    pub fn new(memory: ScopedMemory) -> Self {
        Self {
            nodes: vec![Node::new_leaf()],
            root: 0,
            len: 0,
            memory,
        }
    }

    /// Shared reference to the scoped memory handle.
    pub(crate) fn memory(&self) -> &ScopedMemory {
        &self.memory
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Range search: return all entries whose bbox intersects the query bbox.
    pub fn search(&self, query: &BoundingBox) -> Vec<&RTreeEntry> {
        let mut results = Vec::new();
        super::search::search_node(&self.nodes, self.root, query, &mut results);
        results
    }

    /// Range search returning owned entries.
    pub fn search_owned(&self, query: &BoundingBox) -> Vec<RTreeEntry> {
        self.search(query).into_iter().cloned().collect()
    }

    /// Nearest-neighbor search using incremental distance ordering.
    pub fn nearest(&self, query_lng: f64, query_lat: f64, k: usize) -> Vec<NnResult> {
        super::search::nearest(
            &self.nodes,
            self.root,
            query_lng,
            query_lat,
            k,
            self.is_empty(),
        )
    }

    /// Get all entries (for persistence serialization).
    pub fn entries(&self) -> Vec<&RTreeEntry> {
        let bytes = self.len * std::mem::size_of::<*const RTreeEntry>();
        let _guard = self.memory().reserve(bytes).ok();
        let mut result = Vec::with_capacity(self.len);
        collect_entries(&self.nodes, self.root, &mut result);
        result
    }

    /// Find parent of a node by traversal from root.
    pub(crate) fn find_parent(&self, current: usize, target: usize) -> Option<usize> {
        if let NodeKind::Internal { children } = &self.nodes[current].kind {
            for child in children {
                if child.node_idx == target {
                    return Some(current);
                }
                if let Some(p) = self.find_parent(child.node_idx, target) {
                    return Some(p);
                }
            }
        }
        None
    }

    /// Condense root: if root is internal with 1 child, collapse.
    pub(crate) fn condense_root(&mut self) {
        if let NodeKind::Internal { children } = &self.nodes[self.root].kind
            && children.len() == 1
        {
            self.root = children[0].node_idx;
        }
    }
}

fn collect_entries<'a>(nodes: &'a [Node], node_idx: usize, result: &mut Vec<&'a RTreeEntry>) {
    match &nodes[node_idx].kind {
        NodeKind::Leaf { entries } => result.extend(entries.iter()),
        NodeKind::Internal { children } => {
            for child in children {
                collect_entries(nodes, child.node_idx, result);
            }
        }
    }
}

pub(crate) fn collect_entries_owned(nodes: &[Node], node_idx: usize, result: &mut Vec<RTreeEntry>) {
    match &nodes[node_idx].kind {
        NodeKind::Leaf { entries } => result.extend(entries.iter().cloned()),
        NodeKind::Internal { children } => {
            for child in children {
                collect_entries_owned(nodes, child.node_idx, result);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::node::LEAF_CAPACITY;
    use super::*;
    use crate::test_support::test_memory;

    fn make_entry(id: u64, lng: f64, lat: f64) -> RTreeEntry {
        RTreeEntry {
            id,
            bbox: BoundingBox::from_point(lng, lat),
        }
    }

    fn make_rect(id: u64, min_lng: f64, min_lat: f64, max_lng: f64, max_lat: f64) -> RTreeEntry {
        RTreeEntry {
            id,
            bbox: BoundingBox::new(min_lng, min_lat, max_lng, max_lat),
        }
    }

    #[test]
    fn empty_tree() {
        let tree = RTree::new(test_memory());
        assert!(tree.is_empty());
        assert!(
            tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0))
                .is_empty()
        );
    }

    #[test]
    fn insert_and_search_single() {
        let mut tree = RTree::new(test_memory());
        tree.insert(make_entry(1, 10.0, 20.0));
        assert_eq!(tree.len(), 1);
        assert_eq!(
            tree.search(&BoundingBox::new(5.0, 15.0, 15.0, 25.0)).len(),
            1
        );
        assert!(
            tree.search(&BoundingBox::new(50.0, 50.0, 60.0, 60.0))
                .is_empty()
        );
    }

    #[test]
    fn insert_many_and_search() {
        let mut tree = RTree::new(test_memory());
        for i in 0..200 {
            tree.insert(make_entry(
                i,
                (i as f64) * 0.5 - 50.0,
                (i as f64) * 0.3 - 30.0,
            ));
        }
        assert_eq!(tree.len(), 200);
        let all = tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), 200);
    }

    #[test]
    fn delete_entry() {
        let mut tree = RTree::new(test_memory());
        for i in 0..50 {
            tree.insert(make_entry(i, i as f64, i as f64));
        }
        assert!(tree.delete(25));
        assert_eq!(tree.len(), 49);
        let all = tree.search(&BoundingBox::new(-1.0, -1.0, 100.0, 100.0));
        assert!(all.iter().all(|e| e.id != 25));
        assert!(!tree.delete(999));
    }

    #[test]
    fn nearest_neighbor() {
        let mut tree = RTree::new(test_memory());
        tree.insert(make_entry(1, 0.0, 0.0));
        tree.insert(make_entry(2, 10.0, 10.0));
        tree.insert(make_entry(3, 5.0, 5.0));
        let nn = tree.nearest(4.0, 4.0, 2);
        assert_eq!(nn.len(), 2);
        assert_eq!(nn[0].entry_id, 3);
        assert_eq!(nn[1].entry_id, 1);
    }

    #[test]
    fn nearest_empty() {
        assert!(RTree::new(test_memory()).nearest(0.0, 0.0, 5).is_empty());
    }

    #[test]
    fn rect_overlap_search() {
        let mut tree = RTree::new(test_memory());
        tree.insert(make_rect(1, 0.0, 0.0, 10.0, 10.0));
        tree.insert(make_rect(2, 5.0, 5.0, 15.0, 15.0));
        tree.insert(make_rect(3, 20.0, 20.0, 30.0, 30.0));
        let results = tree.search(&BoundingBox::new(3.0, 3.0, 7.0, 7.0));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn stress_insert_delete() {
        let mut tree = RTree::new(test_memory());
        for i in 0..100_u64 {
            tree.insert(make_entry(i, (i as f64) * 0.5, (i as f64) * 0.3));
        }
        for i in (0..100_u64).step_by(2) {
            assert!(tree.delete(i));
        }
        assert_eq!(tree.len(), 50);
        let all = tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert!(all.iter().all(|e| e.id % 2 == 1));
    }

    #[test]
    fn triggers_node_split() {
        let mut tree = RTree::new(test_memory());
        let count = LEAF_CAPACITY * 3;
        for i in 0..count as u64 {
            tree.insert(make_entry(i, (i as f64) * 0.1, (i as f64) * 0.1));
        }
        assert_eq!(tree.len(), count);
        let all = tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), count);
    }

    #[test]
    fn entries_enumeration() {
        let mut tree = RTree::new(test_memory());
        for i in 0..10 {
            tree.insert(make_entry(i, i as f64, i as f64));
        }
        assert_eq!(tree.entries().len(), 10);
    }
}
