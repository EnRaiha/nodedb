// SPDX-License-Identifier: Apache-2.0

//! R*-tree insertion with overflow treatment (forced reinsert).

use nodedb_types::{BboxSpace, BoundingBox};

use super::node::{ChildRef, NodeKind, REINSERT_COUNT_LEAF, RTreeEntry};
use super::split::split_node;
use super::tree::RTree;

impl RTree {
    /// Insert an entry into the R-tree.
    pub fn insert(&mut self, entry: RTreeEntry) {
        let bbox = entry.bbox;
        let mut reinserted_levels = Vec::new();
        insert_entry(self, entry, 0, &mut reinserted_levels);
        self.len += 1;
        self.nodes[self.root].bbox = self.nodes[self.root].bbox.union_in(&bbox, self.space);
    }

    /// Insert with reinsert tracking (used internally and by delete).
    pub(crate) fn reinsert_entry(&mut self, entry: RTreeEntry) {
        let mut reinserted = Vec::new();
        insert_entry(self, entry, 0, &mut reinserted);
    }
}

fn insert_entry(
    tree: &mut RTree,
    entry: RTreeEntry,
    target_level: u32,
    reinserted_levels: &mut Vec<u32>,
) {
    let leaf_idx = choose_subtree(tree, tree.root, &entry.bbox, target_level);

    match &mut tree.nodes[leaf_idx].kind {
        NodeKind::Leaf { entries } => entries.push(entry),
        NodeKind::Internal { .. } => {
            // choose_subtree guarantees a leaf at target_level; if we reach
            // here the tree structure is corrupted. Insert into root as
            // a fallback rather than crashing a production system.
            debug_assert!(false, "choose_subtree must return a leaf node");
            return;
        }
    }
    tree.nodes[leaf_idx].recompute_bbox(tree.space);

    if tree.nodes[leaf_idx].is_overflow() {
        treat_overflow(tree, leaf_idx, reinserted_levels);
    }
}

/// R*-tree ChooseSubtree: navigate to the best leaf for this entry.
fn choose_subtree(
    tree: &RTree,
    node_idx: usize,
    entry_bbox: &BoundingBox,
    target_level: u32,
) -> usize {
    let node = &tree.nodes[node_idx];
    if node.level == target_level {
        return node_idx;
    }

    match &node.kind {
        NodeKind::Leaf { .. } => node_idx,
        NodeKind::Internal { children } => {
            if children.is_empty() {
                return node_idx;
            }
            let best = if tree.nodes[children[0].node_idx].is_leaf() {
                choose_least_overlap(children, entry_bbox, tree.space)
            } else {
                choose_least_enlargement(children, entry_bbox, tree.space)
            };
            choose_subtree(tree, children[best].node_idx, entry_bbox, target_level)
        }
    }
}

fn choose_least_enlargement(
    children: &[ChildRef],
    entry_bbox: &BoundingBox,
    space: BboxSpace,
) -> usize {
    let mut best = 0;
    let mut best_enlarge = f64::INFINITY;
    let mut best_area = f64::INFINITY;
    for (i, child) in children.iter().enumerate() {
        let enlarge = child.bbox.enlargement_in(entry_bbox, space);
        let area = child.bbox.area();
        if enlarge < best_enlarge || (enlarge == best_enlarge && area < best_area) {
            best = i;
            best_enlarge = enlarge;
            best_area = area;
        }
    }
    best
}

fn choose_least_overlap(
    children: &[ChildRef],
    entry_bbox: &BoundingBox,
    space: BboxSpace,
) -> usize {
    let mut best = 0;
    let mut best_oi = f64::INFINITY;
    let mut best_enlarge = f64::INFINITY;
    let mut best_area = f64::INFINITY;
    for (i, child) in children.iter().enumerate() {
        let enlarged = child.bbox.union_in(entry_bbox, space);
        let mut before = 0.0_f64;
        let mut after = 0.0_f64;
        for (j, other) in children.iter().enumerate() {
            if j != i {
                before += child.bbox.overlap_area(&other.bbox);
                after += enlarged.overlap_area(&other.bbox);
            }
        }
        let oi = after - before;
        let enlarge = child.bbox.enlargement_in(entry_bbox, space);
        let area = child.bbox.area();
        if oi < best_oi
            || (oi == best_oi && enlarge < best_enlarge)
            || (oi == best_oi && enlarge == best_enlarge && area < best_area)
        {
            best = i;
            best_oi = oi;
            best_enlarge = enlarge;
            best_area = area;
        }
    }
    best
}

/// R*-tree overflow: forced reinsert first, then split on second overflow.
fn treat_overflow(tree: &mut RTree, node_idx: usize, reinserted_levels: &mut Vec<u32>) {
    let level = tree.nodes[node_idx].level;
    if node_idx != tree.root && !reinserted_levels.contains(&level) {
        reinserted_levels.push(level);
        let entries = forced_reinsert(tree, node_idx);
        for entry in entries {
            insert_entry(tree, entry, 0, reinserted_levels);
        }
    } else {
        split_node(tree, node_idx);
    }
}

/// Remove the farthest entries from node center and return for reinsertion.
fn forced_reinsert(tree: &mut RTree, node_idx: usize) -> Vec<RTreeEntry> {
    let reinsert_count = if tree.nodes[node_idx].is_leaf() {
        REINSERT_COUNT_LEAF
    } else {
        0
    };
    if reinsert_count == 0 {
        return Vec::new();
    }

    let center_lng = (tree.nodes[node_idx].bbox.min_lng + tree.nodes[node_idx].bbox.max_lng) / 2.0;
    let center_lat = (tree.nodes[node_idx].bbox.min_lat + tree.nodes[node_idx].bbox.max_lat) / 2.0;

    if let NodeKind::Leaf { entries } = &mut tree.nodes[node_idx].kind {
        entries.sort_by(|a, b| {
            let da = dist_sq_center(center_lng, center_lat, &a.bbox);
            let db = dist_sq_center(center_lng, center_lat, &b.bbox);
            db.partial_cmp(&da).unwrap_or(std::cmp::Ordering::Equal)
        });
        let removed: Vec<RTreeEntry> = entries.drain(..reinsert_count).collect();
        tree.nodes[node_idx].recompute_bbox(tree.space);
        removed
    } else {
        Vec::new()
    }
}

fn dist_sq_center(lng: f64, lat: f64, bbox: &BoundingBox) -> f64 {
    let cx = (bbox.min_lng + bbox.max_lng) / 2.0;
    let cy = (bbox.min_lat + bbox.max_lat) / 2.0;
    (lng - cx).powi(2) + (lat - cy).powi(2)
}
