// SPDX-License-Identifier: Apache-2.0

//! R*-tree node splitting (axis split strategy).

use nodedb_types::BboxSpace;

use super::node::{ChildRef, MIN_FILL_INTERNAL, MIN_FILL_LEAF, Node, NodeKind, RTreeEntry};
use super::tree::RTree;

/// Split an overflowing node using the R*-tree axis split strategy.
pub(crate) fn split_node(tree: &mut RTree, node_idx: usize) {
    let is_root = node_idx == tree.root;
    let level = tree.nodes[node_idx].level;
    let space = tree.space;

    let (sibling_idx, sibling_bbox) = match &mut tree.nodes[node_idx].kind {
        NodeKind::Leaf { entries } => {
            let all = std::mem::take(entries);
            let (keep, split_off) = split_leaf_entries(all, space);
            if let NodeKind::Leaf { entries } = &mut tree.nodes[node_idx].kind {
                *entries = keep;
            }
            tree.nodes[node_idx].recompute_bbox(space);

            let mut sibling = Node::new_leaf();
            if let NodeKind::Leaf { entries } = &mut sibling.kind {
                *entries = split_off;
            }
            sibling.recompute_bbox(space);
            let bbox = sibling.bbox;
            let idx = tree.nodes.len();
            tree.nodes.push(sibling);
            (idx, bbox)
        }
        NodeKind::Internal { children } => {
            let all = std::mem::take(children);
            let (keep, split_off) = split_internal_children(all, space);
            if let NodeKind::Internal { children } = &mut tree.nodes[node_idx].kind {
                *children = keep;
            }
            tree.nodes[node_idx].recompute_bbox(space);

            let mut sibling = Node::new_internal(level);
            if let NodeKind::Internal { children } = &mut sibling.kind {
                *children = split_off;
            }
            sibling.recompute_bbox(space);
            let bbox = sibling.bbox;
            let idx = tree.nodes.len();
            tree.nodes.push(sibling);
            (idx, bbox)
        }
    };

    if is_root {
        let old_root_bbox = tree.nodes[node_idx].bbox;
        let mut new_root = Node::new_internal(level + 1);
        if let NodeKind::Internal { children } = &mut new_root.kind {
            children.push(ChildRef {
                bbox: old_root_bbox,
                node_idx,
            });
            children.push(ChildRef {
                bbox: sibling_bbox,
                node_idx: sibling_idx,
            });
        }
        new_root.recompute_bbox(space);
        let new_root_idx = tree.nodes.len();
        tree.nodes.push(new_root);
        tree.root = new_root_idx;
    } else {
        let parent_idx = tree.find_parent(tree.root, node_idx);
        if let Some(pidx) = parent_idx {
            let updated_bbox = tree.nodes[node_idx].bbox;
            if let NodeKind::Internal { children } = &mut tree.nodes[pidx].kind {
                children.push(ChildRef {
                    bbox: sibling_bbox,
                    node_idx: sibling_idx,
                });
                for c in children.iter_mut() {
                    if c.node_idx == node_idx {
                        c.bbox = updated_bbox;
                        break;
                    }
                }
            }
            tree.nodes[pidx].recompute_bbox(space);
            if tree.nodes[pidx].is_overflow() {
                split_node(tree, pidx);
            }
        }
    }
}

/// Choose split axis (min margin sum), then split index (min overlap).
fn split_leaf_entries(
    mut entries: Vec<RTreeEntry>,
    space: BboxSpace,
) -> (Vec<RTreeEntry>, Vec<RTreeEntry>) {
    let min_fill = MIN_FILL_LEAF;
    let best_axis = choose_best_axis_leaf(&mut entries, min_fill, space);

    sort_entries_by_axis(&mut entries, best_axis);
    let best_k = choose_best_split_leaf(&entries, min_fill, space);

    let split_off = entries.split_off(best_k);
    (entries, split_off)
}

fn split_internal_children(
    mut children: Vec<ChildRef>,
    space: BboxSpace,
) -> (Vec<ChildRef>, Vec<ChildRef>) {
    let min_fill = MIN_FILL_INTERNAL;
    let best_axis = choose_best_axis_internal(&mut children, min_fill, space);

    sort_children_by_axis(&mut children, best_axis);
    let best_k = choose_best_split_internal(&children, min_fill, space);

    let split_off = children.split_off(best_k);
    (children, split_off)
}

fn choose_best_axis_leaf(entries: &mut [RTreeEntry], min_fill: usize, space: BboxSpace) -> usize {
    let mut best_axis = 0;
    let mut best_margin = f64::INFINITY;
    for axis in 0..2 {
        sort_entries_by_axis(entries, axis);
        let mut margin_sum = 0.0;
        for k in min_fill..=(entries.len() - min_fill) {
            let left = entries[..k]
                .iter()
                .fold(entries[0].bbox, |a, e| a.union_in(&e.bbox, space));
            let right = entries[k..]
                .iter()
                .fold(entries[k].bbox, |a, e| a.union_in(&e.bbox, space));
            margin_sum += left.margin() + right.margin();
        }
        if margin_sum < best_margin {
            best_margin = margin_sum;
            best_axis = axis;
        }
    }
    best_axis
}

fn choose_best_axis_internal(
    children: &mut [ChildRef],
    min_fill: usize,
    space: BboxSpace,
) -> usize {
    let mut best_axis = 0;
    let mut best_margin = f64::INFINITY;
    for axis in 0..2 {
        sort_children_by_axis(children, axis);
        let mut margin_sum = 0.0;
        for k in min_fill..=(children.len() - min_fill) {
            let left = children[..k]
                .iter()
                .fold(children[0].bbox, |a, c| a.union_in(&c.bbox, space));
            let right = children[k..]
                .iter()
                .fold(children[k].bbox, |a, c| a.union_in(&c.bbox, space));
            margin_sum += left.margin() + right.margin();
        }
        if margin_sum < best_margin {
            best_margin = margin_sum;
            best_axis = axis;
        }
    }
    best_axis
}

fn choose_best_split_leaf(entries: &[RTreeEntry], min_fill: usize, space: BboxSpace) -> usize {
    let mut best_k = min_fill;
    let mut best_overlap = f64::INFINITY;
    let mut best_area = f64::INFINITY;
    for k in min_fill..=(entries.len() - min_fill) {
        let left = entries[..k]
            .iter()
            .fold(entries[0].bbox, |a, e| a.union_in(&e.bbox, space));
        let right = entries[k..]
            .iter()
            .fold(entries[k].bbox, |a, e| a.union_in(&e.bbox, space));
        let overlap = left.overlap_area(&right);
        let area = left.area() + right.area();
        if overlap < best_overlap || (overlap == best_overlap && area < best_area) {
            best_overlap = overlap;
            best_area = area;
            best_k = k;
        }
    }
    best_k
}

fn choose_best_split_internal(children: &[ChildRef], min_fill: usize, space: BboxSpace) -> usize {
    let mut best_k = min_fill;
    let mut best_overlap = f64::INFINITY;
    let mut best_area = f64::INFINITY;
    for k in min_fill..=(children.len() - min_fill) {
        let left = children[..k]
            .iter()
            .fold(children[0].bbox, |a, c| a.union_in(&c.bbox, space));
        let right = children[k..]
            .iter()
            .fold(children[k].bbox, |a, c| a.union_in(&c.bbox, space));
        let overlap = left.overlap_area(&right);
        let area = left.area() + right.area();
        if overlap < best_overlap || (overlap == best_overlap && area < best_area) {
            best_overlap = overlap;
            best_area = area;
            best_k = k;
        }
    }
    best_k
}

fn sort_entries_by_axis(entries: &mut [RTreeEntry], axis: usize) {
    entries.sort_by(|a, b| {
        let va = if axis == 0 {
            a.bbox.min_lng
        } else {
            a.bbox.min_lat
        };
        let vb = if axis == 0 {
            b.bbox.min_lng
        } else {
            b.bbox.min_lat
        };
        va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn sort_children_by_axis(children: &mut [ChildRef], axis: usize) {
    children.sort_by(|a, b| {
        let va = if axis == 0 {
            a.bbox.min_lng
        } else {
            a.bbox.min_lat
        };
        let vb = if axis == 0 {
            b.bbox.min_lng
        } else {
            b.bbox.min_lat
        };
        va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
    });
}
