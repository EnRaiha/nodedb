// SPDX-License-Identifier: Apache-2.0

//! R-tree range search and nearest-neighbor queries.

use nodedb_types::{BboxSpace, BoundingBox};
use std::collections::BinaryHeap;

use super::node::{EntryId, Node, NodeKind, RTreeEntry};

/// Result from nearest-neighbor search.
#[derive(Debug, Clone)]
pub struct NnResult {
    pub entry_id: EntryId,
    pub bbox: BoundingBox,
    /// Minimum distance in the tree's coordinate units: approximate degrees
    /// for a geographic tree, metres for a projected one.
    pub distance: f64,
}

/// Recursive range search.
pub(crate) fn search_node<'a>(
    nodes: &'a [Node],
    node_idx: usize,
    query: &BoundingBox,
    results: &mut Vec<&'a RTreeEntry>,
) {
    let node = &nodes[node_idx];
    if !node.bbox.intersects(query) {
        return;
    }
    match &node.kind {
        NodeKind::Leaf { entries } => {
            for entry in entries {
                if entry.bbox.intersects(query) {
                    results.push(entry);
                }
            }
        }
        NodeKind::Internal { children } => {
            for child in children {
                if child.bbox.intersects(query) {
                    search_node(nodes, child.node_idx, query, results);
                }
            }
        }
    }
}

/// A nearest-neighbor query point and the space its distances are taken in.
pub(crate) struct NnQuery {
    pub lng: f64,
    pub lat: f64,
    pub k: usize,
    pub space: BboxSpace,
}

/// Nearest-neighbor search via priority queue (min-heap).
pub(crate) fn nearest(
    nodes: &[Node],
    root: usize,
    query: NnQuery,
    is_empty: bool,
) -> Vec<NnResult> {
    let NnQuery {
        lng: query_lng,
        lat: query_lat,
        k,
        space,
    } = query;
    if k == 0 || is_empty {
        return Vec::new();
    }

    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    let mut results: Vec<NnResult> = Vec::with_capacity(k);

    heap.push(HeapItem {
        dist: min_dist_point_bbox(query_lng, query_lat, &nodes[root].bbox, space),
        node_idx: root,
    });

    while let Some(item) = heap.pop() {
        if results.len() >= k && item.dist > results[k - 1].distance {
            continue;
        }
        let node = &nodes[item.node_idx];
        match &node.kind {
            NodeKind::Internal { children } => {
                for child in children {
                    let d = min_dist_point_bbox(query_lng, query_lat, &child.bbox, space);
                    if results.len() < k || d <= results[results.len() - 1].distance {
                        heap.push(HeapItem {
                            dist: d,
                            node_idx: child.node_idx,
                        });
                    }
                }
            }
            NodeKind::Leaf { entries } => {
                for entry in entries {
                    let d = min_dist_point_bbox(query_lng, query_lat, &entry.bbox, space);
                    if results.len() < k || d < results[results.len() - 1].distance {
                        let nn = NnResult {
                            entry_id: entry.id,
                            bbox: entry.bbox,
                            distance: d,
                        };
                        insert_sorted(&mut results, nn, k);
                    }
                }
            }
        }
    }

    results
}

/// Min-heap item for NN traversal.
#[derive(Debug)]
struct HeapItem {
    dist: f64,
    node_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}
impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap (BinaryHeap is max-heap).
        other
            .dist
            .partial_cmp(&self.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Minimum Euclidean distance from point to bbox in coordinate units:
/// approximate degrees in geographic space, metres in planar space. Only
/// geographic space honours an anti-meridian-wrapping box.
fn min_dist_point_bbox(lng: f64, lat: f64, bbox: &BoundingBox, space: BboxSpace) -> f64 {
    let dlat = if lat < bbox.min_lat {
        bbox.min_lat - lat
    } else if lat > bbox.max_lat {
        lat - bbox.max_lat
    } else {
        0.0
    };

    let wraps = match space {
        BboxSpace::Geographic => bbox.crosses_antimeridian(),
        BboxSpace::Planar => false,
    };
    let dlng = if wraps {
        if lng >= bbox.min_lng || lng <= bbox.max_lng {
            0.0
        } else {
            (bbox.min_lng - lng).min(lng - bbox.max_lng).max(0.0)
        }
    } else if lng < bbox.min_lng {
        bbox.min_lng - lng
    } else if lng > bbox.max_lng {
        lng - bbox.max_lng
    } else {
        0.0
    };

    (dlat * dlat + dlng * dlng).sqrt()
}

fn insert_sorted(results: &mut Vec<NnResult>, item: NnResult, k: usize) {
    let pos = results
        .binary_search_by(|r| {
            r.distance
                .partial_cmp(&item.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or_else(|pos| pos);
    results.insert(pos, item);
    if results.len() > k {
        results.truncate(k);
    }
}
