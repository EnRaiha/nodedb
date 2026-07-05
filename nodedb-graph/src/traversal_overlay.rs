// SPDX-License-Identifier: Apache-2.0

//! In-transaction (read-your-own-writes) BFS and subgraph materialization.
//!
//! These paths run only when a non-empty [`GraphOverlayDelta`] is supplied.
//! Unlike the durable dense paths (which key the frontier on the u32 CSR id),
//! these key the frontier on the node *string*: a node discovered only via a
//! staged edge has no CSR surrogate, yet its own staged out/in edges must
//! still be followed at the next hop. Durable nodes still resolve through
//! `node_to_id` for the CSR expansion, so the dense adjacency is used wherever
//! it exists; the string key is what lets staged-only nodes participate.

use std::collections::{HashSet, VecDeque};

use crate::bfs_params::BfsParams;
use crate::csr::{CsrIndex, Direction};
use crate::overlay_delta::GraphOverlayDelta;

impl CsrIndex {
    /// String-keyed BFS that merges the transaction's staged edges/tombstones.
    pub(crate) fn traverse_bfs_overlay(
        &self,
        params: BfsParams<'_>,
        overlay: &GraphOverlayDelta,
    ) -> Vec<String> {
        let BfsParams {
            start_nodes,
            label_filter,
            direction,
            max_depth,
            max_visited,
            frontier_bitmap,
        } = params;
        let label_id = label_filter.and_then(|l| self.label_id(l));
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();

        for &node in start_nodes {
            if visited.insert(node.to_string()) {
                queue.push_back((node.to_string(), 0));
            }
        }

        let want_out = matches!(direction, Direction::Out | Direction::Both);
        let want_in = matches!(direction, Direction::In | Direction::Both);

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth || visited.len() >= max_visited {
                continue;
            }
            let next_depth = depth + 1;

            // Durable CSR expansion for nodes that carry a surrogate.
            if let Some(&node_id) = self.node_to_id.get(node.as_str()) {
                self.record_access(node_id);
                if want_out {
                    for (lid, dst) in self.dense_iter_out(node_id) {
                        if label_id.is_some_and(|f| f != lid) {
                            continue;
                        }
                        let dst_name = &self.id_to_node[dst as usize];
                        if overlay.is_tombstoned(&node, self.label_name(lid), dst_name) {
                            continue;
                        }
                        if !frontier_bitmap.is_none_or(|bm| {
                            bm.contains(nodedb_types::Surrogate::new(self.node_surrogate_raw(dst)))
                        }) {
                            continue;
                        }
                        self.enqueue_str(
                            dst_name,
                            next_depth,
                            max_visited,
                            &mut visited,
                            &mut queue,
                        );
                    }
                }
                if want_in {
                    for (lid, src) in self.dense_iter_in(node_id) {
                        if label_id.is_some_and(|f| f != lid) {
                            continue;
                        }
                        let src_name = &self.id_to_node[src as usize];
                        if overlay.is_tombstoned(src_name, self.label_name(lid), &node) {
                            continue;
                        }
                        if !frontier_bitmap.is_none_or(|bm| {
                            bm.contains(nodedb_types::Surrogate::new(self.node_surrogate_raw(src)))
                        }) {
                            continue;
                        }
                        self.enqueue_str(
                            src_name,
                            next_depth,
                            max_visited,
                            &mut visited,
                            &mut queue,
                        );
                    }
                }
            }

            // Staged edges — followed for durable and staged-only nodes alike.
            // Staged edges are the transaction's own writes, so bitmap gating
            // (which needs a durable surrogate) does not apply.
            if want_out {
                let staged: Vec<String> = overlay
                    .out_neighbors(&node, label_filter)
                    .map(|(_, dst)| dst.to_string())
                    .collect();
                for dst in staged {
                    self.enqueue_str(&dst, next_depth, max_visited, &mut visited, &mut queue);
                }
            }
            if want_in {
                let staged: Vec<String> = overlay
                    .in_neighbors(&node, label_filter)
                    .map(|(_, src)| src.to_string())
                    .collect();
                for src in staged {
                    self.enqueue_str(&src, next_depth, max_visited, &mut visited, &mut queue);
                }
            }
        }

        visited.into_iter().collect()
    }

    /// String-keyed subgraph materialization merging staged edges/tombstones.
    pub(crate) fn subgraph_overlay(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
        overlay: &GraphOverlayDelta,
    ) -> Vec<(String, String, String)> {
        let label_id = label_filter.and_then(|l| self.label_id(l));
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();
        let mut edges: Vec<(String, String, String)> = Vec::new();

        for &node in start_nodes {
            if visited.insert(node.to_string()) {
                queue.push_back((node.to_string(), 0));
            }
        }

        let want_out = matches!(direction, Direction::Out | Direction::Both);
        let want_in = matches!(direction, Direction::In | Direction::Both);

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth || visited.len() >= max_visited {
                continue;
            }
            let next_depth = depth + 1;

            if let Some(&node_id) = self.node_to_id.get(node.as_str()) {
                self.record_access(node_id);
                if want_out {
                    for (lid, dst) in self.dense_iter_out(node_id) {
                        if label_id.is_some_and(|f| f != lid) {
                            continue;
                        }
                        let label = self.label_name(lid);
                        let dst_name = &self.id_to_node[dst as usize];
                        if overlay.is_tombstoned(&node, label, dst_name) {
                            continue;
                        }
                        edges.push((node.clone(), label.to_string(), dst_name.clone()));
                        self.enqueue_str(
                            dst_name,
                            next_depth,
                            max_visited,
                            &mut visited,
                            &mut queue,
                        );
                    }
                }
                if want_in {
                    for (lid, src) in self.dense_iter_in(node_id) {
                        if label_id.is_some_and(|f| f != lid) {
                            continue;
                        }
                        let label = self.label_name(lid);
                        let src_name = &self.id_to_node[src as usize];
                        if overlay.is_tombstoned(src_name, label, &node) {
                            continue;
                        }
                        edges.push((src_name.clone(), label.to_string(), node.clone()));
                        self.enqueue_str(
                            src_name,
                            next_depth,
                            max_visited,
                            &mut visited,
                            &mut queue,
                        );
                    }
                }
            }

            if want_out {
                let staged: Vec<(String, String)> = overlay
                    .out_neighbors(&node, label_filter)
                    .map(|(l, d)| (l.to_string(), d.to_string()))
                    .collect();
                for (label, dst) in staged {
                    edges.push((node.clone(), label, dst.clone()));
                    self.enqueue_str(&dst, next_depth, max_visited, &mut visited, &mut queue);
                }
            }
            if want_in {
                let staged: Vec<(String, String)> = overlay
                    .in_neighbors(&node, label_filter)
                    .map(|(l, s)| (l.to_string(), s.to_string()))
                    .collect();
                for (label, src) in staged {
                    edges.push((src.clone(), label, node.clone()));
                    self.enqueue_str(&src, next_depth, max_visited, &mut visited, &mut queue);
                }
            }
        }

        edges
    }

    /// Enqueue `name` at `next_depth` if it is newly visited and the visited
    /// cap has not been reached.
    fn enqueue_str(
        &self,
        name: &str,
        next_depth: usize,
        max_visited: usize,
        visited: &mut HashSet<String>,
        queue: &mut VecDeque<(String, usize)>,
    ) {
        if visited.len() < max_visited && visited.insert(name.to_string()) {
            queue.push_back((name.to_string(), next_depth));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bfs_params::BfsParams;
    use crate::csr::{CsrIndex, Direction};
    use crate::overlay_delta::GraphOverlayDelta;
    use crate::traversal::DEFAULT_MAX_VISITED;

    fn base() -> CsrIndex {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b").unwrap();
        csr
    }

    #[test]
    fn multi_hop_through_staged_only_node() {
        // Durable: a->b. Staged: a->x, x->y. A 2-hop BFS from "a" must reach
        // "y" through the staged-only intermediate "x" (which has no CSR id).
        let csr = base();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("a", "KNOWS", "x");
        ov.stage_edge("x", "KNOWS", "y");

        let mut r = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: Some("KNOWS"),
                direction: Direction::Out,
                max_depth: 2,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            Some(&ov),
        );
        r.sort();
        assert_eq!(r, vec!["a", "b", "x", "y"]);
    }

    #[test]
    fn tombstone_skips_durable_edge() {
        let csr = base();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_tombstone("a", "KNOWS", "b");

        let mut r = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: Some("KNOWS"),
                direction: Direction::Out,
                max_depth: 2,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            Some(&ov),
        );
        r.sort();
        assert_eq!(r, vec!["a"]);
    }

    #[test]
    fn subgraph_includes_staged_and_skips_tombstone() {
        // Durable a->b (tombstoned) + a->c. Staged a->x.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b").unwrap();
        csr.add_edge("a", "KNOWS", "c").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_tombstone("a", "KNOWS", "b");
        ov.stage_edge("a", "KNOWS", "x");

        let edges = csr.subgraph(
            &["a"],
            Some("KNOWS"),
            Direction::Out,
            1,
            DEFAULT_MAX_VISITED,
            Some(&ov),
        );
        assert!(edges.contains(&("a".into(), "KNOWS".into(), "c".into())));
        assert!(edges.contains(&("a".into(), "KNOWS".into(), "x".into())));
        assert!(!edges.contains(&("a".into(), "KNOWS".into(), "b".into())));
    }

    #[test]
    fn subgraph_in_direction_surfaces_staged_in_edge() {
        // Staged in-edge z->a; querying subgraph In from "a" surfaces it.
        let csr = base();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("z", "KNOWS", "a");

        let edges = csr.subgraph(
            &["a"],
            Some("KNOWS"),
            Direction::In,
            1,
            DEFAULT_MAX_VISITED,
            Some(&ov),
        );
        assert!(edges.contains(&("z".into(), "KNOWS".into(), "a".into())));
    }

    #[test]
    fn empty_overlay_matches_durable() {
        let csr = base();
        let ov = GraphOverlayDelta::new();
        let mut with = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: None,
                direction: Direction::Out,
                max_depth: 2,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            Some(&ov),
        );
        let mut without = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: None,
                direction: Direction::Out,
                max_depth: 2,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            None,
        );
        with.sort();
        without.sort();
        assert_eq!(with, without);
    }
}
