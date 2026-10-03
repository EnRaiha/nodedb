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
//!
//! Both walks run level by level, as the durable paths do: each level's new
//! nodes are admitted in node-name order under `max_visited`.

use std::collections::HashSet;

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
        let labels = self.label_filter(label_filter);
        let in_bitmap = |id: u32| {
            frontier_bitmap.is_none_or(|bm| {
                bm.contains(nodedb_types::Surrogate::new(self.node_surrogate_raw(id)))
            })
        };
        let mut visited: HashSet<String> = HashSet::new();
        let mut frontier: Vec<String> = Vec::new();
        for &node in start_nodes {
            if visited.insert(node.to_string()) {
                frontier.push(node.to_string());
            }
        }

        let want_out = matches!(direction, Direction::Out | Direction::Both);
        let want_in = matches!(direction, Direction::In | Direction::Both);

        for _depth in 0..max_depth {
            if frontier.is_empty() || visited.len() >= max_visited {
                break;
            }
            let mut candidates: Vec<String> = Vec::new();
            for node in &frontier {
                // Durable CSR expansion for nodes that carry a surrogate.
                if let Some(&node_id) = self.node_to_id.get(node.as_str()) {
                    self.record_access(node_id);
                    if want_out {
                        for (lid, dst) in self.dense_iter_out(node_id) {
                            let dst_name = &self.id_to_node[dst as usize];
                            if labels.keeps(lid)
                                && !overlay.is_tombstoned(node, self.label_name(lid), dst_name)
                                && in_bitmap(dst)
                                && !visited.contains(dst_name)
                            {
                                candidates.push(dst_name.clone());
                            }
                        }
                    }
                    if want_in {
                        for (lid, src) in self.dense_iter_in(node_id) {
                            let src_name = &self.id_to_node[src as usize];
                            if labels.keeps(lid)
                                && !overlay.is_tombstoned(src_name, self.label_name(lid), node)
                                && in_bitmap(src)
                                && !visited.contains(src_name)
                            {
                                candidates.push(src_name.clone());
                            }
                        }
                    }
                }

                // Staged edges — followed for durable and staged-only nodes
                // alike. Staged edges are the transaction's own writes, so
                // bitmap gating (which needs a durable surrogate) does not
                // apply.
                if want_out {
                    candidates.extend(
                        overlay
                            .out_neighbors(node, label_filter)
                            .map(|(_, dst)| dst.to_string())
                            .filter(|dst| !visited.contains(dst)),
                    );
                }
                if want_in {
                    candidates.extend(
                        overlay
                            .in_neighbors(node, label_filter)
                            .map(|(_, src)| src.to_string())
                            .filter(|src| !visited.contains(src)),
                    );
                }
            }
            frontier = admit_names(candidates, &mut visited, max_visited);
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
        let labels = self.label_filter(label_filter);
        let mut visited: HashSet<String> = HashSet::new();
        let mut frontier: Vec<String> = Vec::new();
        let mut edges: Vec<(String, String, String)> = Vec::new();

        for &node in start_nodes {
            if visited.insert(node.to_string()) {
                frontier.push(node.to_string());
            }
        }

        let want_out = matches!(direction, Direction::Out | Direction::Both);
        let want_in = matches!(direction, Direction::In | Direction::Both);

        for _depth in 0..max_depth {
            if frontier.is_empty() || visited.len() >= max_visited {
                break;
            }
            let mut candidates: Vec<String> = Vec::new();
            for node in &frontier {
                if let Some(&node_id) = self.node_to_id.get(node.as_str()) {
                    self.record_access(node_id);
                    if want_out {
                        for (lid, dst) in self.dense_iter_out(node_id) {
                            if !labels.keeps(lid) {
                                continue;
                            }
                            let label = self.label_name(lid);
                            let dst_name = &self.id_to_node[dst as usize];
                            if overlay.is_tombstoned(node, label, dst_name) {
                                continue;
                            }
                            edges.push((node.clone(), label.to_string(), dst_name.clone()));
                            if !visited.contains(dst_name) {
                                candidates.push(dst_name.clone());
                            }
                        }
                    }
                    if want_in {
                        for (lid, src) in self.dense_iter_in(node_id) {
                            if !labels.keeps(lid) {
                                continue;
                            }
                            let label = self.label_name(lid);
                            let src_name = &self.id_to_node[src as usize];
                            if overlay.is_tombstoned(src_name, label, node) {
                                continue;
                            }
                            edges.push((src_name.clone(), label.to_string(), node.clone()));
                            if !visited.contains(src_name) {
                                candidates.push(src_name.clone());
                            }
                        }
                    }
                }

                if want_out {
                    for (label, dst) in overlay.out_neighbors(node, label_filter) {
                        edges.push((node.clone(), label.to_string(), dst.to_string()));
                        if !visited.contains(dst) {
                            candidates.push(dst.to_string());
                        }
                    }
                }
                if want_in {
                    for (label, src) in overlay.in_neighbors(node, label_filter) {
                        edges.push((src.to_string(), label.to_string(), node.clone()));
                        if !visited.contains(src) {
                            candidates.push(src.to_string());
                        }
                    }
                }
            }
            frontier = admit_names(candidates, &mut visited, max_visited);
        }

        edges
    }
}

/// Admit `candidates` into `visited` in name order, until `visited` holds
/// `max_visited` nodes. Returns the nodes admitted, in name order.
fn admit_names(
    mut candidates: Vec<String>,
    visited: &mut HashSet<String>,
    max_visited: usize,
) -> Vec<String> {
    candidates.sort();
    candidates.dedup();
    let mut admitted = Vec::with_capacity(candidates.len());
    for name in candidates {
        if visited.len() >= max_visited {
            break;
        }
        if visited.insert(name.clone()) {
            admitted.push(name);
        }
    }
    admitted
}

#[cfg(test)]
mod tests {
    use crate::bfs_params::BfsParams;
    use crate::csr::{CsrIndex, Direction};
    use crate::overlay_delta::GraphOverlayDelta;
    use crate::test_support::test_memory;
    use crate::traversal::DEFAULT_MAX_VISITED;

    fn base() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
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
    fn a_capped_overlay_bfs_admits_in_name_order() {
        // Durable a->b, staged a->x and a->c: a cap of 3 admits b and c.
        let csr = base();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("a", "KNOWS", "x");
        ov.stage_edge("a", "KNOWS", "c");

        let mut r = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: Some("KNOWS"),
                direction: Direction::Out,
                max_depth: 2,
                max_visited: 3,
                frontier_bitmap: None,
            },
            Some(&ov),
        );
        r.sort();
        assert_eq!(r, vec!["a", "b", "c"]);
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
        let mut csr = CsrIndex::new(test_memory());
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
