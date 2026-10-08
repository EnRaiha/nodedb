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
//!
//! `partition` is `None` for a tenant with no CSR partition. The walks then
//! follow only staged edges, and allocate no CSR.
//!
//! A start node joins the walk only when it exists: the partition holds it,
//! or a staged edge names it. The dense walks drop an absent start the same
//! way.

use std::collections::HashSet;

use crate::bfs_params::BfsParams;
use crate::csr::index::LabelFilter;
use crate::csr::{CsrIndex, Direction};
use crate::overlay_delta::GraphOverlayDelta;

/// True when `node` exists for an overlay walk: `partition` holds it, or a
/// staged edge names it.
pub(crate) fn node_present(
    partition: Option<&CsrIndex>,
    overlay: &GraphOverlayDelta,
    node: &str,
) -> bool {
    partition.is_some_and(|csr| csr.node_to_id.contains_key(node)) || overlay.names_node(node)
}

/// Admit each present start node once into `visited`. Returns the admitted
/// nodes in input order.
fn admit_starts(
    partition: Option<&CsrIndex>,
    overlay: &GraphOverlayDelta,
    start_nodes: &[&str],
    visited: &mut HashSet<String>,
) -> Vec<String> {
    let mut frontier = Vec::new();
    for &node in start_nodes {
        if node_present(partition, overlay, node) && visited.insert(node.to_string()) {
            frontier.push(node.to_string());
        }
    }
    frontier
}

/// String-keyed BFS that merges the transaction's staged edges/tombstones.
pub(crate) fn traverse_bfs_overlay(
    partition: Option<&CsrIndex>,
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
    let durable_labels = partition.map(|csr| csr.label_filter(label_filter));
    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier = admit_starts(partition, overlay, start_nodes, &mut visited);

    let want_out = matches!(direction, Direction::Out | Direction::Both);
    let want_in = matches!(direction, Direction::In | Direction::Both);

    for _depth in 0..max_depth {
        if frontier.is_empty() || visited.len() >= max_visited {
            break;
        }
        let mut candidates: Vec<String> = Vec::new();
        for node in &frontier {
            // Durable CSR expansion for nodes that carry a surrogate.
            if let (Some(csr), Some(labels)) = (partition, durable_labels.as_ref())
                && let Some(&node_id) = csr.node_to_id.get(node.as_str())
            {
                let in_bitmap = |id: u32| {
                    frontier_bitmap.is_none_or(|bm| {
                        bm.contains(nodedb_types::Surrogate::new(csr.node_surrogate_raw(id)))
                    })
                };
                csr.record_access(node_id);
                if want_out {
                    for (lid, dst) in csr.dense_iter_out(node_id) {
                        let dst_name = &csr.id_to_node[dst as usize];
                        if labels.keeps(lid)
                            && !overlay.is_tombstoned(node, csr.label_name(lid), dst_name)
                            && in_bitmap(dst)
                            && !visited.contains(dst_name)
                        {
                            candidates.push(dst_name.clone());
                        }
                    }
                }
                if want_in {
                    for (lid, src) in csr.dense_iter_in(node_id) {
                        let src_name = &csr.id_to_node[src as usize];
                        if labels.keeps(lid)
                            && !overlay.is_tombstoned(src_name, csr.label_name(lid), node)
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

/// The arguments of one overlay subgraph walk.
pub(crate) struct OverlaySubgraphParams<'a> {
    pub start_nodes: &'a [&'a str],
    pub label_filter: &'a [&'a str],
    pub direction: Direction,
    pub max_depth: usize,
    pub max_visited: usize,
}

/// String-keyed subgraph materialization merging staged edges/tombstones.
pub(crate) fn subgraph_overlay(
    partition: Option<&CsrIndex>,
    params: OverlaySubgraphParams<'_>,
    overlay: &GraphOverlayDelta,
) -> Vec<(String, String, String)> {
    let OverlaySubgraphParams {
        start_nodes,
        label_filter,
        direction,
        max_depth,
        max_visited,
    } = params;
    let mut visited: HashSet<String> = HashSet::new();
    let mut edges: Vec<(String, String, String)> = Vec::new();
    // Each physical edge once: `Both` reaches an edge from both ends, and
    // one triple can be stored under several collections.
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    let mut frontier = admit_starts(partition, overlay, start_nodes, &mut visited);

    let scope = OverlayEdgeScope {
        partition: partition.map(|csr| (csr, csr.label_filter(label_filter))),
        label_filter,
        want_out: matches!(direction, Direction::Out | Direction::Both),
        want_in: matches!(direction, Direction::In | Direction::Both),
        overlay,
    };

    for _depth in 0..max_depth {
        if frontier.is_empty() || visited.len() >= max_visited {
            break;
        }
        let mut candidates: Vec<String> = Vec::new();
        for node in &frontier {
            for (edge, neighbor) in overlay_node_edges(node, &scope) {
                push_once(&mut edges, &mut seen, edge);
                if !visited.contains(&neighbor) {
                    candidates.push(neighbor);
                }
            }
        }
        frontier = admit_names(candidates, &mut visited, max_visited);
    }

    // The last admitted level is never expanded. Its edges to admitted
    // nodes, itself included, are still part of the subgraph.
    for node in &frontier {
        for (edge, neighbor) in overlay_node_edges(node, &scope) {
            if visited.contains(&neighbor) {
                push_once(&mut edges, &mut seen, edge);
            }
        }
    }

    edges
}

/// Each edge of `node` in the walk's directions, durable then staged, as
/// `(physical edge, neighbour)`. A tombstoned durable edge is skipped.
fn overlay_node_edges(
    node: &str,
    scope: &OverlayEdgeScope<'_>,
) -> Vec<((String, String, String), String)> {
    let mut out = Vec::new();
    if let Some((csr, labels)) = scope.partition.as_ref()
        && let Some(&node_id) = csr.node_to_id.get(node)
    {
        csr.record_access(node_id);
        if scope.want_out {
            for (lid, dst) in csr.dense_iter_out(node_id) {
                if !labels.keeps(lid) {
                    continue;
                }
                let label = csr.label_name(lid);
                let dst_name = &csr.id_to_node[dst as usize];
                if !scope.overlay.is_tombstoned(node, label, dst_name) {
                    out.push((
                        (node.to_string(), label.to_string(), dst_name.clone()),
                        dst_name.clone(),
                    ));
                }
            }
        }
        if scope.want_in {
            for (lid, src) in csr.dense_iter_in(node_id) {
                if !labels.keeps(lid) {
                    continue;
                }
                let label = csr.label_name(lid);
                let src_name = &csr.id_to_node[src as usize];
                if !scope.overlay.is_tombstoned(src_name, label, node) {
                    out.push((
                        (src_name.clone(), label.to_string(), node.to_string()),
                        src_name.clone(),
                    ));
                }
            }
        }
    }
    if scope.want_out {
        for (label, dst) in scope.overlay.out_neighbors(node, scope.label_filter) {
            out.push((
                (node.to_string(), label.to_string(), dst.to_string()),
                dst.to_string(),
            ));
        }
    }
    if scope.want_in {
        for (label, src) in scope.overlay.in_neighbors(node, scope.label_filter) {
            out.push((
                (src.to_string(), label.to_string(), node.to_string()),
                src.to_string(),
            ));
        }
    }
    out
}

/// What one overlay subgraph walk keeps of each node's edges.
struct OverlayEdgeScope<'a> {
    /// The durable partition and the walk's labels resolved against it.
    partition: Option<(&'a CsrIndex, LabelFilter)>,
    label_filter: &'a [&'a str],
    want_out: bool,
    want_in: bool,
    overlay: &'a GraphOverlayDelta,
}

/// Append `edge` unless `seen` already holds it.
fn push_once(
    edges: &mut Vec<(String, String, String)>,
    seen: &mut HashSet<(String, String, String)>,
    edge: (String, String, String),
) {
    if seen.insert(edge.clone()) {
        edges.push(edge);
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
                label_filter: &["KNOWS"],
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
                label_filter: &["KNOWS"],
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
                label_filter: &["KNOWS"],
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
            &["KNOWS"],
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
    fn subgraph_both_returns_each_physical_edge_once() {
        // Durable a->b. Staged b->a and a self-loop on a.
        let csr = base();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("b", "KNOWS", "a");
        ov.stage_edge("a", "KNOWS", "a");
        let mut edges = csr.subgraph(
            &["a"],
            &[],
            Direction::Both,
            3,
            DEFAULT_MAX_VISITED,
            Some(&ov),
        );
        edges.sort();
        let expected: Vec<(String, String, String)> = [
            ("a", "KNOWS", "a"),
            ("a", "KNOWS", "b"),
            ("b", "KNOWS", "a"),
        ]
        .iter()
        .map(|(s, l, d)| (s.to_string(), l.to_string(), d.to_string()))
        .collect();
        assert_eq!(edges, expected);
    }

    #[test]
    fn subgraph_in_direction_surfaces_staged_in_edge() {
        // Staged in-edge z->a; querying subgraph In from "a" surfaces it.
        let csr = base();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("z", "KNOWS", "a");

        let edges = csr.subgraph(
            &["a"],
            &["KNOWS"],
            Direction::In,
            1,
            DEFAULT_MAX_VISITED,
            Some(&ov),
        );
        assert!(edges.contains(&("z".into(), "KNOWS".into(), "a".into())));
    }

    /// Durable `a -KNOWS-> b`. Staged `a -LIKES-> x` and `a -HATES-> y`. The
    /// set `["KNOWS", "LIKES"]` follows the staged edge under its second label.
    #[test]
    fn a_staged_edge_under_the_second_label_of_a_set_is_followed() {
        let csr = base();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("a", "LIKES", "x");
        ov.stage_edge("a", "HATES", "y");

        let mut r = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: &["KNOWS", "LIKES"],
                direction: Direction::Out,
                max_depth: 1,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            Some(&ov),
        );
        r.sort();
        assert_eq!(r, vec!["a", "b", "x"]);

        let mut edges = csr.subgraph(
            &["a"],
            &["KNOWS", "LIKES"],
            Direction::Out,
            1,
            DEFAULT_MAX_VISITED,
            Some(&ov),
        );
        edges.sort();
        let expected: Vec<(String, String, String)> = [("a", "KNOWS", "b"), ("a", "LIKES", "x")]
            .iter()
            .map(|(s, l, d)| (s.to_string(), l.to_string(), d.to_string()))
            .collect();
        assert_eq!(edges, expected);
    }

    fn out_params<'a>(starts: &'a [&'a str], depth: usize) -> BfsParams<'a> {
        BfsParams {
            start_nodes: starts,
            label_filter: &[],
            direction: Direction::Out,
            max_depth: depth,
            max_visited: DEFAULT_MAX_VISITED,
            frontier_bitmap: None,
        }
    }

    /// A start the partition lacks and no staged edge names is dropped, as
    /// in the dense walk. A start a staged edge names is kept.
    #[test]
    fn an_absent_start_is_dropped() {
        let csr = base();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("x", "KNOWS", "y");

        let mut r = csr.traverse_bfs(out_params(&["a", "ghost", "x"], 1), Some(&ov));
        r.sort();
        assert_eq!(r, vec!["a", "b", "x", "y"]);
        let dense = csr.traverse_bfs(out_params(&["ghost"], 1), None);
        assert!(dense.is_empty());

        let edges = csr.subgraph(
            &["ghost"],
            &[],
            Direction::Out,
            2,
            DEFAULT_MAX_VISITED,
            Some(&ov),
        );
        assert!(edges.is_empty());
    }

    /// With no partition, the walks follow staged edges only, and keep only
    /// starts a staged edge names.
    #[test]
    fn a_missing_partition_walks_staged_edges() {
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("a", "KNOWS", "x");
        ov.stage_edge("x", "KNOWS", "y");

        let mut r = CsrIndex::traverse_bfs_on(None, out_params(&["a", "ghost"], 1), Some(&ov));
        r.sort();
        assert_eq!(r, vec!["a", "x"]);
        let mut r = CsrIndex::traverse_bfs_on(None, out_params(&["a"], 2), Some(&ov));
        r.sort();
        assert_eq!(r, vec!["a", "x", "y"]);
        assert!(CsrIndex::traverse_bfs_on(None, out_params(&["a"], 2), None).is_empty());

        let mut edges = CsrIndex::subgraph_on(
            None,
            &["a"],
            &[],
            Direction::Out,
            2,
            DEFAULT_MAX_VISITED,
            Some(&ov),
        );
        edges.sort();
        let expected: Vec<(String, String, String)> = [("a", "KNOWS", "x"), ("x", "KNOWS", "y")]
            .iter()
            .map(|(s, l, d)| (s.to_string(), l.to_string(), d.to_string()))
            .collect();
        assert_eq!(edges, expected);
        assert!(
            CsrIndex::subgraph_on(
                None,
                &["a"],
                &[],
                Direction::Out,
                2,
                DEFAULT_MAX_VISITED,
                None
            )
            .is_empty()
        );
    }

    #[test]
    fn empty_overlay_matches_durable() {
        let csr = base();
        let ov = GraphOverlayDelta::new();
        let mut with = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: &[],
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
                label_filter: &[],
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
