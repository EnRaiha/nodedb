// SPDX-License-Identifier: Apache-2.0

//! Graph traversal algorithms on the CSR index.
//!
//! BFS, bidirectional shortest path, and subgraph materialization.
//! All algorithms respect a max-visited cap to prevent supernode fan-out
//! explosion from consuming unbounded memory.
//!
//! Access tracking and prefetch hints are integrated: each traversal records
//! node access for hot/cold partition decisions, and prefetches frontier
//! neighbors for cache efficiency.

use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};

pub use nodedb_types::config::tuning::DEFAULT_MAX_VISITED;

use crate::bfs_params::BfsParams;
use crate::csr::{CsrIndex, Direction};
use crate::overlay_delta::GraphOverlayDelta;
use crate::path_params::ShortestPathParams;

impl CsrIndex {
    /// BFS traversal. Returns all reachable node IDs within max_depth hops.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    ///
    /// `frontier_bitmap`: when `Some`, only nodes whose surrogate is present in the
    /// bitmap are eligible as traversal targets. Start nodes are not gated — only
    /// newly discovered frontier nodes are checked.
    ///
    /// `overlay`: when `Some` and non-empty, the traversal observes the
    /// transaction's staged edge writes/deletes (read-your-own-writes),
    /// including through nodes reachable only via a staged edge. When `None`
    /// or empty, the durable-only dense fast path runs unchanged.
    pub fn traverse_bfs(
        &self,
        params: BfsParams<'_>,
        overlay: Option<&GraphOverlayDelta>,
    ) -> Vec<String> {
        match overlay {
            Some(ov) if !ov.is_empty() => self.traverse_bfs_overlay(params, ov),
            _ => self.traverse_bfs_dense(params),
        }
    }

    /// Durable-only BFS over the dense u32 CSR ids.
    ///
    /// The walk runs level by level. Each level's new nodes are admitted in
    /// node-name order until `max_visited` nodes are visited, so a capped walk
    /// admits the same nodes however the edges are stored. A cluster
    /// coordinator walking the same edges across partitions admits the same
    /// nodes.
    fn traverse_bfs_dense(&self, params: BfsParams<'_>) -> Vec<String> {
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
        let mut visited: HashSet<u32> = HashSet::new();
        let mut frontier: Vec<u32> = Vec::new();
        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node)
                && visited.insert(id)
            {
                frontier.push(id);
            }
        }

        for _depth in 0..max_depth {
            if frontier.is_empty() || visited.len() >= max_visited {
                break;
            }
            let mut candidates: Vec<u32> = Vec::new();
            for &node_id in &frontier {
                // Track access for hot/cold partition decisions.
                self.record_access(node_id);
                if matches!(direction, Direction::Out | Direction::Both) {
                    for (lid, dst) in self.dense_iter_out(node_id) {
                        if labels.keeps(lid) && !visited.contains(&dst) && in_bitmap(dst) {
                            candidates.push(dst);
                        }
                    }
                }
                if matches!(direction, Direction::In | Direction::Both) {
                    for (lid, src) in self.dense_iter_in(node_id) {
                        if labels.keeps(lid) && !visited.contains(&src) && in_bitmap(src) {
                            candidates.push(src);
                        }
                    }
                }
            }
            frontier = self.admit_by_name(candidates, &mut visited, max_visited);
        }

        visited
            .into_iter()
            .map(|id| self.id_to_node[id as usize].clone())
            .collect()
    }

    /// Admit `candidates` into `visited` in node-name order, until `visited`
    /// holds `max_visited` nodes. Returns the nodes admitted, in name order.
    pub(crate) fn admit_by_name(
        &self,
        mut candidates: Vec<u32>,
        visited: &mut HashSet<u32>,
        max_visited: usize,
    ) -> Vec<u32> {
        self.sort_by_name(&mut candidates);
        candidates.dedup();
        let mut admitted = Vec::with_capacity(candidates.len());
        for id in candidates {
            if visited.len() >= max_visited {
                break;
            }
            if visited.insert(id) {
                self.prefetch_node(id);
                admitted.push(id);
            }
        }
        admitted
    }

    /// BFS traversal returning nodes with depth information.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    pub fn traverse_bfs_with_depth(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<(String, u8)> {
        let filters: Vec<&str> = label_filter.into_iter().collect();
        self.traverse_bfs_with_depth_multi(start_nodes, &filters, direction, max_depth, max_visited)
    }

    /// BFS traversal with multi-label filter. Empty labels = all edges.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    pub fn traverse_bfs_with_depth_multi(
        &self,
        start_nodes: &[&str],
        label_filters: &[&str],
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<(String, u8)> {
        let label_ids: Vec<u32> = label_filters
            .iter()
            .filter_map(|l| self.label_id(l))
            .collect();
        // Filters this partition has never seen match no edge here; they must
        // not widen the filter to every edge.
        let match_label = |lid: u32| label_filters.is_empty() || label_ids.contains(&lid);
        let mut visited: HashMap<u32, u8> = HashMap::new();
        let mut queue: VecDeque<(u32, u8)> = VecDeque::new();

        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node) {
                visited.insert(id, 0);
                queue.push_back((id, 0));
            }
        }

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth as usize >= max_depth || visited.len() >= max_visited {
                continue;
            }

            let next_depth = depth + 1;

            if matches!(direction, Direction::Out | Direction::Both) {
                for (lid, dst) in self.dense_iter_out(node_id) {
                    if match_label(lid)
                        && visited.len() < max_visited
                        && !visited.contains_key(&dst)
                    {
                        visited.insert(dst, next_depth);
                        queue.push_back((dst, next_depth));
                    }
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for (lid, src) in self.dense_iter_in(node_id) {
                    if match_label(lid)
                        && visited.len() < max_visited
                        && !visited.contains_key(&src)
                    {
                        visited.insert(src, next_depth);
                        queue.push_back((src, next_depth));
                    }
                }
            }
        }

        visited
            .into_iter()
            .map(|(id, depth)| (self.id_to_node[id as usize].clone(), depth))
            .collect()
    }

    /// Shortest path via bidirectional BFS.
    ///
    /// `max_visited` caps the combined forward+backward visited set to prevent
    /// supernode fan-out explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    ///
    /// `frontier_bitmap`: when `Some`, only nodes whose surrogate is present in the
    /// bitmap are eligible for expansion. Start and end nodes are not gated.
    ///
    /// `overlay`: when `Some` and non-empty, the search observes the
    /// transaction's staged edge writes/deletes (read-your-own-writes),
    /// including a path that must pass through a node reachable only via a
    /// staged edge. When `None` or empty, the durable-only dense bidirectional
    /// fast path runs unchanged.
    pub fn shortest_path(
        &self,
        params: ShortestPathParams<'_>,
        overlay: Option<&GraphOverlayDelta>,
    ) -> Option<Vec<String>> {
        match overlay {
            Some(ov) if !ov.is_empty() => self.shortest_path_overlay(params, ov),
            _ => self.shortest_path_dense(params),
        }
    }

    /// Durable-only bidirectional BFS over the dense u32 CSR ids.
    ///
    /// Each step expands one forward level, then one backward level. A level
    /// relaxes its edges in `(neighbour, frontier node)` name order, and the
    /// search stops at the first node both sides reached. The cap is checked
    /// before each step.
    fn shortest_path_dense(&self, params: ShortestPathParams<'_>) -> Option<Vec<String>> {
        let ShortestPathParams {
            src,
            dst,
            label_filter,
            max_depth,
            max_visited,
            frontier_bitmap,
        } = params;
        let src_id = *self.node_to_id.get(src)?;
        let dst_id = *self.node_to_id.get(dst)?;
        if src_id == dst_id {
            return Some(vec![src.to_string()]);
        }

        let labels = self.label_filter(label_filter);
        let in_bitmap = |id: u32| {
            frontier_bitmap.is_none_or(|bm| {
                bm.contains(nodedb_types::Surrogate::new(self.node_surrogate_raw(id)))
            })
        };
        let mut fwd_parent: HashMap<u32, u32> = HashMap::new();
        let mut bwd_parent: HashMap<u32, u32> = HashMap::new();
        fwd_parent.insert(src_id, src_id);
        bwd_parent.insert(dst_id, dst_id);

        let mut fwd_frontier: Vec<u32> = vec![src_id];
        let mut bwd_frontier: Vec<u32> = vec![dst_id];

        for _depth in 0..max_depth {
            if fwd_parent.len() + bwd_parent.len() >= max_visited {
                break;
            }

            // Each level's edges are relaxed in (neighbour, frontier node)
            // name order, so the parent a node gets, and the meeting point,
            // do not depend on how the edges are stored. A cluster coordinator
            // relaxes cross-shard hops in the same order.
            let mut candidates: Vec<(u32, u32)> = Vec::new();
            for &node in &fwd_frontier {
                self.record_access(node);
                for (lid, neighbor) in self.dense_iter_out(node) {
                    if labels.keeps(lid) && in_bitmap(neighbor) {
                        candidates.push((neighbor, node));
                    }
                }
            }
            self.sort_edges_by_name(&mut candidates);
            let mut next_fwd = Vec::new();
            for (neighbor, node) in candidates {
                if let Entry::Vacant(e) = fwd_parent.entry(neighbor) {
                    e.insert(node);
                    next_fwd.push(neighbor);
                }
                if bwd_parent.contains_key(&neighbor) {
                    return Some(self.reconstruct_path(neighbor, &fwd_parent, &bwd_parent));
                }
            }
            fwd_frontier = next_fwd;

            let mut candidates: Vec<(u32, u32)> = Vec::new();
            for &node in &bwd_frontier {
                self.record_access(node);
                for (lid, neighbor) in self.dense_iter_in(node) {
                    if labels.keeps(lid) && in_bitmap(neighbor) {
                        candidates.push((neighbor, node));
                    }
                }
            }
            self.sort_edges_by_name(&mut candidates);
            let mut next_bwd = Vec::new();
            for (neighbor, node) in candidates {
                if let Entry::Vacant(e) = bwd_parent.entry(neighbor) {
                    e.insert(node);
                    next_bwd.push(neighbor);
                }
                if fwd_parent.contains_key(&neighbor) {
                    return Some(self.reconstruct_path(neighbor, &fwd_parent, &bwd_parent));
                }
            }
            bwd_frontier = next_bwd;

            if fwd_frontier.is_empty() && bwd_frontier.is_empty() {
                break;
            }
        }
        None
    }

    /// Order `(neighbour, frontier node)` edges by the two node names.
    fn sort_edges_by_name(&self, edges: &mut [(u32, u32)]) {
        edges.sort_by(|a, b| {
            let name = |id: u32| self.node_name_checked(id);
            (name(a.0), name(a.1)).cmp(&(name(b.0), name(b.1)))
        });
    }

    fn reconstruct_path(
        &self,
        meeting: u32,
        fwd_parent: &HashMap<u32, u32>,
        bwd_parent: &HashMap<u32, u32>,
    ) -> Vec<String> {
        let mut fwd_path = Vec::new();
        let mut current = meeting;
        loop {
            fwd_path.push(current);
            let parent = fwd_parent[&current];
            if parent == current {
                break;
            }
            current = parent;
        }
        fwd_path.reverse();

        current = bwd_parent[&meeting];
        if current != meeting {
            loop {
                fwd_path.push(current);
                let parent = bwd_parent[&current];
                if parent == current {
                    break;
                }
                current = parent;
            }
        }

        fwd_path
            .into_iter()
            .map(|id| self.id_to_node[id as usize].clone())
            .collect()
    }

    /// Materialize a subgraph as `(src, label, dst)` edge tuples within
    /// max_depth, expanding in `direction`.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    ///
    /// `overlay`: when `Some` and non-empty, staged edges are included and
    /// staged tombstones subtract durable edges (read-your-own-writes),
    /// including through staged-only intermediate nodes. When `None` or
    /// empty, the durable-only dense path runs unchanged.
    pub fn subgraph(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
        overlay: Option<&GraphOverlayDelta>,
    ) -> Vec<(String, String, String)> {
        match overlay {
            Some(ov) if !ov.is_empty() => self.subgraph_overlay(
                start_nodes,
                label_filter,
                direction,
                max_depth,
                max_visited,
                ov,
            ),
            _ => self.subgraph_dense(start_nodes, label_filter, direction, max_depth, max_visited),
        }
    }

    /// Durable-only subgraph materialization over the dense u32 CSR ids.
    fn subgraph_dense(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<(String, String, String)> {
        let labels = self.label_filter(label_filter);
        let mut visited: HashSet<u32> = HashSet::new();
        let mut frontier: Vec<u32> = Vec::new();
        let mut edges = Vec::new();

        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node)
                && visited.insert(id)
            {
                frontier.push(id);
            }
        }

        // Level by level, as `traverse_bfs_dense`: every frontier node's edges
        // are recorded, then the level's new nodes are admitted in name order.
        for _depth in 0..max_depth {
            if frontier.is_empty() || visited.len() >= max_visited {
                break;
            }
            let mut candidates: Vec<u32> = Vec::new();
            for &node_id in &frontier {
                self.record_access(node_id);
                if matches!(direction, Direction::Out | Direction::Both) {
                    for (lid, dst) in self.dense_iter_out(node_id) {
                        if labels.keeps(lid) {
                            edges.push((
                                self.id_to_node[node_id as usize].clone(),
                                self.label_name(lid).to_string(),
                                self.id_to_node[dst as usize].clone(),
                            ));
                            if !visited.contains(&dst) {
                                candidates.push(dst);
                            }
                        }
                    }
                }
                if matches!(direction, Direction::In | Direction::Both) {
                    for (lid, src) in self.dense_iter_in(node_id) {
                        if labels.keeps(lid) {
                            edges.push((
                                self.id_to_node[src as usize].clone(),
                                self.label_name(lid).to_string(),
                                self.id_to_node[node_id as usize].clone(),
                            ));
                            if !visited.contains(&src) {
                                candidates.push(src);
                            }
                        }
                    }
                }
            }
            frontier = self.admit_by_name(candidates, &mut visited, max_visited);
        }

        edges
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_memory;

    fn make_csr() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "b").unwrap();
        csr.add_edge("b", "KNOWS", "c").unwrap();
        csr.add_edge("c", "KNOWS", "d").unwrap();
        csr.add_edge("a", "WORKS", "e").unwrap();
        csr
    }

    #[test]
    fn bfs_traversal() {
        let csr = make_csr();
        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: Some("KNOWS"),
                direction: Direction::Out,
                max_depth: 2,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            None,
        );
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn bfs_all_labels() {
        let csr = make_csr();
        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: None,
                direction: Direction::Out,
                max_depth: 1,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            None,
        );
        result.sort();
        assert_eq!(result, vec!["a", "b", "e"]);
    }

    /// `a` points at `z`, `m` and `b`, stored in that order. A cap of 3 leaves
    /// room for two of them, and name order picks `b` and `m`.
    fn fan_out_csr() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "z").unwrap();
        csr.add_edge("a", "L", "m").unwrap();
        csr.add_edge("a", "L", "b").unwrap();
        csr
    }

    #[test]
    fn a_capped_bfs_admits_each_level_in_name_order() {
        let csr = fan_out_csr();
        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: None,
                direction: Direction::Out,
                max_depth: 2,
                max_visited: 3,
                frontier_bitmap: None,
            },
            None,
        );
        result.sort();
        assert_eq!(result, vec!["a", "b", "m"]);
    }

    #[test]
    fn a_capped_subgraph_expands_only_admitted_levels() {
        let mut csr = fan_out_csr();
        csr.add_edge("z", "L", "y").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        let mut edges = csr.subgraph(&["a"], None, Direction::Out, 3, 3, None);
        edges.sort();
        // Level 1 fills the cap, so no level-1 node expands.
        let expected: Vec<(String, String, String)> = ["b", "m", "z"]
            .iter()
            .map(|dst| ("a".to_string(), "L".to_string(), dst.to_string()))
            .collect();
        assert_eq!(edges, expected);
    }

    #[test]
    fn bfs_cycle() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: None,
                direction: Direction::Out,
                max_depth: 10,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            None,
        );
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn bfs_with_depth() {
        let csr = make_csr();
        let result = csr.traverse_bfs_with_depth(
            &["a"],
            Some("KNOWS"),
            Direction::Out,
            3,
            DEFAULT_MAX_VISITED,
        );
        let map: HashMap<String, u8> = result.into_iter().collect();
        assert_eq!(map["a"], 0);
        assert_eq!(map["b"], 1);
        assert_eq!(map["c"], 2);
        assert_eq!(map["d"], 3);
    }

    fn path_params<'a>(
        src: &'a str,
        dst: &'a str,
        label_filter: Option<&'a str>,
        max_depth: usize,
        frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    ) -> ShortestPathParams<'a> {
        ShortestPathParams {
            src,
            dst,
            label_filter,
            max_depth,
            max_visited: DEFAULT_MAX_VISITED,
            frontier_bitmap,
        }
    }

    #[test]
    fn shortest_path_direct() {
        let csr = make_csr();
        let path = csr
            .shortest_path(path_params("a", "c", Some("KNOWS"), 5, None), None)
            .unwrap();
        assert_eq!(path, vec!["a", "b", "c"]);
    }

    #[test]
    fn shortest_path_takes_the_smallest_named_tie() {
        // Two paths of equal length, the `z` one stored first.
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "z").unwrap();
        csr.add_edge("z", "L", "d").unwrap();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "d").unwrap();
        let path = csr
            .shortest_path(path_params("a", "d", None, 5, None), None)
            .unwrap();
        assert_eq!(path, vec!["a", "b", "d"]);
    }

    #[test]
    fn shortest_path_same_node() {
        let csr = make_csr();
        let path = csr
            .shortest_path(path_params("a", "a", None, 5, None), None)
            .unwrap();
        assert_eq!(path, vec!["a"]);
    }

    #[test]
    fn shortest_path_unreachable() {
        let csr = make_csr();
        let path = csr.shortest_path(path_params("d", "a", Some("KNOWS"), 5, None), None);
        assert!(path.is_none());
    }

    #[test]
    fn shortest_path_depth_limit() {
        let csr = make_csr();
        let path = csr.shortest_path(path_params("a", "d", Some("KNOWS"), 1, None), None);
        assert!(path.is_none());
    }

    #[test]
    fn subgraph_materialization() {
        let csr = make_csr();
        let edges = csr.subgraph(&["a"], None, Direction::Out, 2, DEFAULT_MAX_VISITED, None);
        assert_eq!(edges.len(), 3);
        assert!(edges.contains(&("a".into(), "KNOWS".into(), "b".into())));
        assert!(edges.contains(&("a".into(), "WORKS".into(), "e".into())));
        assert!(edges.contains(&("b".into(), "KNOWS".into(), "c".into())));
    }

    #[test]
    fn large_graph_bfs() {
        let mut csr = CsrIndex::new(test_memory());
        for i in 0..999 {
            csr.add_edge(&format!("n{i}"), "NEXT", &format!("n{}", i + 1))
                .unwrap();
        }
        csr.compact()
            .expect("test governor ceiling covers this reservation");

        let result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["n0"],
                label_filter: Some("NEXT"),
                direction: Direction::Out,
                max_depth: 100,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            None,
        );
        assert_eq!(result.len(), 101);

        let path = csr
            .shortest_path(path_params("n0", "n50", Some("NEXT"), 100, None), None)
            .unwrap();
        assert_eq!(path.len(), 51);
    }

    /// BFS with a frontier bitmap that includes only "b". Starting from "a",
    /// "b" is reachable but "c" is blocked (its surrogate is not in the bitmap).
    #[test]
    fn bfs_frontier_bitmap_excludes_non_members() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let mut csr = make_csr();
        // Assign surrogates: b=10, c=20, d=30. "a" and "e" get no surrogate.
        csr.set_node_surrogate("b", Surrogate::new(10));
        csr.set_node_surrogate("c", Surrogate::new(20));
        csr.set_node_surrogate("d", Surrogate::new(30));

        // Bitmap contains only "b" (surrogate 10).
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate::new(10));

        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: Some("KNOWS"),
                direction: Direction::Out,
                max_depth: 10,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: Some(&bm),
            },
            None,
        );
        result.sort();
        // "a" is the start node (not gated). "b" passes the bitmap. "c" is
        // excluded (surrogate 20 not in bitmap) so traversal stops there.
        assert_eq!(result, vec!["a", "b"]);
    }

    /// shortest_path with a bitmap that excludes the only intermediate node.
    /// "b" is the only path from "a" to "c" via KNOWS edges; if "b" is blocked
    /// then no path exists.
    #[test]
    fn shortest_path_frontier_bitmap_blocks_intermediate() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let mut csr = make_csr();
        csr.set_node_surrogate("b", Surrogate::new(10));
        csr.set_node_surrogate("c", Surrogate::new(20));

        // Bitmap that does NOT contain "b".
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate::new(20)); // only "c" is in the bitmap

        let path = csr.shortest_path(path_params("a", "c", Some("KNOWS"), 5, Some(&bm)), None);
        // "b" (surrogate 10) is not in the bitmap so expansion through it is
        // blocked, making the path from "a" to "c" unreachable.
        assert!(path.is_none());
    }
}
