// SPDX-License-Identifier: BUSL-1.1

//! Distributed WCC — cross-shard component merging via label propagation.
//!
//! Each shard computes local WCC via union-find, then iteratively exchanges
//! component labels across shard boundaries. For each cross-shard edge,
//! the target shard adopts the lexicographically smaller label. Converges
//! when no shard changes any labels in a round.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Stitch every shard's local WCC result into one global component assignment.
///
/// Single-round contraction: each shard has already computed connected
/// components over its OWNED nodes and returned `node_labels`
/// (`(name, local_component_root_name)` for every owned node) plus
/// `boundary_edges` (`(owned_name, ghost_name)` for every owned→ghost out-edge).
/// This builds ONE string-keyed union-find over node names:
///
/// 1. union each `(name, local_root)` — re-establishes every shard's local
///    components in the global structure, and registers every owned node name.
/// 2. union each boundary edge `(a, b)` — stitches components that span shard
///    boundaries (a ghost endpoint `b` not present in any `node_labels` is still
///    registered here, so a cross-shard edge to a node owned by ANOTHER shard
///    correctly merges the two components once both shards report).
///
/// Then each global component is assigned a dense `i64` id ordered by the
/// component's minimum node name, and one `(node_name, component_id)` row is
/// emitted per registered node name. Row order is the deterministic
/// name-sorted order so output is stable across runs.
pub fn stitch_components(
    node_labels: Vec<(String, String)>,
    boundary_edges: Vec<(String, String)>,
) -> Vec<(String, i64)> {
    let mut uf = StringUnionFind::default();

    for (name, local_root) in &node_labels {
        uf.union(name, local_root);
    }
    for (a, b) in &boundary_edges {
        uf.union(a, b);
    }

    // Collect every registered name and resolve its global root.
    let names = uf.names();
    let mut root_of: HashMap<String, String> = HashMap::with_capacity(names.len());
    for name in &names {
        root_of.insert(name.clone(), uf.find(name));
    }

    // Each component's canonical key = the lexicographically-minimum node name
    // in that component. Assign dense ids ordered by that minimum name.
    let mut component_min: HashMap<String, String> = HashMap::new();
    for name in &names {
        let root = &root_of[name];
        component_min
            .entry(root.clone())
            .and_modify(|m| {
                if name < m {
                    *m = name.clone();
                }
            })
            .or_insert_with(|| name.clone());
    }

    // Dense id per component, ordered by the component's minimum name.
    let mut mins: Vec<(&String, &String)> = component_min.iter().collect();
    // Sort components by their minimum node name.
    mins.sort_by(|a, b| a.1.cmp(b.1));
    let mut id_of_root: HashMap<&String, i64> = HashMap::with_capacity(mins.len());
    for (id, (root, _min)) in mins.iter().enumerate() {
        id_of_root.insert(*root, id as i64);
    }

    // Emit one row per node name in deterministic name-sorted order.
    let mut sorted_names = names;
    sorted_names.sort();
    sorted_names
        .into_iter()
        .map(|name| {
            let root = &root_of[&name];
            let id = id_of_root[root];
            (name, id)
        })
        .collect()
}

/// String-keyed disjoint-set with path-compressed `find` over interned names.
///
/// Names are interned to dense indices on first sight; the union-find runs over
/// those indices for O(α) operations while preserving the string identity for
/// the coordinator's min-name component keying.
#[derive(Default)]
struct StringUnionFind {
    index: HashMap<String, usize>,
    names: Vec<String>,
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl StringUnionFind {
    /// Intern `name`, returning its dense index (registering it if new).
    fn intern(&mut self, name: &str) -> usize {
        if let Some(&i) = self.index.get(name) {
            return i;
        }
        let i = self.names.len();
        self.index.insert(name.to_string(), i);
        self.names.push(name.to_string());
        self.parent.push(i);
        self.rank.push(0);
        i
    }

    fn find_idx(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    /// Union the components of `a` and `b` (both interned on demand).
    fn union(&mut self, a: &str, b: &str) {
        let ia = self.intern(a);
        let ib = self.intern(b);
        let ra = self.find_idx(ia);
        let rb = self.find_idx(ib);
        if ra == rb {
            return;
        }
        match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => self.parent[ra] = rb,
            std::cmp::Ordering::Greater => self.parent[rb] = ra,
            std::cmp::Ordering::Equal => {
                self.parent[rb] = ra;
                self.rank[ra] += 1;
            }
        }
    }

    /// Resolve the root NAME of the component containing `name`.
    /// `name` must already be interned (every queried name was unioned in).
    fn find(&mut self, name: &str) -> String {
        let i = self.index[name];
        let r = self.find_idx(i);
        self.names[r].clone()
    }

    /// Every interned node name.
    fn names(&self) -> Vec<String> {
        self.names.clone()
    }
}

/// Cross-shard component merge request: shard → target shard.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ComponentMergeRequest {
    pub round: u32,
    pub source_shard: u32,
    /// (target_vertex_name, source_component_label).
    pub merges: Vec<(String, String)>,
}

/// WCC round acknowledgement: shard → coordinator.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct WccRoundAck {
    pub shard_id: u32,
    pub round: u32,
    pub labels_changed: usize,
    pub merges_sent: usize,
}

/// Per-shard WCC execution state.
#[derive(Debug)]
pub struct ShardWccState {
    pub vertex_count: usize,
    parent: Vec<usize>,
    rank: Vec<u8>,
    pub global_labels: Vec<String>,
    pub shard_id: u32,
    pub boundary_edges: HashMap<u32, Vec<(String, u32)>>,
    node_names: Vec<String>,
}

impl ShardWccState {
    /// Initialize WCC state for a local CSR partition.
    pub fn init(
        vertex_count: usize,
        shard_id: u32,
        node_names: Vec<String>,
        local_edges: &dyn Fn(u32) -> Vec<u32>,
        ghost_edges: &dyn Fn(u32) -> Vec<(String, u32)>,
    ) -> Self {
        let parent: Vec<usize> = (0..vertex_count).collect();
        let rank = vec![0u8; vertex_count];

        let mut state = Self {
            vertex_count,
            parent,
            rank,
            global_labels: Vec::new(),
            shard_id,
            boundary_edges: HashMap::new(),
            node_names,
        };

        // Local union-find pass.
        for u in 0..vertex_count {
            for v in local_edges(u as u32) {
                state.union(u, v as usize);
            }
        }

        // Build boundary edge map.
        for u in 0..vertex_count {
            let ghosts = ghost_edges(u as u32);
            if !ghosts.is_empty() {
                state.boundary_edges.insert(u as u32, ghosts);
            }
        }

        // Initialize global labels from local roots.
        state.global_labels = (0..vertex_count)
            .map(|i| {
                let root = state.find(i);
                format!("{}:{}", shard_id, state.node_names[root])
            })
            .collect();

        state
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => self.parent[ra] = rb,
            std::cmp::Ordering::Greater => self.parent[rb] = ra,
            std::cmp::Ordering::Equal => {
                self.parent[rb] = ra;
                self.rank[ra] += 1;
            }
        }
    }

    /// Produce outbound merge requests for boundary edges.
    pub fn round(&self) -> (HashMap<u32, Vec<(String, String)>>, usize) {
        let mut outbound: HashMap<u32, Vec<(String, String)>> = HashMap::new();

        for (&local_id, ghost_list) in &self.boundary_edges {
            let root = find_static(&self.parent, local_id as usize);
            let label = self.global_labels[root].clone();
            for (remote_name, target_shard) in ghost_list {
                outbound
                    .entry(*target_shard)
                    .or_default()
                    .push((remote_name.clone(), label.clone()));
            }
        }

        let sent: usize = outbound.values().map(|v| v.len()).sum();
        (outbound, sent)
    }

    /// Apply incoming merges. Returns number of labels changed.
    pub fn apply_merges(&mut self, merges: &[(String, String)]) -> usize {
        let mut changed = 0;

        let name_to_id: HashMap<&str, usize> = self
            .node_names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.as_str(), i))
            .collect();

        for (vertex_name, remote_label) in merges {
            let Some(&local_id) = name_to_id.get(vertex_name.as_str()) else {
                continue;
            };

            let root = find_static(&self.parent, local_id);
            let local_label = &self.global_labels[root];

            if local_label != remote_label && *remote_label < *local_label {
                self.global_labels[root] = remote_label.clone();
                changed += 1;
            }
        }

        // Propagate updated labels to all nodes.
        for i in 0..self.vertex_count {
            let root = find_static(&self.parent, i);
            if i != root {
                self.global_labels[i] = self.global_labels[root].clone();
            }
        }

        changed
    }

    /// Get current component assignment: (vertex_name, global_label).
    pub fn component_labels(&self) -> Vec<(String, String)> {
        (0..self.vertex_count)
            .map(|i| {
                let root = find_static(&self.parent, i);
                (self.node_names[i].clone(), self.global_labels[root].clone())
            })
            .collect()
    }
}

/// Non-mutating find (no path compression). Borrow-safe.
fn find_static(parent: &[usize], mut x: usize) -> usize {
    while parent[x] != x {
        x = parent[x];
    }
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_merge_request_serde() {
        let req = ComponentMergeRequest {
            round: 2,
            source_shard: 1,
            merges: vec![("alice".into(), "0:root_a".into())],
        };
        let bytes = zerompk::to_msgpack_vec(&req).unwrap();
        let decoded: ComponentMergeRequest = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.round, 2);
    }

    #[test]
    fn wcc_round_ack_serde() {
        let ack = WccRoundAck {
            shard_id: 3,
            round: 1,
            labels_changed: 5,
            merges_sent: 12,
        };
        let bytes = zerompk::to_msgpack_vec(&ack).unwrap();
        let decoded: WccRoundAck = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.labels_changed, 5);
    }

    #[test]
    fn wcc_shard_local_only() {
        let state = ShardWccState::init(
            3,
            0,
            vec!["a".into(), "b".into(), "c".into()],
            &|node| match node {
                0 => vec![1],
                1 => vec![2],
                _ => Vec::new(),
            },
            &|_| Vec::new(),
        );
        let labels = state.component_labels();
        assert_eq!(labels[0].1, labels[1].1);
        assert_eq!(labels[1].1, labels[2].1);
    }

    #[test]
    fn wcc_shard_with_boundary_edges() {
        let state = ShardWccState::init(
            2,
            0,
            vec!["a".into(), "b".into()],
            &|node| match node {
                0 => vec![1],
                _ => Vec::new(),
            },
            &|node| {
                if node == 1 {
                    vec![("c".into(), 1)]
                } else {
                    Vec::new()
                }
            },
        );
        assert_eq!(state.boundary_edges.len(), 1);
        let (outbound, sent) = state.round();
        assert!(outbound.contains_key(&1));
        assert_eq!(sent, 1);
    }

    #[test]
    fn wcc_apply_merges_adopts_smaller_label() {
        let mut state = ShardWccState::init(
            2,
            1,
            vec!["c".into(), "d".into()],
            &|node| match node {
                0 => vec![1],
                _ => Vec::new(),
            },
            &|_| Vec::new(),
        );
        let changed = state.apply_merges(&[("c".into(), "0:a".into())]);
        assert!(changed > 0);
        let labels = state.component_labels();
        assert_eq!(labels[0].1, "0:a");
        assert_eq!(labels[1].1, "0:a");
    }

    #[test]
    fn wcc_apply_merges_keeps_smaller_label() {
        let mut state =
            ShardWccState::init(1, 0, vec!["a".into()], &|_| Vec::new(), &|_| Vec::new());
        let changed = state.apply_merges(&[("a".into(), "1:z".into())]);
        assert_eq!(changed, 0);
        assert_eq!(state.component_labels()[0].1, "0:a");
    }

    #[test]
    fn wcc_multi_round_convergence() {
        let mut shard0 = ShardWccState::init(
            2,
            0,
            vec!["a".into(), "b".into()],
            &|node| match node {
                0 => vec![1],
                _ => Vec::new(),
            },
            &|node| {
                if node == 1 {
                    vec![("c".into(), 1)]
                } else {
                    Vec::new()
                }
            },
        );

        let mut shard1 = ShardWccState::init(
            2,
            1,
            vec!["c".into(), "d".into()],
            &|node| match node {
                0 => vec![1],
                _ => Vec::new(),
            },
            &|node| {
                if node == 0 {
                    vec![("b".into(), 0)]
                } else {
                    Vec::new()
                }
            },
        );

        // Round 1.
        let (out0, _) = shard0.round();
        let (out1, _) = shard1.round();
        let c0 = out1.get(&0).map_or(0, |m| shard0.apply_merges(m));
        let c1 = out0.get(&1).map_or(0, |m| shard1.apply_merges(m));
        assert!(c0 + c1 > 0);

        // Round 2.
        let (out0_r2, _) = shard0.round();
        let (out1_r2, _) = shard1.round();
        let c0_r2 = out1_r2.get(&0).map_or(0, |m| shard0.apply_merges(m));
        let c1_r2 = out0_r2.get(&1).map_or(0, |m| shard1.apply_merges(m));
        assert_eq!(c0_r2 + c1_r2, 0, "should converge");

        // All 4 nodes should share one global label.
        let l0 = shard0.component_labels();
        let l1 = shard1.component_labels();
        assert_eq!(l0[0].1, l1[0].1, "cross-shard merge");
    }

    fn id_map(rows: &[(String, i64)]) -> HashMap<&str, i64> {
        rows.iter().map(|(n, id)| (n.as_str(), *id)).collect()
    }

    #[test]
    fn stitch_single_shard_two_components() {
        // One shard, fully local: a-b in one component, z alone.
        let labels = vec![
            ("a".to_string(), "a".to_string()),
            ("b".to_string(), "a".to_string()),
            ("z".to_string(), "z".to_string()),
        ];
        let rows = stitch_components(labels, Vec::new());
        assert_eq!(rows.len(), 3);
        let m = id_map(&rows);
        assert_eq!(m["a"], m["b"]);
        assert_ne!(m["a"], m["z"]);
        // Dense ids ordered by component min name: {a,b} → 0, {z} → 1.
        assert_eq!(m["a"], 0);
        assert_eq!(m["z"], 1);
    }

    #[test]
    fn stitch_boundary_edge_merges_cross_shard_components() {
        // Shard 0 owns a,b (local root a). Shard 1 owns c,d (local root c).
        // A boundary edge b->c stitches the two shards into ONE component.
        let labels = vec![
            ("a".to_string(), "a".to_string()),
            ("b".to_string(), "a".to_string()),
            ("c".to_string(), "c".to_string()),
            ("d".to_string(), "c".to_string()),
        ];
        let boundary = vec![("b".to_string(), "c".to_string())];
        let rows = stitch_components(labels, boundary);
        assert_eq!(rows.len(), 4);
        let m = id_map(&rows);
        // All four share one component.
        assert_eq!(m["a"], m["b"]);
        assert_eq!(m["b"], m["c"]);
        assert_eq!(m["c"], m["d"]);
        // Single component → dense id 0.
        assert_eq!(m["a"], 0);
    }

    #[test]
    fn stitch_dense_ids_ordered_by_min_name() {
        // Two disjoint components reported across two shards. Min names z and a;
        // the component with min name "a" must get id 0, "z" gets id 1.
        let labels = vec![
            ("z0".to_string(), "z0".to_string()),
            ("z1".to_string(), "z0".to_string()),
            ("a0".to_string(), "a0".to_string()),
            ("a1".to_string(), "a0".to_string()),
        ];
        let rows = stitch_components(labels, Vec::new());
        let m = id_map(&rows);
        assert_eq!(m["a0"], 0, "component with min name a0 → id 0");
        assert_eq!(m["a1"], 0);
        assert_eq!(m["z0"], 1, "component with min name z0 → id 1");
        assert_eq!(m["z1"], 1);
        // Row order is deterministic name-sorted.
        let order: Vec<&str> = rows.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(order, vec!["a0", "a1", "z0", "z1"]);
    }

    #[test]
    fn stitch_boundary_edge_to_unreported_ghost() {
        // A boundary edge naming a ghost not present in node_labels still
        // registers the ghost and keeps it in the same component as its owner.
        let labels = vec![("a".to_string(), "a".to_string())];
        let boundary = vec![("a".to_string(), "g".to_string())];
        let rows = stitch_components(labels, boundary);
        let m = id_map(&rows);
        assert_eq!(rows.len(), 2);
        assert_eq!(m["a"], m["g"]);
    }

    #[test]
    fn stitch_empty() {
        let rows = stitch_components(Vec::new(), Vec::new());
        assert!(rows.is_empty());
    }
}
