// SPDX-License-Identifier: BUSL-1.1

//! Distributed WCC — single-round cross-shard component stitching.
//!
//! Each shard computes local WCC via union-find and reports its owned nodes'
//! local component labels plus its owned→ghost boundary edges. The coordinator
//! stitches every shard's result into one global component assignment in a
//! single contraction round via [`stitch_components`].

use std::collections::HashMap;

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

#[cfg(test)]
mod tests {
    use super::*;

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
