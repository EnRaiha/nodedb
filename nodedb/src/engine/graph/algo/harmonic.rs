// SPDX-License-Identifier: BUSL-1.1

//! Harmonic Centrality — sum of inverse distances.
//!
//! `HC(v) = (1 / (n - 1)) * sum(1 / d(v, u))` for all u != v.
//! Unreachable nodes contribute 0 (d = infinity → 1/d = 0).
//!
//! Better than closeness for disconnected graphs — no normalization needed.
//! BFS from each node (unweighted). O(V * (V + E)).

use std::collections::VecDeque;

use super::result::AlgoResultBatch;
use super::util::cmp_desc_nan_last;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run Harmonic Centrality on the CSR index.
///
/// Returns `(node_id, centrality)` sorted by centrality descending.
pub fn run(csr: &CsrIndex) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::Harmonic);
    }

    let normalizer = if n > 1 { (n - 1) as f64 } else { 1.0 };
    let mut scored: Vec<(usize, f64)> = Vec::with_capacity(n);

    for v in 0..n {
        let inv_sum = bfs_inverse_distances(csr, v as u32, n);
        scored.push((v, inv_sum / normalizer));
    }

    scored.sort_by(|a, b| cmp_desc_nan_last(a.1, b.1));

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Harmonic);
    for (node, centrality) in scored {
        batch.push_node_f64(csr.node_name_raw(node as u32).to_string(), centrality);
    }
    batch
}

/// BFS from source, return sum of 1/d(source, u) for all reachable u.
fn bfs_inverse_distances(csr: &CsrIndex, source: u32, n: usize) -> f64 {
    let mut dist = vec![u32::MAX; n];
    dist[source as usize] = 0;
    let mut queue = VecDeque::new();
    queue.push_back(source);
    let mut inv_sum = 0.0f64;

    while let Some(v) = queue.pop_front() {
        let d = dist[v as usize];
        for (_, neighbor) in csr.iter_out_edges_raw(v) {
            let ni = neighbor as usize;
            if dist[ni] == u32::MAX {
                dist[ni] = d + 1;
                inv_sum += 1.0 / dist[ni] as f64;
                queue.push_back(neighbor);
            }
        }
        for (_, neighbor) in csr.iter_in_edges_raw(v) {
            let ni = neighbor as usize;
            if dist[ni] == u32::MAX {
                dist[ni] = d + 1;
                inv_sum += 1.0 / dist[ni] as f64;
                queue.push_back(neighbor);
            }
        }
    }

    inv_sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harmonic_path() {
        // a - b - c. b has shortest distances to a (1) and c (1).
        // HC(b) = (1/1 + 1/1) / 2 = 1.0
        // HC(a) = (1/1 + 1/2) / 2 = 0.75
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["centrality"].as_f64().unwrap(),
                )
            })
            .collect();

        assert!(map["b"] > map["a"]);
        assert!(map["b"] > map["c"]);
    }

    #[test]
    fn harmonic_disconnected() {
        // a-b connected, c isolated. c still gets HC=0 naturally.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_node("c").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["centrality"].as_f64().unwrap(),
                )
            })
            .collect();

        assert!(map["c"].abs() < 1e-9);
        assert!(map["a"] > 0.0);
    }

    #[test]
    fn harmonic_complete() {
        // Fully connected 3-node graph: HC = 1.0 for all.
        let mut csr = CsrIndex::new();
        for (s, d) in &[
            ("a", "b"),
            ("b", "a"),
            ("b", "c"),
            ("c", "b"),
            ("a", "c"),
            ("c", "a"),
        ] {
            csr.add_edge(s, "L", d).unwrap();
        }
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        for row in &rows {
            let c = row["centrality"].as_f64().unwrap();
            assert!((c - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn harmonic_empty() {
        let csr = CsrIndex::new();
        assert!(run(&csr).is_empty());
    }

    #[test]
    fn harmonic_sort_is_total_nan_goes_last() {
        // Direct comparator test: NaN must sort after all finite values.
        let mut scores: Vec<(usize, f64)> = vec![(0, f64::NAN), (1, 0.9), (2, 0.5), (3, 0.1)];
        scores.sort_by(|a, b| cmp_desc_nan_last(a.1, b.1));
        // Largest finite first, NaN last.
        assert!(!scores[0].1.is_nan());
        assert!(!scores[1].1.is_nan());
        assert!(!scores[2].1.is_nan());
        assert!(scores[3].1.is_nan());
        // Descending order among finite values.
        assert!(scores[0].1 > scores[1].1);
        assert!(scores[1].1 > scores[2].1);
    }
}
