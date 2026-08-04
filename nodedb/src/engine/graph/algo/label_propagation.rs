// SPDX-License-Identifier: BUSL-1.1

//! Community Detection — Label Propagation Algorithm (LPA) on the CSR index.
//!
//! Each node adopts the most frequent label among its neighbors using
//! synchronous iterations. Ties are broken by the smallest original numeric
//! node identifier when available, then by node name for determinism.
//!
//! Performance target: 633K vertices / 34M edges in < 15s for 10 iterations.

use std::collections::HashMap;

use super::params::AlgoParams;
use super::progress::ProgressReporter;
use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run Label Propagation on the CSR index.
///
/// Returns an `AlgoResultBatch` with `(node_id, community_id)` rows.
/// Community IDs are dense node IDs — the label that "won" for each node.
pub fn run(csr: &CsrIndex, params: &AlgoParams) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::LabelPropagation);
    }

    let max_iter = params.iterations(10);
    let mut reporter = ProgressReporter::new(GraphAlgorithm::LabelPropagation, max_iter, None, n);

    // Initialize: each node is its own community.
    let mut labels: Vec<u32> = (0..n as u32).collect();

    for iter in 1..=max_iter {
        let mut next_labels = labels.clone();
        let mut changed = 0usize;

        for (node, next_label) in next_labels.iter_mut().enumerate() {
            let node_id = node as u32;
            let mut label_counts: HashMap<u32, u32> = HashMap::new();

            for (_lid, neighbor) in csr.iter_out_edges_raw(node_id) {
                *label_counts.entry(labels[neighbor as usize]).or_insert(0) += 1;
            }
            for (_lid, neighbor) in csr.iter_in_edges_raw(node_id) {
                *label_counts.entry(labels[neighbor as usize]).or_insert(0) += 1;
            }

            let Some(&max_count) = label_counts.values().max() else {
                continue;
            };
            let best_label = label_counts
                .into_iter()
                .filter(|(_, count)| *count == max_count)
                .map(|(label, _)| label)
                .min_by(|left, right| compare_labels(csr, *left, *right))
                .expect("at least one label has the maximum count");

            if labels[node] != best_label {
                *next_label = best_label;
                changed += 1;
            }
        }

        labels = next_labels;
        reporter.report_iteration(iter, Some(changed as f64));

        if changed == 0 {
            break;
        }
    }

    reporter.finish();

    // Build result.
    let mut batch = AlgoResultBatch::new(GraphAlgorithm::LabelPropagation);
    for (node, &label) in labels.iter().enumerate() {
        let label_name = csr.node_name_raw(label);
        let community = label_name.parse::<i64>().unwrap_or(label as i64);
        batch.push_node_i64(csr.node_name_raw(node as u32).to_string(), community);
    }
    batch
}

fn compare_labels(csr: &CsrIndex, left: u32, right: u32) -> std::cmp::Ordering {
    let left_name = csr.node_name_raw(left);
    let right_name = csr.node_name_raw(right);
    match (left_name.parse::<u64>(), right_name.parse::<u64>()) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left_name.cmp(right_name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_prop_triangle() {
        // Fully connected triangle — all should be same community.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        csr.add_edge("b", "L", "a").unwrap();
        csr.add_edge("c", "L", "b").unwrap();
        csr.add_edge("a", "L", "c").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr, &AlgoParams::default());
        assert_eq!(batch.len(), 3);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let communities: Vec<i64> = rows
            .iter()
            .map(|r| r["community_id"].as_i64().unwrap())
            .collect();

        assert_eq!(communities[0], communities[1]);
        assert_eq!(communities[1], communities[2]);
    }

    #[test]
    fn label_prop_breaks_ties_by_original_numeric_vertex_id() {
        let mut csr = CsrIndex::new();
        csr.add_edge("10", "L", "6").unwrap();
        csr.add_edge("10", "L", "41").unwrap();
        csr.compact().expect("no governor, cannot fail");
        let batch = run(
            &csr,
            &AlgoParams {
                max_iterations: Some(1),
                ..Default::default()
            },
        );
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&batch.to_json().unwrap()).unwrap();
        let center = rows.iter().find(|row| row["node_id"] == "10").unwrap();
        assert_eq!(center["community_id"].as_i64(), Some(6));
    }

    #[test]
    fn label_prop_two_communities() {
        // Two cliques connected by a single bridge.
        // Clique 1: a-b-c (fully connected)
        // Clique 2: d-e-f (fully connected)
        // Bridge: c-d
        let mut csr = CsrIndex::new();
        for (s, d) in &[
            ("a", "b"),
            ("b", "a"),
            ("a", "c"),
            ("c", "a"),
            ("b", "c"),
            ("c", "b"),
            ("d", "e"),
            ("e", "d"),
            ("d", "f"),
            ("f", "d"),
            ("e", "f"),
            ("f", "e"),
            ("c", "d"),
            ("d", "c"),
        ] {
            csr.add_edge(s, "L", d).unwrap();
        }
        csr.compact().expect("no governor, cannot fail");

        let batch = run(
            &csr,
            &AlgoParams {
                max_iterations: Some(20),
                ..Default::default()
            },
        );
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: HashMap<&str, i64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["community_id"].as_i64().unwrap(),
                )
            })
            .collect();

        // Within each clique, all should have the same label.
        assert_eq!(map["a"], map["b"]);
        assert_eq!(map["a"], map["c"]);
        assert_eq!(map["d"], map["e"]);
        assert_eq!(map["d"], map["f"]);
    }

    #[test]
    fn label_prop_isolated_node() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_node("isolated").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr, &AlgoParams::default());
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn label_prop_empty_graph() {
        let csr = CsrIndex::new();
        let batch = run(&csr, &AlgoParams::default());
        assert!(batch.is_empty());
    }

    #[test]
    fn label_prop_deterministic() {
        // Same graph, same params → same result.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let params = AlgoParams::default();
        let r1 = run(&csr, &params).to_json().unwrap();
        let r2 = run(&csr, &params).to_json().unwrap();
        assert_eq!(r1, r2);
    }
}
