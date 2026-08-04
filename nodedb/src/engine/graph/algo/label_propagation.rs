// SPDX-License-Identifier: BUSL-1.1

//! Community Detection — Label Propagation Algorithm (LPA) on the CSR index.
//!
//! Each node adopts the most frequent label among its neighbors using
//! synchronous iterations. Ties are broken by the smallest original numeric
//! node identifier when available, then by node name for determinism.
//!
//! Performance target: 633K vertices / 34M edges in < 15s for 10 iterations.

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

    // Initialize: each node is its own community. `label_priority` hoists the
    // original numeric-ID tie order out of the hot iteration loop.
    let mut labels: Vec<u32> = (0..n as u32).collect();
    let label_priority = label_priorities(csr, n);
    let mut next_labels = labels.clone();
    let mut neighbor_labels = Vec::new();
    let dense = csr
        .compacted_out_adjacency_raw()
        .zip(csr.compacted_in_adjacency_raw());

    for iter in 1..=max_iter {
        next_labels.copy_from_slice(&labels);
        let mut changed = 0usize;

        for (node, next_label) in next_labels.iter_mut().enumerate() {
            neighbor_labels.clear();
            if let Some(((out_offsets, out_targets), (in_offsets, in_targets))) = dense {
                neighbor_labels.extend(
                    out_targets[out_offsets[node] as usize..out_offsets[node + 1] as usize]
                        .iter()
                        .map(|neighbor| labels[*neighbor as usize]),
                );
                neighbor_labels.extend(
                    in_targets[in_offsets[node] as usize..in_offsets[node + 1] as usize]
                        .iter()
                        .map(|neighbor| labels[*neighbor as usize]),
                );
            } else {
                let node_id = node as u32;
                neighbor_labels.extend(
                    csr.iter_out_edges_raw(node_id)
                        .map(|(_, neighbor)| labels[neighbor as usize]),
                );
                neighbor_labels.extend(
                    csr.iter_in_edges_raw(node_id)
                        .map(|(_, neighbor)| labels[neighbor as usize]),
                );
            }

            let Some(best_label) = most_frequent_label(&mut neighbor_labels, &label_priority)
            else {
                continue;
            };
            if labels[node] != best_label {
                *next_label = best_label;
                changed += 1;
            }
        }

        std::mem::swap(&mut labels, &mut next_labels);
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

fn label_priorities(csr: &CsrIndex, n: usize) -> Vec<u32> {
    let mut ordered: Vec<u32> = (0..n as u32).collect();
    ordered.sort_unstable_by(|left, right| compare_labels(csr, *left, *right));
    let mut priority = vec![0u32; n];
    for (rank, label) in ordered.into_iter().enumerate() {
        priority[label as usize] = rank as u32;
    }
    priority
}

fn most_frequent_label(labels: &mut [u32], priority: &[u32]) -> Option<u32> {
    labels.sort_unstable_by_key(|label| priority[*label as usize]);
    let (&first, rest) = labels.split_first()?;
    let mut best_label = first;
    let mut best_count = 1usize;
    let mut current_label = first;
    let mut current_count = 1usize;
    for &label in rest {
        if label == current_label {
            current_count += 1;
        } else {
            if current_count > best_count {
                best_label = current_label;
                best_count = current_count;
            }
            current_label = label;
            current_count = 1;
        }
    }
    if current_count > best_count {
        best_label = current_label;
    }
    Some(best_label)
}

fn compare_labels(csr: &CsrIndex, left: u32, right: u32) -> std::cmp::Ordering {
    let left_name = csr.node_name_raw(left);
    let right_name = csr.node_name_raw(right);
    match (left_name.parse::<u64>(), right_name.parse::<u64>()) {
        (Ok(left), Ok(right)) => left.cmp(&right).then_with(|| left_name.cmp(right_name)),
        _ => left_name.cmp(right_name),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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
        let rows: Vec<serde_json::Value> =
            serde_json::from_slice(&batch.to_json().unwrap()).unwrap();
        let center = rows.iter().find(|row| row["node_id"] == "10").unwrap();
        assert_eq!(center["community_id"].as_i64(), Some(6));
    }

    #[test]
    fn equivalent_numeric_labels_use_lexical_secondary_order() {
        for names in [["6", "06"], ["06", "6"]] {
            let mut csr = CsrIndex::new();
            csr.add_node(names[0]).unwrap();
            csr.add_node(names[1]).unwrap();
            let priority = label_priorities(&csr, 2);
            let mut labels = [
                csr.node_id_raw("6").unwrap(),
                csr.node_id_raw("06").unwrap(),
            ];
            let best = most_frequent_label(&mut labels, &priority).unwrap();
            assert_eq!(csr.node_name_raw(best), "06");
        }
    }

    #[test]
    fn most_frequent_label_uses_precomputed_priority_for_ties() {
        let priority = [2, 0, 1, 3];
        let mut labels = [0, 2, 1, 0, 2, 1];
        assert_eq!(most_frequent_label(&mut labels, &priority), Some(1));
    }

    #[test]
    fn compacted_and_buffered_graphs_produce_the_same_labels() {
        fn graph(compact: bool) -> CsrIndex {
            let mut csr = CsrIndex::new();
            csr.add_edge("10", "L", "6").unwrap();
            csr.add_edge("10", "L", "41").unwrap();
            csr.add_edge("41", "L", "6").unwrap();
            csr.add_node("99").unwrap();
            if compact {
                csr.compact().unwrap();
            }
            csr
        }
        let params = AlgoParams {
            max_iterations: Some(4),
            ..Default::default()
        };
        assert_eq!(
            run(&graph(true), &params).to_json().unwrap(),
            run(&graph(false), &params).to_json().unwrap()
        );
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
