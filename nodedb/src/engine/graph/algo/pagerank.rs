// SPDX-License-Identifier: BUSL-1.1

//! PageRank — link analysis via power iteration on the CSR index.
//!
//! Algorithm: `PR(v) = (1 - d) / N + d * sum(PR(u) / out_degree(u))` for each
//! in-neighbor u. Iterates until L1 norm of rank delta < tolerance or
//! max_iterations reached. Dangling nodes (zero out-degree) redistribute
//! their rank uniformly across all nodes.
//!
//! SIMD-accelerated hot loops:
//! - `simd_fill_f64`: broadcast base rank into next_rank vector
//! - `simd_dangling_sum`: sum ranks of dangling nodes
//! - `simd_l1_norm_delta`: L1 convergence check
//!
//! Performance target: 633K vertices / 34M edges in < 10s for 20 iterations.

use super::params::AlgoParams;
use super::progress::ProgressReporter;
use super::result::AlgoResultBatch;
use super::simd;
use super::util::cmp_desc_nan_last;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run PageRank on the CSR index.
///
/// Returns an `AlgoResultBatch` with `(node_id, rank)` rows sorted by rank
/// descending.
pub fn run(csr: &CsrIndex, params: &AlgoParams) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::PageRank);
    }

    let damping = params.damping_factor();
    let max_iter = params.iterations(20);
    let tolerance = params.convergence_tolerance();

    let mut reporter =
        ProgressReporter::new(GraphAlgorithm::PageRank, max_iter, Some(tolerance), n);

    // Personalization distribution for Personalized PageRank (PPR). `None`
    // recovers standard PageRank with a uniform 1/n teleport. When present,
    // teleport mass and dangling-node mass both redistribute according to the
    // seed distribution instead of uniformly, biasing rank toward seed nodes.
    let personalization = build_personalization(csr, params, n);

    // Initialize ranks: from the seed distribution for PPR (already sums to
    // 1.0), uniformly otherwise.
    let mut rank = match &personalization {
        Some(p) => p.clone(),
        None => vec![1.0 / n as f64; n],
    };
    let mut next_rank = vec![0.0f64; n];

    // Precompute out-degrees and dangling mask for SIMD dangling sum.
    let out_degrees: Vec<usize> = (0..n).map(|i| csr.out_degree_raw(i as u32)).collect();
    let is_dangling: Vec<bool> = out_degrees.iter().map(|&d| d == 0).collect();

    for iter in 1..=max_iter {
        // ── SIMD: dangling node rank sum ──
        let dangling_sum = simd::simd_dangling_sum(&rank, &is_dangling);

        // Total mass to redistribute per the teleport/seed distribution:
        // the (1 - damping) teleport budget plus the damped dangling mass.
        let redistributed = (1.0 - damping) + damping * dangling_sum;

        match &personalization {
            // ── SIMD: broadcast fill next_rank with the uniform base rank ──
            None => simd::simd_fill_f64(&mut next_rank, redistributed / n as f64),
            // PPR: each node's base rank is its seed share of the redistributed
            // mass. Per-node base, so the SIMD broadcast fill does not apply.
            Some(p) => {
                for (slot, &seed) in next_rank.iter_mut().zip(p.iter()) {
                    *slot = redistributed * seed;
                }
            }
        }

        // ── Scatter: distribute rank contributions via outbound edges ──
        // This is inherently scatter (random write) — not SIMD-able per se,
        // but the fill + dangling_sum above are the dominant SIMD wins.
        for u in 0..n {
            let deg = out_degrees[u];
            if deg == 0 {
                continue;
            }
            let contrib = damping * rank[u] / deg as f64;
            for (_lid, dst) in csr.iter_out_edges_raw(u as u32) {
                next_rank[dst as usize] += contrib;
            }
        }

        // ── SIMD: L1 norm convergence check ──
        let delta = simd::simd_l1_norm_delta(&rank, &next_rank);

        // Swap rank vectors (avoids allocation).
        std::mem::swap(&mut rank, &mut next_rank);

        reporter.report_iteration(iter, Some(delta));

        if delta < tolerance {
            break;
        }
    }

    reporter.finish();

    // Build result batch sorted by rank descending.
    let mut indexed: Vec<(usize, f64)> = rank.into_iter().enumerate().collect();
    indexed.sort_by(|a, b| cmp_desc_nan_last(a.1, b.1));

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::PageRank);
    for (node_id, r) in indexed {
        batch.push_node_f64(csr.node_name_raw(node_id as u32).to_string(), r);
    }
    batch
}

/// Build the normalized per-node seed distribution for Personalized PageRank.
///
/// Returns `None` (→ standard uniform PageRank) when no personalization vector
/// is supplied, or when none of its seed nodes exist in the graph / all seed
/// weights are non-positive — falling back to uniform rather than emitting an
/// all-zero ranking. Negative weights are clamped to 0.0. The returned vector
/// is indexed by CSR node ordinal and sums to 1.0.
fn build_personalization(csr: &CsrIndex, params: &AlgoParams, n: usize) -> Option<Vec<f64>> {
    let seeds = params.personalization_vector()?;
    let mut p = vec![0.0f64; n];
    let mut sum = 0.0;
    for (i, slot) in p.iter_mut().enumerate() {
        if let Some(&w) = seeds.get(csr.node_name_raw(i as u32)) {
            let w = w.max(0.0);
            *slot = w;
            sum += w;
        }
    }
    if sum <= 0.0 {
        return None;
    }
    for v in &mut p {
        *v /= sum;
    }
    Some(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triangle_csr() -> CsrIndex {
        // a -> b -> c -> a (cycle)
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        csr.compact().expect("no governor, cannot fail");
        csr
    }

    #[test]
    fn pagerank_uniform_cycle() {
        let csr = triangle_csr();
        let params = AlgoParams::default();
        let batch = run(&csr, &params);

        // Symmetric cycle → all ranks equal ≈ 1/3.
        assert_eq!(batch.len(), 3);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        for row in &rows {
            let rank = row["rank"].as_f64().unwrap();
            assert!((rank - 1.0 / 3.0).abs() < 1e-6, "rank {rank} != 1/3");
        }
    }

    #[test]
    fn pagerank_star_topology() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("a", "L", "c").unwrap();
        csr.add_edge("a", "L", "d").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let params = AlgoParams {
            max_iterations: Some(50),
            ..Default::default()
        };
        let batch = run(&csr, &params);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let ranks: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| (r["node_id"].as_str().unwrap(), r["rank"].as_f64().unwrap()))
            .collect();

        assert!(
            ranks["b"] > ranks["a"],
            "b={} should > a={}",
            ranks["b"],
            ranks["a"]
        );
    }

    #[test]
    fn pagerank_empty_graph() {
        let csr = CsrIndex::new();
        let batch = run(&csr, &AlgoParams::default());
        assert!(batch.is_empty());
    }

    #[test]
    fn pagerank_single_node() {
        let mut csr = CsrIndex::new();
        csr.add_node("lonely").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr, &AlgoParams::default());
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn pagerank_dangling_nodes() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_node("c").unwrap(); // dangling
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr, &AlgoParams::default());
        assert_eq!(batch.len(), 3);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let total: f64 = rows.iter().map(|r| r["rank"].as_f64().unwrap()).sum();
        assert!((total - 1.0).abs() < 1e-6, "total rank {total} != 1.0");
    }

    #[test]
    fn pagerank_converges() {
        let csr = triangle_csr();
        let params = AlgoParams {
            tolerance: Some(1e-10),
            max_iterations: Some(100),
            ..Default::default()
        };
        let batch = run(&csr, &params);
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn personalized_pagerank_biases_toward_seed() {
        use std::collections::HashMap;

        // Symmetric cycle: standard PageRank gives all three nodes ~1/3.
        // Seeding the teleport on "a" must lift "a" above its peers.
        let csr = triangle_csr();
        let mut seed = HashMap::new();
        seed.insert("a".to_string(), 1.0);
        let params = AlgoParams {
            max_iterations: Some(100),
            tolerance: Some(1e-10),
            personalization_vector: Some(seed),
            ..Default::default()
        };
        let batch = run(&csr, &params);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let ranks: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| (r["node_id"].as_str().unwrap(), r["rank"].as_f64().unwrap()))
            .collect();

        assert!(
            ranks["a"] > ranks["b"] && ranks["a"] > ranks["c"],
            "seed node a={} should outrank b={} and c={}",
            ranks["a"],
            ranks["b"],
            ranks["c"]
        );
        let total: f64 = ranks.values().sum();
        assert!(
            (total - 1.0).abs() < 1e-6,
            "ranks must sum to 1.0, got {total}"
        );
    }

    #[test]
    fn personalized_pagerank_unknown_seed_falls_back_to_uniform() {
        use std::collections::HashMap;

        // A seed naming only nonexistent nodes must not zero out the result —
        // it falls back to standard uniform PageRank.
        let csr = triangle_csr();
        let mut seed = HashMap::new();
        seed.insert("ghost".to_string(), 1.0);
        let params = AlgoParams {
            personalization_vector: Some(seed),
            ..Default::default()
        };
        let batch = run(&csr, &params);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        for row in &rows {
            let rank = row["rank"].as_f64().unwrap();
            assert!((rank - 1.0 / 3.0).abs() < 1e-6, "rank {rank} != 1/3");
        }
    }

    #[test]
    fn pagerank_to_record_batch() {
        let csr = triangle_csr();
        let batch = run(&csr, &AlgoParams::default());
        let rb = batch.to_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 2);
        assert_eq!(rb.schema().field(0).name(), "node_id");
        assert_eq!(rb.schema().field(1).name(), "rank");
    }

    #[test]
    fn pagerank_sort_is_total_nan_goes_last() {
        // Direct comparator test: NaN must sort deterministically after all finite values.
        let mut indexed: Vec<(usize, f64)> = vec![(0, f64::NAN), (1, 0.5), (2, 0.3), (3, 0.2)];
        indexed.sort_by(|a, b| cmp_desc_nan_last(a.1, b.1));
        assert!(!indexed[0].1.is_nan());
        assert!(!indexed[1].1.is_nan());
        assert!(!indexed[2].1.is_nan());
        assert!(indexed[3].1.is_nan());
        assert!(indexed[0].1 > indexed[1].1);
        assert!(indexed[1].1 > indexed[2].1);
    }
}
