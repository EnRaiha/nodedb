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
    let ranks = run_raw_with_progress(csr, params, true);
    let mut indexed: Vec<(usize, f64)> = ranks.into_iter().enumerate().collect();
    indexed.sort_by(|a, b| cmp_desc_nan_last(a.1, b.1));

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::PageRank);
    for (node_id, rank) in indexed {
        batch.push_node_f64(csr.node_name_raw(node_id as u32).to_string(), rank);
    }
    batch
}

/// Compute dense PageRank values in CSR node order without telemetry or
/// presentation work. Callers own sorting and node-name conversion.
pub fn run_raw(csr: &CsrIndex, params: &AlgoParams) -> Vec<f64> {
    run_raw_with_progress(csr, params, false)
}

fn run_raw_with_progress(csr: &CsrIndex, params: &AlgoParams, report_progress: bool) -> Vec<f64> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }

    let damping = params.damping_factor();
    let max_iter = params.iterations(20);
    let tolerance = params.convergence_tolerance();
    let mut reporter = report_progress
        .then(|| ProgressReporter::new(GraphAlgorithm::PageRank, max_iter, Some(tolerance), n));

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

    let both = params
        .direction
        .as_deref()
        .is_some_and(|direction| direction.eq_ignore_ascii_case("both"));

    // A compacted CSR can be traversed directly as immutable slices. Pull
    // workers own disjoint destination ranges and share no mutable graph state.
    // Live buffered graphs fall back to the iterator-based serial scatter path.
    let dense_in = csr.compacted_in_adjacency_raw();
    let dense_out = csr.compacted_out_adjacency_raw();
    let pull_available = dense_in.is_some() && (!both || dense_out.is_some());
    let degrees: Vec<usize> = (0..n as u32)
        .map(|node| csr.out_degree_raw(node) + if both { csr.in_degree_raw(node) } else { 0 })
        .collect();
    let is_dangling: Vec<bool> = degrees.iter().map(|&degree| degree == 0).collect();
    let mut contributions = vec![0.0f64; n];

    for iter in 1..=max_iter {
        // ── SIMD: dangling node rank sum ──
        let dangling_sum = simd::simd_dangling_sum(&rank, &is_dangling);

        // Total mass to redistribute per the teleport/seed distribution:
        // the (1 - damping) teleport budget plus the damped dangling mass.
        let redistributed = (1.0 - damping) + damping * dangling_sum;

        for (node, contribution) in contributions.iter_mut().enumerate() {
            *contribution = if degrees[node] == 0 {
                0.0
            } else {
                damping * rank[node] / degrees[node] as f64
            };
        }
        if pull_available {
            pull_rank_iteration(
                dense_in.expect("pull availability checked"),
                both.then_some(dense_out.expect("BOTH pull requires outbound CSR")),
                &contributions,
                &mut next_rank,
                redistributed,
                personalization.as_deref(),
            );
        } else {
            match &personalization {
                None => simd::simd_fill_f64(&mut next_rank, redistributed / n as f64),
                Some(seeds) => {
                    for (slot, seed) in next_rank.iter_mut().zip(seeds) {
                        *slot = redistributed * seed;
                    }
                }
            }
            for node in 0..n {
                if degrees[node] == 0 {
                    continue;
                }
                let contribution = contributions[node];
                for (_, destination) in csr.iter_out_edges_raw(node as u32) {
                    next_rank[destination as usize] += contribution;
                }
                if both {
                    for (_, source) in csr.iter_in_edges_raw(node as u32) {
                        next_rank[source as usize] += contribution;
                    }
                }
            }
        }

        // ── SIMD: L1 norm convergence check ──
        let delta = simd::simd_l1_norm_delta(&rank, &next_rank);

        // Swap rank vectors (avoids allocation).
        std::mem::swap(&mut rank, &mut next_rank);

        if let Some(reporter) = reporter.as_mut() {
            reporter.report_iteration(iter, Some(delta));
        }

        if delta < tolerance {
            break;
        }
    }

    if let Some(reporter) = reporter {
        reporter.finish();
    }
    rank
}

#[cfg(not(target_arch = "wasm32"))]
static ACTIVE_PULL_WORKERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(not(target_arch = "wasm32"))]
struct PullWorkerPermits(usize);

#[cfg(not(target_arch = "wasm32"))]
impl Drop for PullWorkerPermits {
    fn drop(&mut self) {
        ACTIVE_PULL_WORKERS.fetch_sub(self.0, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn reserve_pull_workers(requested: usize) -> PullWorkerPermits {
    const MAX_PROCESS_WORKERS: usize = 31;
    let mut active = ACTIVE_PULL_WORKERS.load(std::sync::atomic::Ordering::Acquire);
    loop {
        let granted = requested.min(MAX_PROCESS_WORKERS.saturating_sub(active));
        match ACTIVE_PULL_WORKERS.compare_exchange_weak(
            active,
            active + granted,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => return PullWorkerPermits(granted),
            Err(observed) => active = observed,
        }
    }
}

fn pull_rank_iteration(
    inbound: (&[u32], &[u32]),
    outbound: Option<(&[u32], &[u32])>,
    contributions: &[f64],
    next_rank: &mut [f64],
    redistributed: f64,
    personalization: Option<&[f64]>,
) {
    #[cfg(target_arch = "wasm32")]
    {
        pull_rank_range(
            inbound,
            outbound,
            contributions,
            next_rank,
            0,
            redistributed,
            personalization,
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let desired_workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(32)
            .min(next_rank.len().max(1));
        let permits = reserve_pull_workers(desired_workers.saturating_sub(1));
        let workers = permits.0;
        if workers <= 1 {
            pull_rank_range(
                inbound,
                outbound,
                contributions,
                next_rank,
                0,
                redistributed,
                personalization,
            );
            return;
        }
        let chunk_size = next_rank.len().div_ceil(workers);
        std::thread::scope(|scope| {
            for (chunk_index, chunk) in next_rank.chunks_mut(chunk_size).enumerate() {
                let start = chunk_index * chunk_size;
                scope.spawn(move || {
                    pull_rank_range(
                        inbound,
                        outbound,
                        contributions,
                        chunk,
                        start,
                        redistributed,
                        personalization,
                    );
                });
            }
        });
    }
}

fn pull_rank_range(
    inbound: (&[u32], &[u32]),
    outbound: Option<(&[u32], &[u32])>,
    contributions: &[f64],
    output: &mut [f64],
    start: usize,
    redistributed: f64,
    personalization: Option<&[f64]>,
) {
    let (in_offsets, in_targets) = inbound;
    for (offset, slot) in output.iter_mut().enumerate() {
        let node = start + offset;
        let base = personalization.map_or(redistributed / contributions.len() as f64, |seeds| {
            redistributed * seeds[node]
        });
        let mut rank = base;
        for &neighbor in &in_targets[in_offsets[node] as usize..in_offsets[node + 1] as usize] {
            rank += contributions[neighbor as usize];
        }
        if let Some((out_offsets, out_targets)) = outbound {
            for &neighbor in
                &out_targets[out_offsets[node] as usize..out_offsets[node + 1] as usize]
            {
                rank += contributions[neighbor as usize];
            }
        }
        *slot = rank;
    }
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
    fn raw_ranks_match_adapter_values() {
        let csr = triangle_csr();
        let raw = run_raw(&csr, &AlgoParams::default());
        let rows: Vec<serde_json::Value> =
            serde_json::from_slice(&run(&csr, &AlgoParams::default()).to_json().unwrap()).unwrap();
        for (node, rank) in raw.into_iter().enumerate() {
            let row = rows
                .iter()
                .find(|row| row["node_id"].as_str() == Some(csr.node_name_raw(node as u32)))
                .unwrap();
            assert_eq!(row["rank"].as_f64(), Some(rank));
        }
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
    fn pagerank_both_treats_a_single_stored_edge_as_undirected() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.compact().expect("no governor, cannot fail");
        let batch = run(
            &csr,
            &AlgoParams {
                direction: Some("both".to_string()),
                max_iterations: Some(10),
                tolerance: Some(f64::MIN_POSITIVE),
                ..Default::default()
            },
        );
        let rows: Vec<serde_json::Value> =
            serde_json::from_slice(&batch.to_json().unwrap()).unwrap();
        assert_eq!(rows.len(), 2);
        assert!((rows[0]["rank"].as_f64().unwrap() - 0.5).abs() < 1e-12);
        assert!((rows[1]["rank"].as_f64().unwrap() - 0.5).abs() < 1e-12);
    }

    #[test]
    fn compacted_pull_matches_buffered_scatter_for_both_personalized() {
        use std::collections::HashMap;

        fn graph(compact: bool) -> CsrIndex {
            let mut csr = CsrIndex::new();
            csr.add_edge("a", "L", "b").unwrap();
            csr.add_edge("a", "L", "c").unwrap();
            csr.add_edge("c", "L", "b").unwrap();
            csr.add_node("d").unwrap();
            if compact {
                csr.compact().unwrap();
            }
            csr
        }

        let mut seeds = HashMap::new();
        seeds.insert("a".to_string(), 2.0);
        seeds.insert("d".to_string(), 1.0);
        let params = AlgoParams {
            direction: Some("both".to_string()),
            max_iterations: Some(20),
            tolerance: Some(f64::MIN_POSITIVE),
            personalization_vector: Some(seeds),
            ..Default::default()
        };
        let ranks = |csr: &CsrIndex| {
            let rows: Vec<serde_json::Value> =
                serde_json::from_slice(&run(csr, &params).to_json().unwrap()).unwrap();
            rows.into_iter()
                .map(|row| {
                    (
                        row["node_id"].as_str().unwrap().to_string(),
                        row["rank"].as_f64().unwrap(),
                    )
                })
                .collect::<HashMap<_, _>>()
        };

        let pull = ranks(&graph(true));
        let scatter = ranks(&graph(false));
        assert_eq!(
            pull.keys().collect::<std::collections::BTreeSet<_>>(),
            scatter.keys().collect()
        );
        for (node, pull_rank) in pull {
            let scatter_rank = scatter[&node];
            assert!(
                (pull_rank - scatter_rank).abs() < 1e-12,
                "{node}: pull={pull_rank}, scatter={scatter_rank}"
            );
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
