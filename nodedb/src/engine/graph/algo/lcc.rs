// SPDX-License-Identifier: BUSL-1.1

//! Local Clustering Coefficient — per-node triangle density on the CSR index.
//!
//! For each node v with degree k:
//! `LCC(v) = 2 * triangles(v) / (k * (k - 1))`
//!
//! where triangles(v) is the number of edges between neighbors of v.
//! Nodes with degree < 2 get LCC = 0.0.
//!
//! Algorithm: for each node v, collect the neighbor set N(v). Sort it.
//! For each pair (u, w) in N(v), check if edge (u, w) exists via sorted
//! neighbor list intersection (binary search on sorted adjacency).
//!
//! Optimization: for high-degree nodes (> 1000 neighbors), approximate
//! via random sampling of neighbor pairs.
//!
//! Performance target: 633K vertices / 34M edges in < 30s.

use std::collections::HashSet;

use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

#[cfg(not(target_arch = "wasm32"))]
static ACTIVE_LCC_WORKERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(not(target_arch = "wasm32"))]
struct LccWorkerPermits(usize);

#[cfg(not(target_arch = "wasm32"))]
impl LccWorkerPermits {
    fn reserve(requested: usize) -> Self {
        use std::sync::atomic::Ordering;

        const MAX_PROCESS_WORKERS: usize = 32;
        let mut active = ACTIVE_LCC_WORKERS.load(Ordering::Acquire);
        loop {
            let granted = requested.min(MAX_PROCESS_WORKERS.saturating_sub(active));
            match ACTIVE_LCC_WORKERS.compare_exchange_weak(
                active,
                active + granted,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Self(granted),
                Err(updated) => active = updated,
            }
        }
    }

    fn workers(&self) -> usize {
        self.0
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for LccWorkerPermits {
    fn drop(&mut self) {
        ACTIVE_LCC_WORKERS.fetch_sub(self.0, std::sync::atomic::Ordering::Release);
    }
}

/// Default maximum neighbor count before switching to sampling approximation.
/// Sourced from `GraphTuning::lcc_high_degree_threshold` at runtime.
pub const DEFAULT_HIGH_DEGREE_THRESHOLD: usize = 2_000;

/// Default number of neighbor pairs to sample for high-degree approximation.
/// Sourced from `GraphTuning::lcc_sample_pairs` at runtime.
pub const DEFAULT_SAMPLE_PAIRS: usize = 10_000;

/// Run Local Clustering Coefficient on the CSR index.
///
/// Treats graph as undirected for neighbor collection (both out + in neighbors).
/// Returns `(node_id, coefficient)` rows.
///
/// `high_degree_threshold` and `sample_pairs` are sourced from
/// `GraphTuning::lcc_high_degree_threshold` and `GraphTuning::lcc_sample_pairs`.
pub fn run(csr: &CsrIndex, high_degree_threshold: usize, sample_pairs: usize) -> AlgoResultBatch {
    let coefficients = run_raw(csr, high_degree_threshold, sample_pairs);
    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Lcc);
    for (node, coefficient) in coefficients.into_iter().enumerate() {
        batch.push_node_f64(csr.node_name_raw(node as u32).to_string(), coefficient);
    }
    batch
}

/// Compute dense LCC coefficients in CSR node order. Neighbor collection,
/// self-loop removal, sorting, and deduplication are part of this primitive.
pub fn run_raw(csr: &CsrIndex, high_degree_threshold: usize, sample_pairs: usize) -> Vec<f64> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }

    let adjacency: Vec<Vec<u32>> =
        if let (Some((out_offsets, out_targets)), Some((in_offsets, in_targets))) = (
            csr.compacted_out_adjacency_raw(),
            csr.compacted_in_adjacency_raw(),
        ) {
            (0..n)
                .map(|node| {
                    let mut neighbors = Vec::with_capacity(
                        out_offsets[node + 1] as usize - out_offsets[node] as usize
                            + in_offsets[node + 1] as usize
                            - in_offsets[node] as usize,
                    );
                    neighbors.extend_from_slice(
                        &out_targets[out_offsets[node] as usize..out_offsets[node + 1] as usize],
                    );
                    neighbors.extend_from_slice(
                        &in_targets[in_offsets[node] as usize..in_offsets[node + 1] as usize],
                    );
                    neighbors.retain(|neighbor| *neighbor != node as u32);
                    neighbors.sort_unstable();
                    neighbors.dedup();
                    neighbors
                })
                .collect()
        } else {
            (0..n as u32)
                .map(|node| {
                    let mut neighbors: Vec<u32> = csr
                        .iter_out_edges_raw(node)
                        .map(|(_, neighbor)| neighbor)
                        .chain(csr.iter_in_edges_raw(node).map(|(_, neighbor)| neighbor))
                        .filter(|neighbor| *neighbor != node)
                        .collect();
                    neighbors.sort_unstable();
                    neighbors.dedup();
                    neighbors
                })
                .collect()
        };
    let exact_triangles = adjacency
        .iter()
        .all(|neighbors| neighbors.len() <= high_degree_threshold)
        .then(|| count_all_triangles_exact(&adjacency));
    let mut coefficients = Vec::with_capacity(n);
    for node in 0..n {
        let node_id = node as u32;
        let coeff = if let Some(triangles) = &exact_triangles {
            let degree = adjacency[node].len();
            if degree < 2 {
                0.0
            } else {
                2.0 * triangles[node] as f64 / (degree * (degree - 1)) as f64
            }
        } else {
            compute_lcc(
                csr,
                &adjacency,
                node_id,
                high_degree_threshold,
                sample_pairs,
            )
        };
        coefficients.push(coeff);
    }

    coefficients
}

/// Compute LCC for a single node.
fn compute_lcc(
    csr: &CsrIndex,
    adjacency: &[Vec<u32>],
    node: u32,
    high_degree_threshold: usize,
    sample_pairs: usize,
) -> f64 {
    let neighbors = &adjacency[node as usize];
    let k = neighbors.len();
    if k < 2 {
        return 0.0;
    }

    let possible_pairs = k * (k - 1) / 2;

    let triangles = if k > high_degree_threshold {
        // Approximate: sample random pairs.
        count_triangles_sampled(csr, neighbors, possible_pairs, sample_pairs)
    } else {
        count_triangles_exact(adjacency, neighbors)
    };

    // LCC = 2 * triangles / (k * (k-1))
    // Since we count each triangle once (unordered pair), and the denominator
    // is k*(k-1)/2 pairs, LCC = triangles / possible_pairs.
    triangles as f64 / possible_pairs as f64
}

fn count_all_triangles_exact(adjacency: &[Vec<u32>]) -> Vec<usize> {
    let degrees: Vec<usize> = adjacency.iter().map(Vec::len).collect();
    let oriented: Vec<Vec<u32>> = adjacency
        .iter()
        .enumerate()
        .map(|(node, neighbors)| {
            neighbors
                .iter()
                .copied()
                .filter(|neighbor| {
                    let neighbor = *neighbor as usize;
                    (degrees[node], node) < (degrees[neighbor], neighbor)
                })
                .collect()
        })
        .collect();
    count_oriented_triangles(&oriented)
}

fn count_oriented_triangles(oriented: &[Vec<u32>]) -> Vec<usize> {
    #[cfg(target_arch = "wasm32")]
    {
        return count_oriented_triangle_range(oriented, 0, oriented.len());
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::sync::atomic::{AtomicUsize, Ordering};

        const MAX_WORKERS: usize = 32;
        const COUNTER_BUDGET_BYTES: usize = 128 * 1024 * 1024;
        let bytes_per_counter = oriented
            .len()
            .checked_mul(std::mem::size_of::<usize>())
            .unwrap_or(usize::MAX)
            .max(1);
        let memory_bounded_workers = (COUNTER_BUDGET_BYTES / bytes_per_counter).max(1);
        let desired_workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MAX_WORKERS)
            .min(memory_bounded_workers)
            .min(oriented.len().max(1));
        let permits = LccWorkerPermits::reserve(desired_workers);
        let workers = permits.workers();
        if workers <= 1 {
            return count_oriented_triangle_range(oriented, 0, oriented.len());
        }

        const CHUNK_SIZE: usize = 64;
        let next = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..workers)
                .map(|_| {
                    let next = &next;
                    scope.spawn(move || {
                        let mut local = vec![0usize; oriented.len()];
                        loop {
                            let start = next.fetch_add(CHUNK_SIZE, Ordering::Relaxed);
                            if start >= oriented.len() {
                                break;
                            }
                            let end = (start + CHUNK_SIZE).min(oriented.len());
                            count_oriented_triangle_range_into(oriented, start, end, &mut local);
                        }
                        local
                    })
                })
                .collect();
            let mut totals = vec![0usize; oriented.len()];
            for handle in handles {
                let local = handle.join().expect("LCC worker panicked");
                for (total, count) in totals.iter_mut().zip(local) {
                    *total += count;
                }
            }
            totals
        })
    }
}

fn count_oriented_triangle_range(oriented: &[Vec<u32>], start: usize, end: usize) -> Vec<usize> {
    let mut triangles = vec![0usize; oriented.len()];
    count_oriented_triangle_range_into(oriented, start, end, &mut triangles);
    triangles
}

fn count_oriented_triangle_range_into(
    oriented: &[Vec<u32>],
    start: usize,
    end: usize,
    triangles: &mut [usize],
) {
    for left in start..end {
        let middle_neighbors = &oriented[left];
        for &middle in middle_neighbors {
            for &right in &oriented[middle as usize] {
                if middle_neighbors.binary_search(&right).is_ok() {
                    triangles[left] += 1;
                    triangles[middle as usize] += 1;
                    triangles[right as usize] += 1;
                }
            }
        }
    }
}

/// Count induced neighbor edges exactly using sorted contiguous adjacency.
fn count_triangles_exact(adjacency: &[Vec<u32>], neighbors: &[u32]) -> usize {
    let mut triangles = 0;
    for &left in neighbors {
        for &right in adjacency[left as usize]
            .iter()
            .skip_while(|right| **right <= left)
        {
            if neighbors.binary_search(&right).is_ok() {
                triangles += 1;
            }
        }
    }
    triangles
}

/// Count triangles via sampling for high-degree nodes (approximation).
fn count_triangles_sampled(
    csr: &CsrIndex,
    neighbors: &[u32],
    total_pairs: usize,
    sample_pairs: usize,
) -> usize {
    let neighbor_set: HashSet<u32> = neighbors.iter().copied().collect();
    let n = neighbors.len();
    let samples = sample_pairs.min(total_pairs);
    let mut found = 0usize;

    // Deterministic sampling via LCG.
    let mut state: u64 = (n as u64).wrapping_mul(0x517cc1b727220a95).wrapping_add(1);

    for _ in 0..samples {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let i = (state >> 33) as usize % n;
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let mut j = (state >> 33) as usize % n;
        if j == i {
            j = (j + 1) % n;
        }

        let u = neighbors[i];
        let w = neighbors[j];

        // Check if edge (u, w) exists.
        if has_undirected_edge(csr, u, w, &neighbor_set) {
            found += 1;
        }
    }

    // Extrapolate: found/samples ≈ triangles/total_pairs.
    (found as f64 / samples as f64 * total_pairs as f64).round() as usize
}

/// Check if an undirected edge exists between u and w.
fn has_undirected_edge(csr: &CsrIndex, u: u32, w: u32, _neighbor_set: &HashSet<u32>) -> bool {
    // Check outbound from u to w.
    for (_lid, dst) in csr.iter_out_edges_raw(u) {
        if dst == w {
            return true;
        }
    }
    // Check inbound to u from w (i.e., outbound from w to u).
    for (_lid, src) in csr.iter_in_edges_raw(u) {
        if src == w {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_coefficients_match_adapter_values() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        csr.compact().unwrap();
        let raw = run_raw(&csr, DEFAULT_HIGH_DEGREE_THRESHOLD, DEFAULT_SAMPLE_PAIRS);
        let rows: Vec<serde_json::Value> = serde_json::from_slice(
            &run(&csr, DEFAULT_HIGH_DEGREE_THRESHOLD, DEFAULT_SAMPLE_PAIRS)
                .to_json()
                .unwrap(),
        )
        .unwrap();
        for (node, coefficient) in raw.into_iter().enumerate() {
            let row = rows
                .iter()
                .find(|row| row["node_id"].as_str() == Some(csr.node_name_raw(node as u32)))
                .unwrap();
            assert_eq!(row["coefficient"].as_f64(), Some(coefficient));
        }
    }

    #[test]
    fn lcc_triangle() {
        // Fully connected triangle: each node has LCC = 1.0.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        csr.add_edge("b", "L", "a").unwrap();
        csr.add_edge("c", "L", "b").unwrap();
        csr.add_edge("a", "L", "c").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr, DEFAULT_HIGH_DEGREE_THRESHOLD, DEFAULT_SAMPLE_PAIRS);
        assert_eq!(batch.len(), 3);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        for row in &rows {
            let coeff = row["coefficient"].as_f64().unwrap();
            assert!(
                (coeff - 1.0).abs() < 1e-9,
                "node {} has LCC {coeff}, expected 1.0",
                row["node_id"]
            );
        }
    }

    #[test]
    fn lcc_star() {
        // Star topology: center a connects to b, c, d. No edges between b, c, d.
        // a has LCC = 0.0 (3 neighbors, 0 edges between them → 0/3).
        // b, c, d have LCC = 0.0 (degree 1 < 2).
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("a", "L", "c").unwrap();
        csr.add_edge("a", "L", "d").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr, DEFAULT_HIGH_DEGREE_THRESHOLD, DEFAULT_SAMPLE_PAIRS);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        for row in &rows {
            let coeff = row["coefficient"].as_f64().unwrap();
            assert!(
                coeff.abs() < 1e-9,
                "node {} has LCC {coeff}, expected 0.0",
                row["node_id"]
            );
        }
    }

    #[test]
    fn lcc_path() {
        // Path: a -> b -> c. Only b has 2 neighbors. No edge a-c → LCC(b) = 0.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr, DEFAULT_HIGH_DEGREE_THRESHOLD, DEFAULT_SAMPLE_PAIRS);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["coefficient"].as_f64().unwrap(),
                )
            })
            .collect();

        assert!(map["a"].abs() < 1e-9); // degree 1
        assert!(map["b"].abs() < 1e-9); // 2 neighbors (a,c) but no a-c edge
        assert!(map["c"].abs() < 1e-9); // degree 1
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn exhausted_worker_permits_fall_back_to_exact_triangle_counting() {
        let oriented = vec![vec![1, 2], vec![2], vec![]];
        let _held = LccWorkerPermits::reserve(32);
        assert_eq!(count_oriented_triangles(&oriented), vec![1, 1, 1]);
    }

    #[test]
    fn lcc_empty_graph() {
        let csr = CsrIndex::new();
        let batch = run(&csr, DEFAULT_HIGH_DEGREE_THRESHOLD, DEFAULT_SAMPLE_PAIRS);
        assert!(batch.is_empty());
    }

    #[test]
    fn lcc_single_node() {
        let mut csr = CsrIndex::new();
        csr.add_node("lonely").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr, DEFAULT_HIGH_DEGREE_THRESHOLD, DEFAULT_SAMPLE_PAIRS);
        assert_eq!(batch.len(), 1);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        assert!(rows[0]["coefficient"].as_f64().unwrap().abs() < 1e-9);
    }

    #[test]
    fn lcc_partial_connectivity() {
        // Diamond: a-b, a-c, b-d, c-d, b-c.
        // Node a: neighbors {b, c}. Edge b-c exists → 1 triangle / 1 pair = 1.0.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("a", "L", "c").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("b", "L", "d").unwrap();
        csr.add_edge("c", "L", "d").unwrap();
        csr.compact().expect("no governor, cannot fail");

        let batch = run(&csr, DEFAULT_HIGH_DEGREE_THRESHOLD, DEFAULT_SAMPLE_PAIRS);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["coefficient"].as_f64().unwrap(),
                )
            })
            .collect();

        // a has neighbors b,c. b-c edge exists → LCC = 1/1 = 1.0.
        assert!(
            (map["a"] - 1.0).abs() < 1e-9,
            "a LCC = {}, expected 1.0",
            map["a"]
        );
    }
}
