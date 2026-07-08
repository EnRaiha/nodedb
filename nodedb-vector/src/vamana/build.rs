// SPDX-License-Identifier: Apache-2.0

//! Two-pass Vamana graph builder (randomized → α-pruned).
//!
//! Pass 1: For each node, assign R random neighbors, then refine via
//!         greedy beam-search to find better candidates.
//! Pass 2: For each node, collect candidates via beam-search, apply
//!         `prune::alpha_prune`, and set the final adjacency list.
//!
//! The medoid is computed as the node with minimum sum of distances to a
//! random sample of up to 1 000 other nodes and is set as the entry point.
//!
//! Reference: Jayaram Subramanya et al., "DiskANN: Fast Accurate Billion-point
//! Nearest Neighbor Search on a Single Node", NeurIPS 2019.

use nodedb_codec::vector_quant::codec::VectorCodec;

use crate::vamana::graph::VamanaGraph;
use crate::vamana::prune::alpha_prune;

// ------------------------------------------------------------------
// Public entry point
// ------------------------------------------------------------------

/// Build a Vamana graph from a set of pre-encoded vectors.
///
/// # Arguments
///
/// * `vectors` — full-precision FP32 vectors, one per node.
/// * `ids` — external IDs corresponding to each vector (same order).
/// * `codec` — quantization codec; used only for `fast_symmetric_distance`
///   during beam-search candidate expansion.
/// * `quantized` — pre-encoded quantized vectors (caller encodes once).
/// * `r` — maximum out-degree per node (typical: 64).
/// * `alpha` — pruning factor (typical: 1.2; must be > 1).
/// * `l_build` — beam width during construction (typical: 100).
///
/// # Panics
///
/// Panics if `vectors`, `ids`, and `quantized` do not all have the same
/// length, or if `vectors` is empty.
pub fn build_vamana<C: VectorCodec>(
    vectors: &[Vec<f32>],
    ids: &[u64],
    codec: &C,
    quantized: &[C::Quantized],
    r: usize,
    alpha: f32,
    l_build: usize,
) -> VamanaGraph {
    assert_eq!(
        vectors.len(),
        ids.len(),
        "vectors and ids must have equal length"
    );
    assert_eq!(
        vectors.len(),
        quantized.len(),
        "vectors and quantized must have equal length"
    );
    assert!(!vectors.is_empty(), "cannot build from empty vector set");

    let n = vectors.len();
    let dim = vectors[0].len();
    let l = l_build.max(r);

    // --- Construct graph skeleton ---
    let mut graph = VamanaGraph::new(dim, r, alpha);
    for &id in ids {
        graph.add_node(id);
    }

    // --- Compute medoid as entry point ---
    let entry = compute_medoid(quantized, codec, 1000);
    graph.entry = entry;

    // --- Pass 1: randomised initialisation ---
    //
    // Each node gets R random distinct neighbors drawn with a simple
    // deterministic PRNG so the build is reproducible.
    let mut rng_state: u64 = 0xdeadbeef_cafebabe;
    let random_neighbors: Vec<Vec<u32>> = (0..n)
        .map(|i| {
            random_r_neighbors(i, n, r, &mut rng_state)
                .into_iter()
                .map(|j| j as u32)
                .collect()
        })
        .collect();

    for (i, nbrs) in random_neighbors.into_iter().enumerate() {
        graph.set_neighbors(i, nbrs);
    }

    // --- Pass 1 refinement: beam-search to replace random neighbors ---
    for (i, vector) in vectors.iter().enumerate().take(n) {
        let query = codec.prepare_query(vector);
        let candidates = greedy_beam_search_quantized(&graph, &query, codec, quantized, i, l);
        // Take the R closest from the candidates, excluding self.
        let mut nbrs: Vec<(u32, f32)> = candidates
            .into_iter()
            .filter(|(idx, _)| *idx != i as u32)
            .collect();
        nbrs.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        nbrs.truncate(r);
        graph.set_neighbors(i, nbrs.into_iter().map(|(idx, _)| idx).collect());
    }

    // --- Pass 2: α-pruning ---
    for i in 0..n {
        let query = codec.prepare_query(&vectors[i]);
        let mut candidates = greedy_beam_search_quantized(&graph, &query, codec, quantized, i, l);
        // Include existing neighbors as candidates.
        for &nbr in graph.neighbors(i) {
            if nbr as usize >= quantized.len() {
                continue;
            }
            let d = codec.fast_symmetric_distance(&quantized[i], &quantized[nbr as usize]);
            candidates.push((nbr, d));
        }
        candidates.retain(|(idx, _)| *idx != i as u32);
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.dedup_by_key(|(idx, _)| *idx);

        let pruned = alpha_prune(&candidates, i as u32, r, alpha, |a, b| {
            if a as usize >= quantized.len() || b as usize >= quantized.len() {
                f32::MAX
            } else {
                codec.fast_symmetric_distance(&quantized[a as usize], &quantized[b as usize])
            }
        });
        graph.set_neighbors(i, pruned);
    }

    graph
}

// ------------------------------------------------------------------
// Internal helpers
// ------------------------------------------------------------------

/// Compute the medoid index: node with minimum sum of distances to a random
/// sample of `sample_size` other nodes.
fn compute_medoid<C: VectorCodec>(
    quantized: &[C::Quantized],
    codec: &C,
    sample_size: usize,
) -> usize {
    let n = quantized.len();
    if n == 1 {
        return 0;
    }

    // Sample indices deterministically.
    let sample: Vec<usize> = if n <= sample_size {
        (0..n).collect()
    } else {
        uniform_sample(n, sample_size, 0xabcdef01_23456789)
    };

    let mut best_idx = 0usize;
    let mut best_sum = f32::MAX;

    for i in 0..n {
        let sum: f32 = sample
            .iter()
            .filter(|&&j| j != i)
            .map(|&j| codec.fast_symmetric_distance(&quantized[i], &quantized[j]))
            .sum();
        if sum < best_sum {
            best_sum = sum;
            best_idx = i;
        }
    }

    best_idx
}

/// Draw `count` distinct indices in `[0, n)` using a simple linear congruential
/// sampler. Not statistically uniform, but deterministic and cheap.
fn uniform_sample(n: usize, count: usize, seed: u64) -> Vec<usize> {
    let mut state = seed.max(1);
    let mut out = Vec::with_capacity(count);
    let mut seen = std::collections::HashSet::new();

    while out.len() < count {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let idx = (state >> 33) as usize % n;
        if seen.insert(idx) {
            out.push(idx);
        }
    }
    out
}

/// Generate R distinct random neighbors for node `i` from `[0, n)`.
fn random_r_neighbors(i: usize, n: usize, r: usize, state: &mut u64) -> Vec<usize> {
    let mut out = Vec::with_capacity(r);
    let mut seen = std::collections::HashSet::new();
    seen.insert(i);
    let target = r.min(n.saturating_sub(1));

    while out.len() < target {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        let j = (*state >> 33) as usize % n;
        if seen.insert(j) {
            out.push(j);
        }
    }
    out
}

/// Single-layer greedy beam-search returning `(node_idx, distance)` pairs.
///
/// Excludes `query_node_idx` from the result set.
fn greedy_beam_search_quantized<C: VectorCodec>(
    graph: &VamanaGraph,
    query: &C::Query,
    codec: &C,
    quantized: &[C::Quantized],
    query_node_idx: usize,
    l: usize,
) -> Vec<(u32, f32)> {
    use std::collections::{BinaryHeap, HashSet};

    #[derive(Clone, Copy, PartialEq)]
    struct Cand {
        dist: f32,
        idx: u32,
    }
    impl Eq for Cand {}
    impl PartialOrd for Cand {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for Cand {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            other
                .dist
                .partial_cmp(&self.dist)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(other.idx.cmp(&self.idx))
        }
    }

    let n = quantized.len();
    if n == 0 {
        return Vec::new();
    }

    let entry = graph.entry as u32;
    let entry_dist = codec.exact_asymmetric_distance(query, &quantized[graph.entry]);

    let mut visited: HashSet<u32> = HashSet::new();
    visited.insert(entry);

    let mut front: BinaryHeap<Cand> = BinaryHeap::new();
    front.push(Cand {
        dist: entry_dist,
        idx: entry,
    });

    // Max-heap of results (we keep the l smallest by evicting the largest).
    let _results: BinaryHeap<Cand> = BinaryHeap::new();
    // We use a min-heap wrapper below; simpler: collect then sort at end.
    let mut result_vec: Vec<Cand> = Vec::new();

    while let Some(cur) = front.pop() {
        // Prune if worse than l-th best result.
        if result_vec.len() >= l {
            result_vec.sort_by(|a, b| {
                a.dist
                    .partial_cmp(&b.dist)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            result_vec.truncate(l);
            if cur.dist > result_vec[result_vec.len() - 1].dist {
                break;
            }
        }

        result_vec.push(cur);

        for &nbr_idx in graph.neighbors(cur.idx as usize) {
            if visited.contains(&nbr_idx) || nbr_idx as usize >= n {
                continue;
            }
            visited.insert(nbr_idx);
            let d = codec.exact_asymmetric_distance(query, &quantized[nbr_idx as usize]);
            front.push(Cand {
                dist: d,
                idx: nbr_idx,
            });
        }
    }

    result_vec.sort_by(|a, b| {
        a.dist
            .partial_cmp(&b.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    result_vec.truncate(l);

    result_vec
        .into_iter()
        .filter(|c| c.idx as usize != query_node_idx)
        .map(|c| (c.idx, c.dist))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::scalar::l2_squared;
    use nodedb_codec::vector_quant::codec::VectorCodec;

    struct L2Codec;
    struct L2Q(Vec<f32>);

    impl AsRef<nodedb_codec::vector_quant::layout::UnifiedQuantizedVector> for L2Q {
        fn as_ref(&self) -> &nodedb_codec::vector_quant::layout::UnifiedQuantizedVector {
            panic!("stub")
        }
    }

    impl VectorCodec for L2Codec {
        type Quantized = L2Q;
        type Query = Vec<f32>;

        fn encode(&self, v: &[f32]) -> Self::Quantized {
            L2Q(v.to_vec())
        }

        fn prepare_query(&self, q: &[f32]) -> Self::Query {
            q.to_vec()
        }

        fn fast_symmetric_distance(&self, a: &Self::Quantized, b: &Self::Quantized) -> f32 {
            l2_squared(&a.0, &b.0)
        }

        fn exact_asymmetric_distance(&self, q: &Self::Query, v: &Self::Quantized) -> f32 {
            l2_squared(q, &v.0)
        }
    }

    fn rand_vecs(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut s = seed.max(1);
        (0..n)
            .map(|_| {
                (0..dim)
                    .map(|_| {
                        s ^= s << 13;
                        s ^= s >> 7;
                        s ^= s << 17;
                        (s as f32) / (u64::MAX as f32)
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn build_graph_has_connectivity() {
        let dim = 8;
        let n = 100;
        let codec = L2Codec;

        let vecs = rand_vecs(n, dim, 1234);
        let ids: Vec<u64> = (0..n as u64).collect();
        let quantized: Vec<L2Q> = vecs.iter().map(|v| codec.encode(v)).collect();

        let graph = build_vamana(&vecs, &ids, &codec, &quantized, 8, 1.2, 20);

        assert_eq!(graph.len(), n);

        // Every node should have at least one neighbor (the graph is connected
        // for n=100 with r=8).
        let isolated: usize = (0..n).filter(|&i| graph.neighbors(i).is_empty()).count();
        assert!(
            isolated < n / 2,
            "more than half the nodes are isolated; graph has poor connectivity"
        );

        // Entry point must be a valid index.
        assert!(graph.entry < n);
    }

    #[test]
    fn build_respects_degree_bound() {
        let dim = 4;
        let n = 30;
        let r = 5;
        let codec = L2Codec;

        let vecs = rand_vecs(n, dim, 99);
        let ids: Vec<u64> = (0..n as u64).collect();
        let quantized: Vec<L2Q> = vecs.iter().map(|v| codec.encode(v)).collect();

        let graph = build_vamana(&vecs, &ids, &codec, &quantized, r, 1.2, 15);

        for i in 0..n {
            assert!(
                graph.neighbors(i).len() <= r,
                "node {i} has {} neighbors, exceeds r={r}",
                graph.neighbors(i).len()
            );
        }
    }
}
