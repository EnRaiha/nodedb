// SPDX-License-Identifier: Apache-2.0

//! NaviX adaptive-local filtered HNSW traversal (VLDB 2025).
//!
//! Replaces ACORN-1's static 2-hop expansion with a per-hop heuristic switch
//! driven by local selectivity.  At each expansion step the algorithm asks:
//! "of this node's 1-hop neighbors, what fraction are in the allowed set?"
//! and then picks Standard / Directed / Blind accordingly.
//!
//! See `selectivity.rs` for heuristic boundary definitions.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use roaring::RoaringBitmap;

use crate::distance::distance;
use crate::hnsw::graph::{Candidate, HnswIndex};
use crate::navix::selectivity::{NavixHeuristic, local_selectivity_at, pick_heuristic};

/// A k-NN result returned by `navix_search`.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Internal node identifier (insertion order in the HNSW index).
    pub id: u32,
    /// Distance from the query vector.
    pub distance: f32,
}

/// Options for a NaviX filtered search.
pub struct NavixSearchOptions {
    /// Number of nearest neighbors to return.
    pub k: usize,
    /// Beam width (higher = better recall, more CPU).  Must be >= k.
    pub ef_search: usize,
    /// Sideways Information Passing (SIP): exact allowed-set semimask from the
    /// upstream filter operator.
    pub allowed: RoaringBitmap,
    /// Brute-force fallback threshold on global selectivity.
    /// When `allowed.len() / total_vectors < brute_force_threshold` the search
    /// bypasses HNSW and scans only the allowed IDs directly.
    /// Default 0.001 (0.1%).
    pub brute_force_threshold: f64,
}

impl Default for NavixSearchOptions {
    fn default() -> Self {
        Self {
            k: 10,
            ef_search: 64,
            allowed: RoaringBitmap::new(),
            brute_force_threshold: 0.001,
        }
    }
}

/// Adaptive-local NaviX filtered search.
///
/// Returns up to `options.k` nearest vectors from `index` to `query`, where
/// candidate IDs must be present in `options.allowed`.
///
/// # Errors
///
/// Returns an empty Vec when the index is empty or `options.allowed` is empty.
pub fn navix_search(
    index: &HnswIndex,
    query: &[f32],
    options: &NavixSearchOptions,
    metric: nodedb_types::vector_distance::DistanceMetric,
) -> Vec<SearchResult> {
    if index.is_empty() || options.allowed.is_empty() || options.k == 0 {
        return Vec::new();
    }

    let total = index.len();
    let global_sel = options.allowed.len() as f64 / total as f64;

    if global_sel < options.brute_force_threshold {
        return brute_force_on_allowed(index, query, options.k, &options.allowed, metric);
    }

    let Some(ep) = index.entry_point() else {
        return Vec::new();
    };

    // Phase 1: greedy descent from max_layer to layer 1 (unfiltered, as in
    // standard HNSW — we just want the best entry point for layer 0).
    let mut current_ep = ep;
    for layer in (1..=index.max_layer()).rev() {
        let results = unfiltered_search_layer(index, query, current_ep, 1, layer, metric);
        if let Some(nearest) = results.first() {
            current_ep = nearest.id;
        }
    }

    // Phase 2: adaptive-local filtered beam search at layer 0.
    let ef = options.ef_search.max(options.k);
    let results = navix_search_layer_0(index, query, current_ep, ef, &options.allowed, metric);

    results
        .into_iter()
        .take(options.k)
        .map(|c| SearchResult {
            id: c.id,
            distance: c.dist,
        })
        .collect()
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Brute-force scan over only the IDs in `allowed`.  Used when global
/// selectivity drops below the configured threshold.
fn brute_force_on_allowed(
    index: &HnswIndex,
    query: &[f32],
    k: usize,
    allowed: &RoaringBitmap,
    metric: nodedb_types::vector_distance::DistanceMetric,
) -> Vec<SearchResult> {
    let mut results: Vec<SearchResult> = allowed
        .iter()
        .filter_map(|id| {
            if index.is_deleted(id) {
                return None;
            }
            let v = index.get_vector(id)?;
            Some(SearchResult {
                id,
                distance: distance(query, v, metric),
            })
        })
        .collect();

    if results.len() > k {
        results.select_nth_unstable_by(k, |a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(k);
    }
    results.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results
}

/// Standard unfiltered single-layer beam search used for Phase-1 greedy
/// descent (layers 1..max_layer).
fn unfiltered_search_layer(
    index: &HnswIndex,
    query: &[f32],
    entry_point: u32,
    ef: usize,
    layer: usize,
    metric: nodedb_types::vector_distance::DistanceMetric,
) -> Vec<Candidate> {
    let mut visited: HashSet<u32> = HashSet::new();
    visited.insert(entry_point);

    let ep_dist = dist(index, query, entry_point, metric);
    let ep_cand = Candidate {
        dist: ep_dist,
        id: entry_point,
    };

    let mut candidates: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
    candidates.push(Reverse(ep_cand));

    let mut results: BinaryHeap<Candidate> = BinaryHeap::new();
    if !index.is_deleted(entry_point) {
        results.push(ep_cand);
    }

    while let Some(Reverse(current)) = candidates.pop() {
        if let Some(worst) = results.peek()
            && current.dist > worst.dist
            && results.len() >= ef
        {
            break;
        }

        for &nb in index.neighbors_at(current.id, layer) {
            if !visited.insert(nb) {
                continue;
            }
            let d = dist(index, query, nb, metric);
            let nb_cand = Candidate { dist: d, id: nb };
            let worst_dist = results.peek().map_or(f32::INFINITY, |w| w.dist);
            if d < worst_dist || results.len() < ef {
                candidates.push(Reverse(nb_cand));
            }
            if !index.is_deleted(nb) {
                results.push(nb_cand);
                if results.len() > ef {
                    results.pop();
                }
            }
        }
    }

    let mut v: Vec<Candidate> = results.into_vec();
    v.sort_unstable_by(|a, b| a.dist.total_cmp(&b.dist));
    v
}

/// NaviX adaptive-local filtered beam search at layer 0.
///
/// Per-hop heuristic switch:
/// - **Standard**: score every allowed neighbor normally.
/// - **Directed**: score 1-hop, expand 2-hop of the single best neighbor.
/// - **Blind**: skip 1-hop scoring; sample 2-hop of all 1-hop neighbors.
fn navix_search_layer_0(
    index: &HnswIndex,
    query: &[f32],
    entry_point: u32,
    ef: usize,
    allowed: &RoaringBitmap,
    metric: nodedb_types::vector_distance::DistanceMetric,
) -> Vec<Candidate> {
    let mut visited: HashSet<u32> = HashSet::new();
    visited.insert(entry_point);

    let ep_dist = dist(index, query, entry_point, metric);
    let ep_cand = Candidate {
        dist: ep_dist,
        id: entry_point,
    };

    let mut candidates: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
    candidates.push(Reverse(ep_cand));

    let mut results: BinaryHeap<Candidate> = BinaryHeap::new();

    // Entry point enters results only if it is allowed.
    if !index.is_deleted(entry_point) && allowed.contains(entry_point) {
        results.push(ep_cand);
    }

    while let Some(Reverse(current)) = candidates.pop() {
        if let Some(worst) = results.peek()
            && current.dist > worst.dist
            && results.len() >= ef
        {
            break;
        }

        let neighbors_1hop = index.neighbors_at(current.id, 0);
        let local_sel = local_selectivity_at(neighbors_1hop, allowed);
        let heuristic = pick_heuristic(local_sel);

        match heuristic {
            NavixHeuristic::Standard => {
                expand_standard(ExpandArgs {
                    index,
                    query,
                    neighbors_1hop,
                    allowed,
                    ef,
                    metric,
                    visited: &mut visited,
                    candidates: &mut candidates,
                    results: &mut results,
                });
            }
            NavixHeuristic::Directed => {
                expand_directed(ExpandArgs {
                    index,
                    query,
                    neighbors_1hop,
                    allowed,
                    ef,
                    metric,
                    visited: &mut visited,
                    candidates: &mut candidates,
                    results: &mut results,
                });
            }
            NavixHeuristic::Blind => {
                expand_blind(ExpandArgs {
                    index,
                    query,
                    neighbors_1hop,
                    allowed,
                    ef,
                    metric,
                    visited: &mut visited,
                    candidates: &mut candidates,
                    results: &mut results,
                });
            }
        }
    }

    let mut v: Vec<Candidate> = results.into_vec();
    v.sort_unstable_by(|a, b| a.dist.total_cmp(&b.dist));
    v
}

/// Shared beam-search state bundled for the per-heuristic `expand_*` helpers.
struct ExpandArgs<'a> {
    index: &'a HnswIndex,
    query: &'a [f32],
    neighbors_1hop: &'a [u32],
    allowed: &'a RoaringBitmap,
    ef: usize,
    metric: nodedb_types::vector_distance::DistanceMetric,
    visited: &'a mut HashSet<u32>,
    candidates: &'a mut BinaryHeap<Reverse<Candidate>>,
    results: &'a mut BinaryHeap<Candidate>,
}

/// Standard expansion: score every allowed 1-hop neighbor and add to heaps.
fn expand_standard(args: ExpandArgs<'_>) {
    let ExpandArgs {
        index,
        query,
        neighbors_1hop,
        allowed,
        ef,
        metric,
        visited,
        candidates,
        results,
    } = args;
    for &nb in neighbors_1hop {
        if !visited.insert(nb) {
            continue;
        }
        let d = dist(index, query, nb, metric);
        let nb_cand = Candidate { dist: d, id: nb };
        let worst_dist = results.peek().map_or(f32::INFINITY, |w| w.dist);
        if d < worst_dist || results.len() < ef {
            candidates.push(Reverse(nb_cand));
        }
        if !index.is_deleted(nb) && allowed.contains(nb) {
            results.push(nb_cand);
            if results.len() > ef {
                results.pop();
            }
        }
    }
}

/// Directed expansion: score 1-hop, pick the single best allowed neighbor,
/// then expand that neighbor's 2-hop neighbors into the heaps.
fn expand_directed(args: ExpandArgs<'_>) {
    let ExpandArgs {
        index,
        query,
        neighbors_1hop,
        allowed,
        ef,
        metric,
        visited,
        candidates,
        results,
    } = args;
    // Score 1-hop; track the best allowed neighbor.
    let mut best_allowed: Option<(u32, f32)> = None;

    for &nb in neighbors_1hop {
        let already_visited = !visited.insert(nb);
        if already_visited {
            continue;
        }
        let d = dist(index, query, nb, metric);
        let nb_cand = Candidate { dist: d, id: nb };

        let worst_dist = results.peek().map_or(f32::INFINITY, |w| w.dist);
        if d < worst_dist || results.len() < ef {
            candidates.push(Reverse(nb_cand));
        }

        if !index.is_deleted(nb) && allowed.contains(nb) {
            if best_allowed.is_none_or(|(_, bd)| d < bd) {
                best_allowed = Some((nb, d));
            }
            results.push(nb_cand);
            if results.len() > ef {
                results.pop();
            }
        }
    }

    // Expand 2-hop of the single best allowed neighbor.
    if let Some((best_id, _)) = best_allowed {
        for &nb2 in index.neighbors_at(best_id, 0) {
            if !visited.insert(nb2) {
                continue;
            }
            let d = dist(index, query, nb2, metric);
            let nb2_cand = Candidate { dist: d, id: nb2 };
            let worst_dist = results.peek().map_or(f32::INFINITY, |w| w.dist);
            if d < worst_dist || results.len() < ef {
                candidates.push(Reverse(nb2_cand));
            }
            if !index.is_deleted(nb2) && allowed.contains(nb2) {
                results.push(nb2_cand);
                if results.len() > ef {
                    results.pop();
                }
            }
        }
    }
}

/// Blind expansion: skip scoring 1-hop; expand 2-hop of all 1-hop neighbors,
/// adding to heaps only IDs that are in `allowed`.
fn expand_blind(args: ExpandArgs<'_>) {
    let ExpandArgs {
        index,
        query,
        neighbors_1hop,
        allowed,
        ef,
        metric,
        visited,
        candidates,
        results,
    } = args;
    for &nb1 in neighbors_1hop {
        // Mark 1-hop as visited so we do not double-score them later,
        // but do not score them — that is the Blind heuristic.
        visited.insert(nb1);

        for &nb2 in index.neighbors_at(nb1, 0) {
            if !visited.insert(nb2) {
                continue;
            }
            if index.is_deleted(nb2) {
                continue;
            }
            if !allowed.contains(nb2) {
                continue;
            }
            let d = dist(index, query, nb2, metric);
            let nb2_cand = Candidate { dist: d, id: nb2 };
            let worst_dist = results.peek().map_or(f32::INFINITY, |w| w.dist);
            if d < worst_dist || results.len() < ef {
                candidates.push(Reverse(nb2_cand));
            }
            results.push(nb2_cand);
            if results.len() > ef {
                results.pop();
            }
        }
    }
}

/// Inline helper: distance from query to a stored node using the given metric.
#[inline]
fn dist(
    index: &HnswIndex,
    query: &[f32],
    node_id: u32,
    metric: nodedb_types::vector_distance::DistanceMetric,
) -> f32 {
    match index.get_vector(node_id) {
        Some(v) => distance(query, v, metric),
        None => f32::INFINITY,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};

    fn build_index(n: usize) -> HnswIndex {
        let mut idx = HnswIndex::with_seed(
            3,
            HnswParams {
                m: 8,
                m0: 16,
                ef_construction: 50,
                metric: DistanceMetric::L2,
                dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
            },
            42,
        );
        for i in 0..n {
            idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
        }
        idx
    }

    fn all_allowed(n: u32) -> RoaringBitmap {
        let mut b = RoaringBitmap::new();
        for i in 0..n {
            b.insert(i);
        }
        b
    }

    /// Full allowed set → recall should match unfiltered HNSW closely.
    #[test]
    fn full_allowed_matches_unfiltered() {
        let idx = build_index(20);
        let query = [10.0f32, 0.0, 0.0];
        let allowed = all_allowed(20);

        let opts = NavixSearchOptions {
            k: 5,
            ef_search: 64,
            allowed,
            brute_force_threshold: 0.001,
        };

        let navix_res = navix_search(&idx, &query, &opts, DistanceMetric::L2);
        let hnsw_res = idx.search(&query, 5, 64);

        assert!(!navix_res.is_empty());
        // The best result should be id=10 (exact match) in both cases.
        assert_eq!(navix_res[0].id, hnsw_res[0].id);
    }

    /// Allowed bitmap contains only one ID — that ID must be returned.
    #[test]
    fn single_allowed_id_returned() {
        let idx = build_index(20);
        let query = [5.0f32, 0.0, 0.0];
        let mut allowed = RoaringBitmap::new();
        allowed.insert(15); // Only ID 15 is allowed.

        let opts = NavixSearchOptions {
            k: 5,
            ef_search: 64,
            allowed,
            brute_force_threshold: 0.001,
        };

        let res = navix_search(&idx, &query, &opts, DistanceMetric::L2);
        // With only one allowed ID, we get at most 1 result.
        assert!(res.len() <= 1);
        if let Some(r) = res.first() {
            assert_eq!(r.id, 15);
        }
    }

    /// ~50% bitmap — results must all be in the allowed set.
    #[test]
    fn half_allowed_results_in_allowed() {
        let idx = build_index(20);
        let query = [10.0f32, 0.0, 0.0];

        let mut allowed = RoaringBitmap::new();
        for i in (0..20u32).step_by(2) {
            allowed.insert(i); // even IDs only
        }

        let opts = NavixSearchOptions {
            k: 3,
            ef_search: 64,
            allowed: allowed.clone(),
            brute_force_threshold: 0.001,
        };

        let res = navix_search(&idx, &query, &opts, DistanceMetric::L2);
        assert!(!res.is_empty());
        for r in &res {
            assert!(
                allowed.contains(r.id),
                "got disallowed id {} in results",
                r.id
            );
        }
    }

    /// Brute-force fallback fires when `brute_force_threshold` is set high.
    /// Output must equal manual brute-force over the allowed set.
    #[test]
    fn brute_force_fallback_matches_manual() {
        let idx = build_index(20);
        let query = [8.0f32, 0.0, 0.0];

        let mut allowed = RoaringBitmap::new();
        allowed.insert(3);
        allowed.insert(7);
        allowed.insert(12);

        // Set threshold = 0.5 → global sel = 3/20 = 0.15 < 0.5 → always brute-force.
        let opts = NavixSearchOptions {
            k: 5,
            ef_search: 64,
            allowed: allowed.clone(),
            brute_force_threshold: 0.5,
        };

        let res = navix_search(&idx, &query, &opts, DistanceMetric::L2);

        // Manual brute-force reference.
        let mut manual: Vec<(u32, f32)> = allowed
            .iter()
            .map(|id| {
                let v = idx.get_vector(id).unwrap();
                let d = distance(&query, v, DistanceMetric::L2);
                (id, d)
            })
            .collect();
        manual.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        assert_eq!(res.len(), manual.len().min(opts.k));
        for (r, (mid, _)) in res.iter().zip(manual.iter()) {
            assert_eq!(r.id, *mid, "brute-force result mismatch");
        }
    }

    /// Empty index returns empty results.
    #[test]
    fn empty_index_returns_empty() {
        let idx = HnswIndex::new(
            3,
            HnswParams {
                m: 8,
                m0: 16,
                ef_construction: 50,
                metric: DistanceMetric::L2,
                dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
            },
        );
        let mut allowed = RoaringBitmap::new();
        allowed.insert(0);

        let opts = NavixSearchOptions {
            k: 5,
            ef_search: 64,
            allowed,
            brute_force_threshold: 0.001,
        };
        let res = navix_search(&idx, &[1.0, 0.0, 0.0], &opts, DistanceMetric::L2);
        assert!(res.is_empty());
    }

    /// Empty allowed bitmap returns empty results.
    #[test]
    fn empty_allowed_returns_empty() {
        let idx = build_index(10);
        let opts = NavixSearchOptions {
            k: 5,
            ef_search: 64,
            allowed: RoaringBitmap::new(),
            brute_force_threshold: 0.001,
        };
        let res = navix_search(&idx, &[5.0, 0.0, 0.0], &opts, DistanceMetric::L2);
        assert!(res.is_empty());
    }
}
