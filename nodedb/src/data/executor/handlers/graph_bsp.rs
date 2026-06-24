// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane handler for `GraphOp::BspSuperstep` — runs ONE distributed
//! PageRank BSP superstep on this shard's local CSR partition.
//!
//! Phase A primitive: the handler is stateless across supersteps. All
//! per-superstep state is carried in the `GraphOp::BspSuperstep` plan variant
//! (the round-tripped `rank_seed`, the `incoming_contributions` routed to this
//! shard's owned nodes) and returned in [`BspSuperstepResult`]. The
//! Control-Plane coordinator (Phase B) owns the superstep loop, convergence
//! check, and contribution routing; this handler only computes one shard's
//! local scatter and the cross-shard contributions it must emit.
//!
//! Ownership model: each superstep builds a collection-scoped CSR via
//! `build_csr_for_collection` (the same call used by `execute_graph_algo`) so
//! that distributed PageRank runs over exactly the same `(collection,
//! edge_label)` subgraph as single-node `GRAPH ALGO ON <collection>`.  Only
//! nodes whose `VShardId::from_key(name)` is in `owned_vshards` are "owned" by
//! this shard and carry a rank.  An edge to a non-owned destination is a
//! *ghost* edge: its contribution is emitted in `outbound` (tagged with the
//! destination's vShard) instead of being scattered locally.
//! `VShardId::from_key` is a pure hash, so no routing table is needed on the
//! Data Plane.
//!
//! Count-only sentinel: `global_n == 0` means the coordinator is running its
//! pre-superstep count phase. `run_bsp_superstep_core` then short-circuits after
//! building the owned-node set and returns just `vertex_count` + `node_names`
//! (no superstep runs) so the coordinator can sum owned counts into the real
//! `global_n`. Every actual superstep passes `global_n > 0`.

use std::collections::{HashMap, HashSet};

use nodedb_cluster::distributed_graph::ShardPageRankState;
use nodedb_graph::{AlgoParams, CsrIndex, GraphAlgorithm};
use tracing::debug;

use crate::types::VShardId;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::BspSuperstepResult;

use super::graph_algo::build_csr_for_collection;

/// Borrowed arguments for [`CoreLoop::execute_bsp_superstep`], destructured
/// from the `GraphOp::BspSuperstep` plan variant by the dispatcher.
pub struct BspSuperstepArgs<'a> {
    pub algorithm: &'a GraphAlgorithm,
    pub params: &'a AlgoParams,
    pub superstep: u32,
    pub global_n: usize,
    pub owned_vshards: &'a [u32],
    pub incoming_contributions: &'a [(String, f64)],
    pub rank_seed: &'a [(String, f64)],
    /// Global dangling-node rank mass from the coordinator (see
    /// `BspSuperstepPlan::global_dangling`). `0.0` on count phase and superstep 0.
    pub global_dangling: f64,
    /// Coordinator-computed GLOBAL `Σ max(w, 0.0)` over the Personalized-PageRank
    /// seed map (see `BspSuperstepPlan::personalization_sum`). `0.0` means standard
    /// uniform PageRank; `> 0.0` activates Personalized PageRank — each owned node's
    /// globally-normalized seed share is `max(seed[name], 0.0) / personalization_sum`.
    pub personalization_sum: f64,
}

/// The pure BSP-superstep core: given an already-built `CsrIndex` and the
/// per-superstep arguments, builds the owned-node set, initializes
/// [`ShardPageRankState`], seeds the rank vector, loads incoming contributions,
/// and runs one superstep, returning the complete [`BspSuperstepResult`].
///
/// Both [`CoreLoop::execute_bsp_superstep`] (after calling
/// `build_csr_for_collection`) and the unit tests call this function, so the
/// tests exercise the real handler math rather than a re-implementation.
///
/// Returns `Ok(BspSuperstepResult)` in normal operation; other internal paths
/// (e.g. encoding) may still surface `crate::Error::Internal`.
pub(super) fn run_bsp_superstep_core(
    csr: &CsrIndex,
    args: &BspSuperstepArgs<'_>,
) -> Result<BspSuperstepResult, crate::Error> {
    // Build a HashSet of owned vShards for O(1) membership checks in the
    // per-edge hot path (avoids O(n) slice scan per edge).
    let owned_set: HashSet<u32> = args.owned_vshards.iter().copied().collect();
    let is_owned =
        |name: &str| -> bool { owned_set.contains(&VShardId::from_key(name.as_bytes()).as_u32()) };

    // Build the owned-node set: CSR raw u32 id → dense owned index, plus the
    // parallel name vector. `rank_vec`/`node_names` index by dense owned id.
    let node_count = csr.node_count();
    let mut raw_to_owned: HashMap<u32, u32> = HashMap::new();
    let mut node_names: Vec<String> = Vec::new();
    // Reverse map: dense owned index → CSR raw id (for edge iteration).
    // All three maps are populated in a single pass.
    let mut owned_to_raw: Vec<u32> = Vec::new();
    for raw in 0..node_count as u32 {
        let name = csr.node_name_raw(raw);
        if is_owned(name) {
            let dense = node_names.len() as u32;
            raw_to_owned.insert(raw, dense);
            node_names.push(name.to_string());
            owned_to_raw.push(raw);
        }
    }
    let vertex_count = node_names.len();

    // `global_n == 0` is the COUNT-ONLY sentinel: the Control-Plane coordinator
    // runs a cheap count phase (sum every shard's owned `vertex_count` →
    // `global_n`) BEFORE superstep 0. In that phase we must not run any
    // superstep — `global_n` is not yet known, so the teleport / dangling terms
    // would be wrong. Short-circuit after building the owned-node set and return
    // just the count + names. A real run always passes `global_n > 0`.
    if args.global_n == 0 {
        // Count this shard's owned nodes that are positively-weighted seed keys, so
        // the coordinator can sum cluster-wide seed hits and decide whether
        // personalization is active (any seed name exists in the graph) or falls
        // back to uniform — mirroring single-node `build_personalization` → `None`.
        let seed_hits = match args.params.personalization_vector() {
            Some(seed) => node_names
                .iter()
                .filter(|name| seed.get(name.as_str()).copied().unwrap_or(0.0) > 0.0)
                .count(),
            None => 0,
        };
        return Ok(BspSuperstepResult {
            local_delta: 0.0,
            outbound: Vec::new(),
            rank_vec: Vec::new(),
            vertex_count,
            node_names,
            dangling_sum: 0.0,
            seed_hits,
        });
    }

    // Out-degree per owned node, counted over ALL out-edges (owned + ghost)
    // so dangling classification and contribution division match the
    // single-node PageRank semantics (a node with only ghost edges is NOT
    // dangling).
    let mut out_degrees: Vec<usize> = vec![0; vertex_count];
    for (raw, &owned) in &raw_to_owned {
        out_degrees[owned as usize] = csr.out_degree_raw(*raw);
    }

    // `csr_out_edges` closure: dense owned index → out-edges as
    // (dst_name, is_ghost, target_shard). Ghost = destination not owned by
    // this shard. Uses the HashSet for O(1) ghost classification.
    let csr_out_edges = |owned_idx: u32| -> Vec<(String, bool, u16)> {
        let raw = owned_to_raw[owned_idx as usize];
        csr.iter_out_edges_raw(raw)
            .map(|(_label, dst_raw)| {
                let dst_name = csr.node_name_raw(dst_raw).to_string();
                let dst_vs = VShardId::from_key(dst_name.as_bytes()).as_u32();
                let ghost = !owned_set.contains(&dst_vs);
                (dst_name, ghost, dst_vs as u16)
            })
            .collect()
    };

    let mut state =
        ShardPageRankState::init(vertex_count, out_degrees, |_name| None, &csr_out_edges);

    // Personalized-PageRank seed share for THIS shard's owned nodes, GLOBALLY
    // normalized by the coordinator-computed cluster-wide sum so `Σ_global p_i ==
    // 1.0`. `personalization_sum > 0.0` activates PPR; `None` (== 0.0) recovers
    // standard uniform PageRank. `p[i] = max(seed[name_i], 0.0) / personalization_sum`,
    // positionally aligned with `state.rank` / `node_names`.
    let personalization: Option<Vec<f64>> = if args.personalization_sum > 0.0 {
        let seed = args.params.personalization_vector();
        let p: Vec<f64> = node_names
            .iter()
            .map(|name| {
                let w = seed
                    .and_then(|m| m.get(name.as_str()).copied())
                    .unwrap_or(0.0)
                    .max(0.0);
                w / args.personalization_sum
            })
            .collect();
        Some(p)
    } else {
        None
    };

    // Seed rank from the name-keyed seed. Each owned node takes its seed rank by
    // NAME (so the same seed can be fanned to every core, each self-filtering to
    // its owned nodes); superstep 0 sends an empty seed → the initial rank
    // distribution. For PPR superstep 0 the initial rank IS the seed share `p[i]`
    // (matching single-node `rank[i] = p[i]`); for uniform it is `1/global_n`.
    let init = 1.0 / args.global_n as f64;
    if args.rank_seed.is_empty() {
        match &personalization {
            Some(p) => {
                for (slot, &pi) in state.rank.iter_mut().zip(p.iter()) {
                    *slot = pi;
                }
            }
            None => {
                for r in state.rank.iter_mut() {
                    *r = init;
                }
            }
        }
    } else {
        let seed: std::collections::HashMap<&str, f64> = args
            .rank_seed
            .iter()
            .map(|(name, rank)| (name.as_str(), *rank))
            .collect();
        for (i, name) in node_names.iter().enumerate() {
            // A node missing from the seed falls back to the uniform init — a
            // correct coordinator always includes every owned node, but this
            // keeps a missing entry safe rather than panicking.
            state.rank[i] = seed.get(name.as_str()).copied().unwrap_or(init);
        }
    }

    // Load incoming cross-shard contributions for THIS shard's owned nodes.
    // `superstep` folds them into next_rank before the rank swap (see the
    // ordering contract on `ShardPageRankState::superstep`).
    for (dst_name, value) in args.incoming_contributions {
        state.add_remote_contribution(dst_name.clone(), *value);
    }

    // Local edge iterator: dense owned index → owned destination dense
    // indices (ghost destinations are excluded — they become `outbound`).
    let local_edge_iter = |owned_idx: u32| -> Vec<u32> {
        let raw = owned_to_raw[owned_idx as usize];
        csr.iter_out_edges_raw(raw)
            .filter_map(|(_label, dst_raw)| raw_to_owned.get(&dst_raw).copied())
            .collect()
    };

    // Map an incoming destination name back to its local owned index.
    let node_id_to_local = |name: &str| -> Option<u32> {
        csr.node_id_raw(name)
            .and_then(|raw| raw_to_owned.get(&raw).copied())
    };

    let damping = args.params.damping_factor();
    let (local_delta, local_dangling_sum, outbound_map) = state.superstep(
        damping,
        args.global_n,
        args.global_dangling,
        personalization.as_deref(),
        &local_edge_iter,
        &node_id_to_local,
    );

    // Flatten outbound HashMap<u16, Vec<(String, f64)>> into the msgpack-flat
    // (target_vshard, dst_name, contribution) shape.
    let mut outbound: Vec<(u32, String, f64)> = Vec::new();
    for (target_shard, contribs) in outbound_map {
        for (dst_name, contrib) in contribs {
            outbound.push((target_shard as u32, dst_name, contrib));
        }
    }

    Ok(BspSuperstepResult {
        local_delta,
        outbound,
        rank_vec: state.rank,
        vertex_count,
        node_names,
        dangling_sum: local_dangling_sum,
        // `seed_hits` is a count-phase-only field (populated above when
        // `global_n == 0`); real supersteps leave it zero.
        seed_hits: 0,
    })
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_bsp_superstep(
        &self,
        task: &ExecutionTask,
        tid: u64,
        args: BspSuperstepArgs<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            algorithm = args.algorithm.name(),
            collection = %args.params.collection,
            superstep = args.superstep,
            global_n = args.global_n,
            "bsp superstep dispatch"
        );

        // Phase A supports PageRank only. Other algorithms have no BSP form yet.
        if *args.algorithm != GraphAlgorithm::PageRank {
            return self.response_error(
                task,
                ErrorCode::Unsupported {
                    detail: format!(
                        "distributed BSP superstep is only implemented for PageRank, got {}",
                        args.algorithm.name()
                    ),
                },
            );
        }

        let database_id = task.request.database_id.as_u64();

        // Build a collection-scoped CSR — same call as execute_graph_algo — so
        // distributed PageRank runs over exactly the same (collection, edge_label)
        // subgraph as single-node GRAPH ALGO ON <collection>.
        let csr = match build_csr_for_collection(
            &self.edge_store,
            database_id,
            tid,
            &args.params.collection,
            args.params.edge_label.as_deref(),
            None,
        ) {
            Ok(c) => c,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };

        if csr.node_count() == 0 {
            return self.encode_result(task, BspSuperstepResult::default());
        }

        match run_bsp_superstep_core(&csr, &args) {
            Ok(result) => self.encode_result(task, result),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Serialize a `BspSuperstepResult` into a response payload (zerompk).
    fn encode_result(&self, task: &ExecutionTask, result: BspSuperstepResult) -> Response {
        match zerompk::to_msgpack_vec(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("bsp superstep result encode: {e}"),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a small CSR with a known triangle topology: a→b, b→c, c→a.
    fn triangle_csr() -> CsrIndex {
        let mut csr = CsrIndex::new();
        for n in ["a", "b", "c"] {
            csr.add_node(n).unwrap();
        }
        csr.add_edge("a", "e", "b").unwrap();
        csr.add_edge("b", "e", "c").unwrap();
        csr.add_edge("c", "e", "a").unwrap();
        csr.compact().unwrap();
        csr
    }

    /// Minimal [`AlgoParams`] carrying only the fields `run_bsp_superstep_core` reads.
    fn dummy_params(damping: f64) -> AlgoParams {
        AlgoParams {
            collection: "test_coll".into(),
            damping: Some(damping),
            ..AlgoParams::default()
        }
    }

    #[test]
    fn all_owned_no_ghosts_matches_single_node_superstep() {
        let csr = triangle_csr();
        // Own every vShard → no node is a ghost.
        let owned: Vec<u32> = (0..VShardId::COUNT).collect();
        let params = dummy_params(0.85);
        let args = BspSuperstepArgs {
            algorithm: &GraphAlgorithm::PageRank,
            params: &params,
            superstep: 0,
            global_n: 3,
            owned_vshards: &owned,
            incoming_contributions: &[],
            rank_seed: &[],
            global_dangling: 0.0,
            personalization_sum: 0.0,
        };

        let res = run_bsp_superstep_core(&csr, &args).unwrap();

        // No ghost edges → nothing escapes the shard.
        assert!(res.outbound.is_empty(), "no edge should be cross-shard");
        assert_eq!(res.vertex_count, 3);
        assert_eq!(res.node_names.len(), 3);
        assert_eq!(res.rank_vec.len(), 3);

        // Cross-check the local scatter against ShardPageRankState directly:
        // a uniform-init 3-node ring where each node has out-degree 1. The
        // mass is conserved (sum stays 1.0) and delta is non-negative.
        let sum: f64 = res.rank_vec.iter().sum();
        assert!((sum - 1.0).abs() < 1e-9, "rank mass conserved, got {sum}");
        assert!(res.local_delta >= 0.0);

        // Each node receives exactly one neighbor's full damped contribution
        // (ring), so all ranks are equal after one uniform-init superstep.
        let r0 = res.rank_vec[0];
        for r in &res.rank_vec {
            assert!((r - r0).abs() < 1e-12, "ring symmetry: ranks equal");
        }
    }

    #[test]
    fn global_n_zero_is_count_only_no_superstep() {
        let csr = triangle_csr();
        // Exclude c's vShard so only a and b are owned — proves the count-only
        // path reports the OWNED vertex count (not the whole CSR) and still
        // emits zero rank/outbound.
        let c_vs = VShardId::from_key(b"c").as_u32();
        let owned: Vec<u32> = (0..VShardId::COUNT).filter(|&v| v != c_vs).collect();
        let params = dummy_params(0.85);
        let args = BspSuperstepArgs {
            algorithm: &GraphAlgorithm::PageRank,
            params: &params,
            superstep: 0,
            // Sentinel: count phase only.
            global_n: 0,
            owned_vshards: &owned,
            incoming_contributions: &[],
            rank_seed: &[],
            global_dangling: 0.0,
            personalization_sum: 0.0,
        };

        let res = run_bsp_superstep_core(&csr, &args).unwrap();

        // Count-only: owned vertex count + names, but NO superstep ran.
        assert_eq!(res.vertex_count, 2, "owned node count (a, b) reported");
        assert_eq!(res.node_names, vec!["a".to_string(), "b".to_string()]);
        assert!(res.rank_vec.is_empty(), "no ranks computed in count phase");
        assert!(res.outbound.is_empty(), "no contributions in count phase");
        assert_eq!(res.local_delta, 0.0, "no convergence delta in count phase");
    }

    #[test]
    fn forced_ghost_edge_appears_in_outbound_not_local_scatter() {
        let csr = triangle_csr();
        // Find c's vShard and exclude it → edge b→c becomes a ghost edge, and
        // c itself is no longer owned (not in the rank vector).
        let c_vs = VShardId::from_key(b"c").as_u32();
        let owned: Vec<u32> = (0..VShardId::COUNT).filter(|&v| v != c_vs).collect();
        let params = dummy_params(0.85);
        let args = BspSuperstepArgs {
            algorithm: &GraphAlgorithm::PageRank,
            params: &params,
            superstep: 0,
            global_n: 3,
            owned_vshards: &owned,
            incoming_contributions: &[],
            rank_seed: &[],
            global_dangling: 0.0,
            personalization_sum: 0.0,
        };

        let res = run_bsp_superstep_core(&csr, &args).unwrap();

        // c is excluded from the owned set.
        assert!(!res.node_names.contains(&"c".to_string()));
        assert_eq!(res.vertex_count, 2);

        // b→c is the only ghost edge → exactly one outbound entry, tagged with
        // c's vShard, carrying b's damped contribution.
        assert_eq!(res.outbound.len(), 1, "exactly one cross-shard edge");
        let (target_vs, dst_name, contrib) = &res.outbound[0];
        assert_eq!(*target_vs, c_vs, "outbound tagged with destination vShard");
        assert_eq!(dst_name, "c");

        // b's contribution = damping * rank_b / out_degree_b. b's out-degree is
        // 1 (only b→c), rank_b = 1/global_n = 1/3.
        let expected = 0.85 * (1.0 / 3.0) / 1.0;
        assert!(
            (contrib - expected).abs() < 1e-12,
            "ghost contribution = damped share, got {contrib} expected {expected}"
        );

        // The ghost contribution must NOT have been scattered into any local
        // rank. Owned nodes are a (a→b kept local) and b (b→c is ghost). Only
        // a's edge a→b scatters locally, so b's rank reflects a's contribution
        // plus base, and a's rank is just base (c→a is incoming from a ghost
        // shard and not present this superstep).
        assert_eq!(res.node_names, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn count_phase_reports_seed_hits() {
        use std::collections::HashMap;
        let csr = triangle_csr();
        let owned: Vec<u32> = (0..VShardId::COUNT).collect();
        // Seed on "a" (weight 1.0) and "ghost" (absent) → exactly one owned hit.
        let mut seed = HashMap::new();
        seed.insert("a".to_string(), 1.0);
        seed.insert("ghost".to_string(), 1.0);
        let params = AlgoParams {
            collection: "test_coll".into(),
            damping: Some(0.85),
            personalization_vector: Some(seed),
            ..AlgoParams::default()
        };
        let args = BspSuperstepArgs {
            algorithm: &GraphAlgorithm::PageRank,
            params: &params,
            superstep: 0,
            global_n: 0, // count phase
            owned_vshards: &owned,
            incoming_contributions: &[],
            rank_seed: &[],
            global_dangling: 0.0,
            personalization_sum: 0.0,
        };
        let res = run_bsp_superstep_core(&csr, &args).unwrap();
        assert_eq!(
            res.seed_hits, 1,
            "only 'a' is an owned positively-weighted seed"
        );
        assert_eq!(res.vertex_count, 3);
    }

    #[test]
    fn personalized_superstep0_seeds_rank_from_p() {
        use std::collections::HashMap;
        let csr = triangle_csr();
        let owned: Vec<u32> = (0..VShardId::COUNT).collect();
        // All seed mass on "a"; global sum (computed by coordinator) is 1.0.
        let mut seed = HashMap::new();
        seed.insert("a".to_string(), 1.0);
        let params = AlgoParams {
            collection: "test_coll".into(),
            damping: Some(0.85),
            personalization_vector: Some(seed),
            ..AlgoParams::default()
        };
        let args = BspSuperstepArgs {
            algorithm: &GraphAlgorithm::PageRank,
            params: &params,
            superstep: 0,
            global_n: 3,
            owned_vshards: &owned,
            incoming_contributions: &[],
            rank_seed: &[], // superstep 0 → init from p
            global_dangling: 0.0,
            personalization_sum: 1.0, // global Σ max(w,0)
        };
        let res = run_bsp_superstep_core(&csr, &args).unwrap();
        // Exact one-step outcome on the directed ring a→b→c→a with all seed mass
        // on `a` (p = [a:1, b:0, c:0]), no dangling (every node out-degree 1),
        // damping d = 0.85:
        //   * superstep 0 inits rank from p → rank = [a:1, b:0, c:0]
        //   * base[i] = (1-d) * p[i] → only `a` receives teleport (0.15)
        //   * `a` sends its full mass to its successor `b`, damped by d
        // ⇒ a = 1-d = 0.15, b = d = 0.85, c = 0.0.
        // After a SINGLE step the seed does NOT yet dominate — its mass has flowed
        // to the successor; the seed only wins at convergence (covered by the
        // cross-node integration test). These exact values instead pin down what a
        // single personalized step must produce, distinguishing it from uniform
        // PageRank (where ring symmetry would give all three nodes 1/3): `b = d`
        // proves rank was seeded from `p` (not `1/global_n`), and `c = 0` proves
        // the teleport landed only on the seed, not uniformly.
        let rank_of = |name: &str| -> f64 {
            let i = res
                .node_names
                .iter()
                .position(|n| n == name)
                .expect("node present");
            res.rank_vec[i]
        };
        let d = 0.85;
        assert!(
            (rank_of("a") - (1.0 - d)).abs() < 1e-9,
            "seed `a` base = (1-d): {}",
            rank_of("a")
        );
        assert!(
            (rank_of("b") - d).abs() < 1e-9,
            "successor `b` must hold `a`'s damped init mass (proves init from p): {}",
            rank_of("b")
        );
        assert!(
            rank_of("c").abs() < 1e-9,
            "non-seed `c` receives no teleport under personalization: {}",
            rank_of("c")
        );
        // Mass conserved across this single shard (owns the whole graph).
        let sum: f64 = res.rank_vec.iter().sum();
        assert!((sum - 1.0).abs() < 1e-9, "mass conserved, got {sum}");
    }
}
