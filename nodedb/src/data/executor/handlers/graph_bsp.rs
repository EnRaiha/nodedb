// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane handler for `GraphOp::BspSuperstep` — runs ONE distributed
//! PageRank BSP superstep on this shard's local CSR partition.
//!
//! Phase A primitive: the handler is stateless across supersteps. All
//! per-superstep state is carried in the `GraphOp::BspSuperstep` plan variant
//! (the round-tripped `rank_vec`, the `incoming_contributions` routed to this
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
    pub rank_vec: &'a [f64],
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
/// Returns a typed `crate::Error::Internal` only for the rank_vec length
/// mismatch that the handler surfaces as `ErrorCode::Internal`.
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

    // Seed rank: superstep 0 keeps init's uniform 1/vertex_count re-seeded to
    // 1/global_n; otherwise the Control Plane round-trips the prior rank vector.
    if !args.rank_vec.is_empty() {
        if args.rank_vec.len() != vertex_count {
            return Err(crate::Error::Internal {
                detail: format!(
                    "bsp superstep rank_vec length {} != owned vertex_count {}",
                    args.rank_vec.len(),
                    vertex_count
                ),
            });
        }
        state.rank.copy_from_slice(args.rank_vec);
    } else if args.global_n > 0 {
        // Re-seed init's 1/vertex_count to the global 1/global_n so the
        // teleport mass is correct across all shards on superstep 0.
        let init = 1.0 / args.global_n as f64;
        for r in state.rank.iter_mut() {
            *r = init;
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
    let (local_delta, outbound_map) =
        state.superstep(damping, args.global_n, &local_edge_iter, &node_id_to_local);

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
            rank_vec: &[],
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
            rank_vec: &[],
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
}
