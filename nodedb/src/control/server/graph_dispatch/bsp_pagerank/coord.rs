// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane coordinator for distributed BSP PageRank (F1d-4 Phase B).
//!
//! Drives the superstep loop with one dispatch per DISTINCT OWNER NODE (each
//! carrying that node's full owned-vShard set), NOT one dispatch per vShard,
//! using the Phase A `GraphOp::BspSuperstep` primitive; each node's dispatch is
//! then fanned across that node's cores below. The coordinator OWNS all durable
//! state — the per-node rank vectors and the routed cross-shard contributions;
//! [`BspCoordinator`] is used ONLY for convergence bookkeeping (`record_ack` /
//! `all_acked` / `global_delta` / `advance`).
//!
//! Each shard is one distinct owner node (the local node + each distinct
//! non-local data-group leader), carrying that node's FULL set of owned vShards.
//! This is one dispatch per node per superstep — not one per vShard — mirroring
//! `match_scatter`'s per-owner-node scatter; the handler ranks every node homed
//! on that owner in a single CSR pass (see `enumerate.rs` for the rationale and
//! the multi-core caveat).
//!
//! Phases:
//!
//! 1. **Count.** Dispatch one `BspSuperstep` with `global_n == 0` (the
//!    count-only sentinel) to every node. The handler short-circuits after
//!    building its owned-node set and returns just `vertex_count` + `node_names`.
//!    `global_n = Σ vertex_count` (each graph node is homed on exactly one owner
//!    node, so summing per-node owned counts counts each graph node once).
//! 2. **Superstep loop.** For `s = 0,1,2,…`: dispatch `BspSuperstep` to every
//!    node with `global_n`, the routed `incoming_contributions`, and that node's
//!    current `rank_vec`; collect each node's new `rank_vec` + `outbound` +
//!    `local_delta`. Record one `SuperstepAck` per node, then `advance()`; halt
//!    when the global delta drops below tolerance or `max_iterations` is reached.
//! 3. **Redistribute.** After each superstep, route every node's `outbound`
//!    `(target_vshard, dst_name, contrib)` to the node that OWNS `target_vshard`
//!    (resolved via the `vShard → owner node` map), accumulating into that
//!    node's `incoming_contributions` for the next superstep.
//! 4. **Assemble.** On halt, zip each node's final `rank_vec` with its
//!    `node_names`, concatenate across nodes (each owns a disjoint graph-node
//!    set, so no dedup is needed), and build an `AlgoResultBatch` serialized
//!    exactly like the single-node path so the client output is byte-identical.

use std::collections::HashMap;

use nodedb_cluster::distributed_graph::{BspCoordinator, SuperstepAck};
use nodedb_graph::{AlgoParams, GraphAlgorithm};

use crate::bridge::envelope::Payload;
use crate::control::state::SharedState;
use crate::engine::graph::algo::result::AlgoResultBatch;
use crate::types::{DatabaseId, TenantId};

use super::enumerate::enumerate_shards;
use super::scatter::{ScatterSuperstepParams, ShardDispatch, scatter_superstep};

/// Default max supersteps when the query carries no explicit `ITERATIONS`.
/// Mirrors the single-node PageRank default iteration budget.
const DEFAULT_MAX_ITERATIONS: u32 = 20;

/// Per-node rank state the coordinator owns across supersteps: the owned vShard
/// set (passed to the handler each superstep), the node names (positionally
/// aligned with `rank_vec`), and the current rank vector.
struct ShardRankState {
    is_local: bool,
    owned_vshards: Vec<u32>,
    route_vshard: u32,
    node_names: Vec<String>,
    rank_vec: Vec<f64>,
}

/// Run distributed BSP PageRank and return the bare `AlgoResultBatch` payload
/// (the exact shape `algo_payload_to_query_response` consumes — identical to the
/// single-node path).
///
/// Caller guarantees cluster mode (`cluster_routing.is_some()`) and
/// `algorithm == PageRank`; single-node / other algorithms never enter here.
pub async fn run_bsp_pagerank(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    params: AlgoParams,
    deadline_ms: u64,
) -> crate::Result<Payload> {
    let algorithm = GraphAlgorithm::PageRank;

    // ── Enumerate shards (one per distinct owner node, local + remote). ──
    let enumeration = enumerate_shards(state)?;
    let targets = enumeration.targets;
    // `vShard → owner node` map: routes each outbound contribution's
    // `target_vshard` to the node-shard that owns it during redistribution.
    let vshard_owner = enumeration.vshard_owner;
    if targets.is_empty() {
        // No data shards (single-node would not reach here; an empty cluster
        // routing table yields an empty result set).
        return empty_payload();
    }

    // Personalized-PageRank global seed sum: `Σ max(w, 0.0)` over the seed map.
    // The map already lives on the coordinator (no extra round trip). This is a
    // PRE-CONDITION for personalization being active; the final activation
    // decision also requires at least one seed name to exist somewhere in the
    // cluster graph (checked via the count phase's `seed_hits` below), matching
    // single-node `build_personalization` returning `None` for unknown seeds.
    let global_seed_sum: f64 = params
        .personalization_vector()
        .map(|seed| seed.values().map(|&w| w.max(0.0)).sum())
        .unwrap_or(0.0);

    let max_iterations = params
        .max_iterations
        .map(|m| m.clamp(1, u32::MAX as usize) as u32)
        .unwrap_or(DEFAULT_MAX_ITERATIONS);
    let tolerance = params.convergence_tolerance();
    // Convergence is keyed by owner node now; `BspCoordinator` only needs stable
    // shard ids, so node ids (cast to u32) serve as the per-shard ack keys.
    let shard_ids: Vec<u32> = targets.iter().map(|t| t.node_id as u32).collect();
    let mut bsp = BspCoordinator::new(
        algorithm.name().to_string(),
        max_iterations,
        tolerance,
        shard_ids,
    );

    // ── Phase 1: count. global_n = 0 sentinel → handler returns owned counts. ──
    let count_dispatches: Vec<ShardDispatch> = targets
        .iter()
        .map(|t| ShardDispatch {
            node_id: t.node_id,
            is_local: t.is_local,
            owned_vshards: t.owned_vshards.clone(),
            route_vshard: t.route_vshard(),
            incoming_contributions: Vec::new(),
            rank_seed: Vec::new(),
            global_dangling: 0.0, // count phase: no previous superstep dangling sums.
            personalization_sum: 0.0, // count phase runs no superstep — irrelevant here.
        })
        .collect();
    let counts = scatter_superstep(
        state,
        ScatterSuperstepParams {
            tenant_id,
            database_id,
            algorithm,
            params: &params,
            superstep: 0,
            global_n: 0, // count-only sentinel
            dispatches: count_dispatches,
            deadline_ms,
        },
    )
    .await?;

    // Each graph node is homed on exactly one owner node, so summing per-node
    // owned counts counts every graph node exactly once.
    let global_n: usize = counts.iter().map(|c| c.result.vertex_count).sum();
    if global_n == 0 {
        // No nodes anywhere — empty result (same as single-node empty CSR).
        return empty_payload();
    }

    // Cluster-wide count of owned nodes that are positively-weighted seed keys.
    // Combined with `global_seed_sum`, this is the exact single-node
    // `build_personalization` activation test: personalization is active iff a
    // seed map was supplied (`global_seed_sum > 0.0`) AND at least one seed name
    // exists somewhere in the cluster graph (`global_seed_hits > 0`). Otherwise
    // (unknown seed / empty / non-positive) every dispatch carries
    // `personalization_sum = 0.0` and the supersteps run UNIFORM PageRank.
    let global_seed_hits: usize = counts.iter().map(|c| c.result.seed_hits).sum();
    let personalization_sum = if global_seed_sum > 0.0 && global_seed_hits > 0 {
        global_seed_sum
    } else {
        0.0
    };

    // Seed per-node rank state from the count phase. `rank_vec` starts empty so
    // superstep 0 initializes each owned node to `1/global_n`.
    let mut shard_state: HashMap<u64, ShardRankState> = HashMap::with_capacity(targets.len());
    for (target, count) in targets.iter().zip(counts) {
        shard_state.insert(
            target.node_id,
            ShardRankState {
                is_local: target.is_local,
                owned_vshards: target.owned_vshards.clone(),
                route_vshard: target.route_vshard(),
                node_names: count.result.node_names,
                rank_vec: Vec::new(),
            },
        );
    }

    // Cross-shard contributions routed to each node for the NEXT superstep:
    // owner node id → Vec<(dst_name, contrib)>.
    let mut incoming: HashMap<u64, Vec<(String, f64)>> = HashMap::new();

    // Global dangling-node rank mass aggregated from all shards' previous
    // superstep. Starts at 0.0 before superstep 0 (no previous superstep exists),
    // which collapses the base to the plain teleport `(1−d)/n` — correct for
    // initialization. After each superstep the coordinator sums each shard's
    // returned `dangling_sum` here; the NEXT superstep's dispatches carry this
    // value so dangling mass redistributes globally, not just within the shard
    // that owns the dangling node.
    let mut global_dangling: f64 = 0.0;

    // ── Phase 2/3: superstep loop. ──
    let mut superstep: u32 = 0;
    loop {
        // Build this superstep's dispatches in a STABLE node order so the
        // results zip back deterministically.
        let mut ordered_nodes: Vec<u64> = shard_state.keys().copied().collect();
        ordered_nodes.sort_unstable();

        let dispatches: Vec<ShardDispatch> = ordered_nodes
            .iter()
            .map(|&node_id| {
                let st = &shard_state[&node_id];
                ShardDispatch {
                    node_id,
                    is_local: st.is_local,
                    owned_vshards: st.owned_vshards.clone(),
                    route_vshard: st.route_vshard,
                    incoming_contributions: incoming.remove(&node_id).unwrap_or_default(),
                    rank_seed: st
                        .node_names
                        .iter()
                        .cloned()
                        .zip(st.rank_vec.iter().copied())
                        .collect(),
                    // Pass the globally aggregated dangling mass from the PREVIOUS
                    // superstep. 0.0 on superstep 0 (no previous sums yet).
                    global_dangling,
                    // Cluster-wide PPR seed sum (0.0 = uniform). Constant across the
                    // whole run; each shard normalizes its owned seeds by it.
                    personalization_sum,
                }
            })
            .collect();

        let results = scatter_superstep(
            state,
            ScatterSuperstepParams {
                tenant_id,
                database_id,
                algorithm,
                params: &params,
                superstep,
                global_n,
                dispatches,
                deadline_ms,
            },
        )
        .await?;

        // Store new rank vectors + node names, record ACKs, route outbound, and
        // aggregate per-shard dangling sums into global_dangling for the NEXT step.
        incoming.clear();
        global_dangling = 0.0;
        for sr in results {
            let node_id = sr.node_id;
            let res = sr.result;

            bsp.record_ack(SuperstepAck {
                shard_id: node_id as u32,
                iteration: superstep + 1,
                local_delta: res.local_delta,
                vertex_count: res.vertex_count,
                contributions_sent: res.outbound.len(),
            });

            // Aggregate each node's local dangling mass into the global total for
            // the NEXT superstep. Each graph node is homed on exactly one owner
            // node, so summing per-node dangling sums counts every dangling node
            // exactly once.
            global_dangling += res.dangling_sum;

            // Route this node's outbound contributions to the node that OWNS the
            // target vShard. An unmapped target vShard is a routing
            // inconsistency — surface it rather than silently dropping mass.
            for (target_vshard, dst_name, contrib) in res.outbound {
                let Some(&owner) = vshard_owner.get(&target_vshard) else {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "bsp pagerank: outbound contribution to unmapped target \
                             vshard={target_vshard} (dst={dst_name})"
                        ),
                    });
                };
                if !shard_state.contains_key(&owner) {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "bsp pagerank: outbound contribution to unknown owner \
                             node={owner} for vshard={target_vshard} (dst={dst_name})"
                        ),
                    });
                }
                incoming.entry(owner).or_default().push((dst_name, contrib));
            }

            if let Some(st) = shard_state.get_mut(&node_id) {
                st.node_names = res.node_names;
                st.rank_vec = res.rank_vec;
            }
        }

        // Convergence bookkeeping. All shards always ACK (one result each).
        if !bsp.all_acked() {
            return Err(crate::Error::Internal {
                detail: "bsp pagerank: not all shards acked after superstep dispatch".into(),
            });
        }
        if !bsp.advance() {
            break;
        }
        superstep += 1;
    }

    // ── Phase 4: assemble final AlgoResultBatch (single-node-identical shape). ──
    assemble_result(&shard_state)
}

/// Concatenate every node's `(node_name, rank)` into an `AlgoResultBatch` using
/// the same `push_node_f64` + `to_msgpack` seam as single-node PageRank, so
/// `algo_payload_to_query_response` produces byte-identical client output. Each
/// owner node holds a disjoint graph-node set, so no dedup is required.
fn assemble_result(shard_state: &HashMap<u64, ShardRankState>) -> crate::Result<Payload> {
    let mut batch = AlgoResultBatch::new(GraphAlgorithm::PageRank);
    // Deterministic node order for a stable row order across runs.
    let mut ordered: Vec<u64> = shard_state.keys().copied().collect();
    ordered.sort_unstable();
    for node_id in ordered {
        let st = &shard_state[&node_id];
        for (name, rank) in st.node_names.iter().zip(st.rank_vec.iter()) {
            batch.push_node_f64(name.clone(), *rank);
        }
    }
    let bytes = batch.to_msgpack()?;
    Ok(Payload::from_vec(bytes))
}

/// An empty PageRank result encoded the same way the single-node empty-CSR path
/// encodes it (`AlgoResultBatch::new(...).to_msgpack()`).
fn empty_payload() -> crate::Result<Payload> {
    let bytes = AlgoResultBatch::new(GraphAlgorithm::PageRank).to_msgpack()?;
    Ok(Payload::from_vec(bytes))
}
