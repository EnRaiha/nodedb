// SPDX-License-Identifier: BUSL-1.1

//! Per-node `BspSuperstep` dispatch: one plan per distinct owner node, all
//! issued concurrently via `join_all`, decoding each node's
//! `BspSuperstepResult`.
//!
//! Each dispatch carries the owner node's FULL `owned_vshards` set so the
//! handler ranks every node homed on that owner in a single CSR pass. A remote
//! node gets one `RouteDecision::Remote` dispatch; the local node gets ONE
//! single-core `RouteDecision::Local` dispatch (NOT a broadcast to every local
//! core — that would re-run the superstep on each core's CSR and double-count
//! PageRank contributions per core).

use futures::future::join_all;

use crate::bridge::envelope::{Payload, PhysicalPlan};
use crate::control::gateway::dispatcher::dispatch_route;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::graph_dispatch::cluster_resolve::gateway_shared;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_graph::{AlgoParams, GraphAlgorithm};
use nodedb_physical::physical_plan::{BspSuperstepPlan, BspSuperstepResult, GraphOp};

/// Inputs for one node's superstep, paired with its target.
pub(super) struct ShardDispatch {
    /// Owner node id — the stable per-shard key returned in [`ShardResult`].
    pub(super) node_id: u64,
    /// `true` if this node is the coordinating node (dispatch local, single core).
    pub(super) is_local: bool,
    /// The FULL set of vShards this node owns — passed verbatim as the plan's
    /// `owned_vshards` so the handler ranks every node homed on this owner.
    pub(super) owned_vshards: Vec<u32>,
    /// One of this node's vShards, used as the remote route's `vshard_id` (any
    /// one of the node's vShards selects the same node).
    pub(super) route_vshard: u32,
    /// Cross-shard contributions routed to THIS node's owned nodes this
    /// superstep (empty on the count phase and superstep 0).
    pub(super) incoming_contributions: Vec<(String, f64)>,
    /// This node's current rank vector (empty on the count phase and on
    /// superstep 0 — the handler seeds `1/global_n`).
    pub(super) rank_vec: Vec<f64>,
}

/// One node's decoded superstep result, tagged with its owner node id.
pub(super) struct ShardResult {
    pub(super) node_id: u64,
    pub(super) result: BspSuperstepResult,
}

/// Dispatch one `BspSuperstep` to every owner node concurrently and decode each
/// node's [`BspSuperstepResult`]. `global_n == 0` is the count-only phase
/// (handler short-circuits after counting owned nodes).
#[allow(clippy::too_many_arguments)]
pub(super) async fn scatter_superstep(
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
    superstep: u32,
    global_n: usize,
    dispatches: Vec<ShardDispatch>,
    deadline_ms: u64,
) -> crate::Result<Vec<ShardResult>> {
    let shared_arc = gateway_shared(state)?;
    let version_set = GatewayVersionSet::from_pairs(Vec::new());

    let futs = dispatches.into_iter().map(|d| {
        let plan = PhysicalPlan::Graph(GraphOp::BspSuperstep(Box::new(BspSuperstepPlan {
            algorithm,
            params: params.clone(),
            superstep,
            global_n,
            // FULL owned set for this node — the handler ranks every node homed
            // here in one pass and emits ghosts only for dsts on OTHER nodes.
            owned_vshards: d.owned_vshards.clone(),
            incoming_contributions: d.incoming_contributions,
            rank_vec: d.rank_vec,
        })));
        // Local node → ONE single-core local dispatch (NOT broadcast-to-all-cores,
        // which would double-count contributions per core). Remote node → one
        // remote dispatch carrying any one of the node's vShards as the route key.
        let (decision, route_vshard) = if d.is_local {
            (RouteDecision::Local, d.route_vshard)
        } else {
            (
                RouteDecision::Remote {
                    node_id: d.node_id,
                    vshard_id: d.route_vshard as u64,
                },
                d.route_vshard,
            )
        };
        let route = TaskRoute {
            plan,
            decision,
            vshard_id: route_vshard,
        };
        let version_set = version_set.clone();
        let node_id = d.node_id;
        Box::pin(async move {
            let payloads = dispatch_route(
                route,
                shared_arc,
                tenant_id,
                database_id,
                TraceId::ZERO,
                deadline_ms,
                &version_set,
            )
            .await?;
            let result = decode_single_result(node_id, payloads)?;
            Ok::<ShardResult, crate::Error>(ShardResult { node_id, result })
        })
    });

    let results = join_all(futs).await;
    let mut out = Vec::with_capacity(results.len());
    for res in results {
        out.push(res?);
    }
    Ok(out)
}

/// Decode a single node's `BspSuperstepResult` from a dispatch's payload list.
///
/// Both the local single-core dispatch and the remote `ExecuteRequest` (which
/// runs on one core) yield exactly one payload. An empty CSR on that core yields
/// an empty (default) payload — the handler encodes `BspSuperstepResult::default()`,
/// which decodes to a zero-vertex shard (it contributes nothing to `global_n` or
/// the ranks).
fn decode_single_result(node_id: u64, payloads: Vec<Vec<u8>>) -> crate::Result<BspSuperstepResult> {
    let payload = payloads
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::Internal {
            detail: format!("bsp pagerank: node={node_id} returned no payload"),
        })?;
    let payload = Payload::from_vec(payload);
    if payload.is_empty() {
        return Ok(BspSuperstepResult::default());
    }
    zerompk::from_msgpack::<BspSuperstepResult>(payload.as_ref()).map_err(|e| crate::Error::Codec {
        detail: format!("bsp pagerank: node={node_id} result decode: {e}"),
    })
}
