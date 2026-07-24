// SPDX-License-Identifier: BUSL-1.1

//! Shared per-core fan-out primitive for graph BSP/WCC superstep plans and
//! single-blob Meta ops (tenant snapshot, restore result). Used by every
//! single-blob merge path (`dispatch::single_blob_gather`, `snapshot`, `bsp`,
//! `wcc`).

use std::time::Duration;

use futures::future::join_all;

use crate::bridge::envelope::{Response, Status};
use crate::control::server::exchange::gather::eager_dispatch_to_all_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId};
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};

/// Shared per-core fan for a graph BSP/WCC superstep plan: eagerly dispatch the
/// plan to every local core (scoping each core's `owned_vshards` to the vShards
/// round-robin homed on that core), gather the bounded responses, drop
/// `NotFound`/empty-CSR cores, and return the successful [`Response`]s for the
/// caller to decode and merge.
///
/// CRITICAL: scope each core's `owned_vshards` to the vShards round-robin homed
/// on THAT core (`vshard % num_cores == core_id`, mirroring
/// `VShardRouter::round_robin`). The plan arrives carrying the NODE's full
/// owned-vShard set; if every core received the full set, each core would claim
/// ownership of any node appearing in its local CSR — including nodes physically
/// homed on a SIBLING core (they appear as cross-core edge endpoints). That node
/// would then be emitted by two cores, duplicating it in the merged result.
/// Per-core scoping makes the owned sets genuinely disjoint (each graph node is
/// owned by exactly its home core), so the field-concat merge is correct with no
/// dedup, and cross-core edges become ordinary ghosts / boundary edges.
/// `txn_id` is stamped on every core's request so a session-transaction-scoped
/// single-blob op (a forwarded `MetaOp::StageWrite` / `MetaOp::DropTxnOverlay`,
/// which the leader's Data-Plane handler keys purely by `txn_id`) reaches its
/// per-transaction overlay. `None` for the graph BSP/WCC/snapshot fans, which
/// carry no transaction context.
pub(super) async fn gather_graph_op_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
    label: &'static str,
) -> crate::Result<Vec<Response>> {
    // Shared broadcast call counter (parity with gather_all_cores).
    crate::control::server::broadcast::broadcast_call_count_increment();

    let deadline_secs = state.tuning.network.default_deadline_secs;
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;

    let num_cores = state
        .dispatcher
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .num_cores();

    // Eager dispatch: register a tracker receiver and dispatch to each core
    // BEFORE awaiting any response, matching gather_all_cores' true-parallelism
    // prologue.
    // CRITICAL: scope each core's `owned_vshards` to the vShards round-robin homed
    // on THAT core (`vshard % num_cores == core_id`) so owned sets are genuinely
    // disjoint across cores. See function-level doc for details.
    let receivers =
        eager_dispatch_to_all_cores(state, tenant_id, database_id, trace_id, txn_id, |core_id| {
            let mut core_plan = plan.clone();
            match &mut core_plan {
                PhysicalPlan::Graph(g) => match g {
                    GraphOp::BspSuperstep(bsp) => {
                        bsp.owned_vshards
                            .retain(|v| (*v as usize) % num_cores == core_id);
                    }
                    GraphOp::WccSuperstep(wcc) => {
                        wcc.owned_vshards
                            .retain(|v| (*v as usize) % num_cores == core_id);
                    }
                    // All other graph ops carry no per-core-owned vShard set —
                    // fanned verbatim. Enumerated exhaustively (no `_ =>`) so a new
                    // graph-superstep variant forces a compile error here and the
                    // developer must decide whether it needs per-core scoping.
                    GraphOp::Match { .. }
                    | GraphOp::MatchContinuation { .. }
                    | GraphOp::MatchVarLenResume { .. }
                    | GraphOp::EdgePut { .. }
                    | GraphOp::EdgePutBatch { .. }
                    | GraphOp::EdgeDelete { .. }
                    | GraphOp::EdgeDeleteBatch { .. }
                    | GraphOp::Hop { .. }
                    | GraphOp::Neighbors { .. }
                    | GraphOp::NeighborsMulti { .. }
                    | GraphOp::Path { .. }
                    | GraphOp::Subgraph { .. }
                    | GraphOp::RagFusion { .. }
                    | GraphOp::Algo { .. }
                    | GraphOp::SetNodeLabels { .. }
                    | GraphOp::RemoveNodeLabels { .. }
                    | GraphOp::TemporalNeighbors { .. }
                    | GraphOp::TemporalAlgorithm { .. }
                    | GraphOp::Stats { .. } => {}
                },
                // All non-graph plans are fanned verbatim (no per-core-owned vShard
                // field). Enumerated exhaustively (no `_ =>`) so a new PhysicalPlan
                // variant forces a compile error here.
                PhysicalPlan::Vector(_)
                | PhysicalPlan::Document(_)
                | PhysicalPlan::Kv(_)
                | PhysicalPlan::Text(_)
                | PhysicalPlan::Columnar(_)
                | PhysicalPlan::Timeseries(_)
                | PhysicalPlan::Spatial(_)
                | PhysicalPlan::Crdt(_)
                | PhysicalPlan::Query(_)
                | PhysicalPlan::Meta(_)
                | PhysicalPlan::Array(_)
                | PhysicalPlan::ClusterArray(_)
                | PhysicalPlan::ClusterEvent(_) => {}
            }
            core_plan
        })?;

    let deadline = Duration::from_secs(deadline_secs);
    let response_futures = receivers.into_iter().map(|(core_id, mut rx)| async move {
        match tokio::time::timeout(
            deadline,
            crate::control::server::dispatch_utils::collect_bounded_response(
                &mut rx,
                max_result_bytes,
            ),
        )
        .await
        .map_err(|_| crate::Error::Dispatch {
            detail: format!("{label} gather timeout on core {core_id}"),
        })? {
            Ok(resp) => Ok(resp),
            Err(crate::control::server::dispatch_utils::DispatchCollectError::OverBudget {
                bytes,
            }) => Err(crate::Error::ExecutionLimitExceeded {
                detail: format!(
                    "{label} gather on core {core_id} exceeded max_query_result_bytes \
                     ({bytes} > {max_result_bytes} bytes)"
                ),
            }),
            Err(crate::control::server::dispatch_utils::DispatchCollectError::ChannelClosed) => {
                Err(crate::Error::Dispatch {
                    detail: format!("{label} gather channel closed on core {core_id}"),
                })
            }
        }
    });

    let results: Vec<crate::Result<Response>> = join_all(response_futures).await;

    let mut out = Vec::with_capacity(num_cores);
    let mut had_error = false;
    let mut error_msg = String::new();

    for result in results {
        let resp = match result {
            Ok(r) => r,
            Err(e) => {
                had_error = true;
                error_msg = e.to_string();
                continue;
            }
        };

        if resp.status == Status::Error {
            if let Some(ec) = resp.error_code.as_deref() {
                match ec {
                    crate::bridge::envelope::ErrorCode::NotFound => continue,
                    _ => {
                        had_error = true;
                        error_msg = format!("{ec:?}");
                    }
                }
            }
            continue;
        }

        out.push(resp);
    }

    if had_error && out.is_empty() {
        return Err(crate::Error::Dispatch { detail: error_msg });
    }

    Ok(out)
}
