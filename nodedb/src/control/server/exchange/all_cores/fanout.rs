// SPDX-License-Identifier: BUSL-1.1

//! Shared per-core fan-out primitive for graph BSP/WCC superstep plans and
//! single-blob Meta ops (tenant snapshot, restore result). Used by every
//! single-blob merge path (`dispatch::single_blob_gather`, `snapshot`, `bsp`,
//! `wcc`).

use futures::future::join_all;

use crate::bridge::envelope::{Response, Status};
use crate::control::server::exchange::gather::eager_dispatch_to_all_cores;
use crate::control::server::shared::session::statement_deadline;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId};
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};

/// Shared per-core fan for a BSP/WCC superstep plan: dispatch to every local
/// core, gather bounded responses, drop `NotFound`/empty-CSR cores.
///
/// Must scope `owned_vshards` to `vshard % num_cores == core_id`, or every core
/// claims sibling-homed nodes in its local CSR, duplicating them in the merge.
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

    // The running statement's deadline — the same instant the per-core
    // envelopes carry, so the Control-Plane wait and the Data-Plane execution
    // expire together.
    let deadline = statement_deadline(state.tuning.network.default_deadline_secs);
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;

    let num_cores = state
        .dispatcher
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .num_cores();

    // Eager dispatch: register + dispatch to each core before awaiting any response.
    // Scope owned_vshards to `vshard % num_cores == core_id` — see doc above.
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
                    // No per-core vShard set — fanned verbatim. Exhaustive (no `_ =>`) so a
                    // new superstep variant forces a scoping decision here.
                    GraphOp::Match { .. }
                    | GraphOp::MatchContinuation { .. }
                    | GraphOp::MatchVarLenResume { .. }
                    | GraphOp::EdgePut { .. }
                    | GraphOp::EdgePutBatch { .. }
                    | GraphOp::EdgeDelete { .. }
                    | GraphOp::EdgeDeleteBatch { .. }
                    | GraphOp::ResolveEdgeDelete(_)
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
                // Non-graph plans fanned verbatim. Exhaustive (no `_ =>`) to force a decision.
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

    let response_futures = receivers.into_iter().map(|(core_id, mut rx)| async move {
        match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
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
