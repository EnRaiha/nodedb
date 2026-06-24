// SPDX-License-Identifier: BUSL-1.1

//! Universal node-level "fan to all local cores and merge" primitive.
//!
//! `execute_plan_all_local_cores` is the canonical way to execute a
//! [`PhysicalPlan`] on THIS node and obtain a single merged payload in exactly
//! the same shape a single core's handler produces.  It is called:
//!
//! - by the remote `ExecuteRequest` receiver (`exec_receiver/executor.rs`) so
//!   that an inbound plan from another node is transparently fanned across all
//!   local cores before the merged result is returned,
//! - by the local BSP scatter path (`bsp_pagerank/scatter.rs`) so the
//!   coordinator's own node is treated identically to every remote node.
//!
//! At 1 core/node the fan is over a single core and every path is
//! behaviour-identical to the prior single-core dispatch.

use futures::future::join_all;
use std::time::Duration;

use crate::bridge::envelope::{Response, Status};
use crate::control::server::exchange::gather::eager_dispatch_to_all_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};
use nodedb_physical::physical_plan::{
    BspSuperstepResult, GraphOp, PhysicalPlan, WccSuperstepResult,
};

/// The canonical node-level result of fanning a plan across all local cores
/// and merging into the SAME payload shape a single core produces.
pub struct NodeLevelResult {
    pub payload: Vec<u8>,
    pub watermark_lsn: Lsn,
}

/// Fan `plan` across all local Data-Plane cores, merge per-core payloads, and
/// return a [`NodeLevelResult`] in the same shape the plan's single-core handler
/// produces.
///
/// Dispatch semantics are plan-dependent:
///
/// - **MATCH / MatchContinuation**: calls [`broadcast_match_to_all_cores`] and
///   re-encodes the `{rows, frontier}` envelope so the caller receives exactly
///   the shape a single-core MATCH handler returns.
/// - **BspSuperstep**: fans to all cores via the generic `gather_all_cores`
///   prologue, decodes each core's [`BspSuperstepResult`], merges them by field
///   concatenation (owned-node sets are disjoint across cores), and re-encodes
///   the merged result.
/// - **Everything else**: delegates to [`gather_all_cores`] and wraps the
///   `merged_array` payload.
///
/// At 1 core/node every branch is behaviour-identical to the prior single-core
/// paths.
pub async fn execute_plan_all_local_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    match &plan {
        PhysicalPlan::Graph(g) => match g {
            // ── MATCH / MatchContinuation ─────────────────────────────────────
            GraphOp::Match { .. } | GraphOp::MatchContinuation { .. } => {
                use crate::control::server::graph_dispatch::match_broadcast::broadcast_match_to_all_cores;
                use crate::data::executor::handlers::graph_match::encode_match_envelope_raw;

                let outcome =
                    broadcast_match_to_all_cores(state, tenant_id, database_id, plan, trace_id)
                        .await?;

                let envelope =
                    encode_match_envelope_raw(outcome.rows_payload.as_ref(), &outcome.frontier)?;

                Ok(NodeLevelResult {
                    payload: envelope,
                    watermark_lsn: Lsn::ZERO,
                })
            }

            // ── BspSuperstep ─────────────────────────────────────────────────
            GraphOp::BspSuperstep(_) => {
                fan_bsp_all_cores(state, tenant_id, database_id, plan, trace_id).await
            }

            // ── WccSuperstep ─────────────────────────────────────────────────
            GraphOp::WccSuperstep(_) => {
                fan_wcc_all_cores(state, tenant_id, database_id, plan, trace_id).await
            }

            // ── All other GraphOp variants → generic gather ───────────────────
            GraphOp::EdgePut { .. }
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
            | GraphOp::Stats { .. } => {
                generic_gather(state, tenant_id, database_id, plan, trace_id).await
            }
        },

        // ── All other PhysicalPlan variants → generic gather ──────────────────
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
        | PhysicalPlan::ClusterArray(_) => {
            generic_gather(state, tenant_id, database_id, plan, trace_id).await
        }
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Generic gather path: delegate to [`gather_all_cores`] and wrap.
async fn generic_gather(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    use crate::control::server::exchange::gather::gather_all_cores;

    let outcome = gather_all_cores(state, tenant_id, database_id, plan, trace_id).await?;
    Ok(NodeLevelResult {
        payload: outcome.merged_array,
        watermark_lsn: outcome.watermark_lsn,
    })
}

/// BSP superstep fan: dispatch to all local cores, decode each core's
/// [`BspSuperstepResult`], merge by field concatenation, and re-encode.
///
/// Owned-node sets are disjoint across cores (each graph node is homed on
/// exactly one core via `VShardId::from_key`), so concatenation requires no
/// dedup.
async fn fan_bsp_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    let responses =
        gather_graph_op_all_cores(state, tenant_id, database_id, plan, trace_id, "bsp").await?;

    let mut parts: Vec<BspSuperstepResult> = Vec::with_capacity(responses.len());
    for resp in responses {
        // An empty payload decodes to BspSuperstepResult::default() (a
        // zero-vertex shard — contributes nothing to global_n or the ranks),
        // matching decode_single_result's contract.
        let part = if resp.payload.is_empty() {
            BspSuperstepResult::default()
        } else {
            zerompk::from_msgpack::<BspSuperstepResult>(resp.payload.as_ref()).map_err(|e| {
                crate::Error::Codec {
                    detail: format!("bsp gather: result decode: {e}"),
                }
            })?
        };
        parts.push(part);
    }

    let merged = merge_bsp_results(parts);
    let payload = zerompk::to_msgpack_vec(&merged).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("bsp gather: merged result encode: {e}"),
    })?;

    Ok(NodeLevelResult {
        payload,
        watermark_lsn: Lsn::ZERO,
    })
}

/// WCC contraction-round fan: dispatch to all local cores, decode each core's
/// [`WccSuperstepResult`], merge by field concatenation, and re-encode.
///
/// Owned-node sets are disjoint across cores (each graph node is homed on
/// exactly one core via `VShardId::from_key`), so concatenation requires no
/// dedup. Cross-core edges become ordinary boundary edges (the destination is
/// owned by a sibling core) and are stitched globally by the coordinator.
async fn fan_wcc_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    let responses =
        gather_graph_op_all_cores(state, tenant_id, database_id, plan, trace_id, "wcc").await?;

    let mut parts: Vec<WccSuperstepResult> = Vec::with_capacity(responses.len());
    for resp in responses {
        // An empty payload decodes to WccSuperstepResult::default() (a
        // zero-vertex shard — contributes no labels or boundary edges).
        let part = if resp.payload.is_empty() {
            WccSuperstepResult::default()
        } else {
            zerompk::from_msgpack::<WccSuperstepResult>(resp.payload.as_ref()).map_err(|e| {
                crate::Error::Codec {
                    detail: format!("wcc gather: result decode: {e}"),
                }
            })?
        };
        parts.push(part);
    }

    let merged = merge_wcc_results(parts);
    let payload = zerompk::to_msgpack_vec(&merged).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wcc gather: merged result encode: {e}"),
    })?;

    Ok(NodeLevelResult {
        payload,
        watermark_lsn: Lsn::ZERO,
    })
}

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
async fn gather_graph_op_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
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
        eager_dispatch_to_all_cores(state, tenant_id, database_id, trace_id, |core_id| {
            let mut core_plan = plan.clone();
            match &mut core_plan {
                PhysicalPlan::Graph(GraphOp::BspSuperstep(bsp)) => {
                    bsp.owned_vshards
                        .retain(|v| (*v as usize) % num_cores == core_id);
                }
                PhysicalPlan::Graph(GraphOp::WccSuperstep(wcc)) => {
                    wcc.owned_vshards
                        .retain(|v| (*v as usize) % num_cores == core_id);
                }
                // Caller only invokes this for BSP/WCC plans; any other plan is fanned
                // verbatim (no per-core scoping field exists for it).
                _ => {}
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
            if let Some(ref ec) = resp.error_code {
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

/// Merge per-core [`BspSuperstepResult`] parts by field concatenation.
///
/// Owned-node sets are DISJOINT across cores because `fan_bsp_all_cores` scopes
/// each core's `owned_vshards` to the vShards homed on that core, so each graph
/// node is owned by exactly one core. Concatenation therefore requires no dedup.
fn merge_bsp_results(parts: Vec<BspSuperstepResult>) -> BspSuperstepResult {
    let mut out = BspSuperstepResult::default();
    for p in parts {
        out.local_delta += p.local_delta;
        out.vertex_count += p.vertex_count;
        out.outbound.extend(p.outbound);
        out.node_names.extend(p.node_names);
        out.rank_vec.extend(p.rank_vec);
        // Owned-node sets are DISJOINT across cores (each graph node is homed on
        // exactly one core), so summing per-core dangling sums counts every
        // dangling node exactly once.
        out.dangling_sum += p.dangling_sum;
    }
    out
}

/// Merge per-core [`WccSuperstepResult`] parts by field concatenation.
///
/// Owned-node sets are DISJOINT across cores because `gather_graph_op_all_cores`
/// scopes each core's `owned_vshards` to the vShards homed on that core, so each
/// graph node is owned by exactly one core. Concatenation therefore requires no
/// dedup; cross-core edges already appear as boundary edges (their destination
/// is owned by a sibling core) and are stitched globally by the coordinator.
fn merge_wcc_results(parts: Vec<WccSuperstepResult>) -> WccSuperstepResult {
    let mut out = WccSuperstepResult::default();
    for p in parts {
        out.vertex_count += p.vertex_count;
        out.node_labels.extend(p.node_labels);
        out.boundary_edges.extend(p.boundary_edges);
    }
    out
}
