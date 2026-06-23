// SPDX-License-Identifier: BUSL-1.1

//! Round-loop continuation dispatch: one round of pending continuations,
//! grouped by target shard, issued concurrently.

use std::collections::HashMap;

use futures::future::join_all;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::dispatcher::dispatch_route;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::graph_dispatch::match_broadcast::broadcast_match_to_all_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_cluster::distributed_graph::PatternContinuation;
use nodedb_physical::physical_plan::GraphOp;

use crate::control::server::graph_dispatch::cluster_resolve::{gateway_shared, resolve_for_vshard};

use super::coord::{TaggedShardResult, decode_rows};
use super::round_zero::collect_remote_envelopes;

/// Dispatch one round of pending continuations, grouped by target shard.
pub(super) async fn dispatch_continuations(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    query_bytes: &[u8],
    deadline_ms: u64,
    pending: HashMap<u32, Vec<PatternContinuation>>,
) -> crate::Result<Vec<TaggedShardResult>> {
    let shared_arc = gateway_shared(state)?;
    let version_set = GatewayVersionSet::from_pairs(Vec::new());

    // One dispatch future per continuation. Local and remote arms produce
    // distinct concrete future types, so the boxed `dyn Future` keeps the
    // collection homogeneous for `join_all`.
    type ContFut<'f> = std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::Result<Vec<TaggedShardResult>>> + Send + 'f>,
    >;
    let mut futs: Vec<ContFut<'_>> = Vec::new();
    for (target_shard, conts) in pending {
        // Resolve once per target shard, not once per continuation: every
        // continuation targeting the same vShard gets the same routing
        // decision, and `resolve_for_vshard` acquires a routing-table read
        // lock on each call.
        let decision = resolve_for_vshard(state, target_shard);

        // Error arms do not depend on the individual continuation; handle
        // them at the outer (per-shard) level so the error fires once rather
        // than on the first iteration of the inner loop.
        //
        // Extract the remote node coordinates (Copy-able u64 fields) so the
        // inner loop can reuse them without re-acquiring the routing lock.
        // `None` means Local.
        let remote_coords: Option<(u64, u64)> = match decision {
            RouteDecision::LeaderUnknown { vshard_id } => {
                return Err(crate::Error::NotLeader {
                    vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
                    leader_node: 0,
                    leader_addr: String::new(),
                });
            }
            RouteDecision::Broadcast { .. } => {
                return Err(crate::Error::Internal {
                    detail: "match scatter: resolve_decision returned Broadcast \
                             for a single vShard"
                        .into(),
                });
            }
            RouteDecision::Local => None,
            RouteDecision::Remote { node_id, vshard_id } => Some((node_id, vshard_id)),
        };

        for cont in conts {
            let partial_row = zerompk::to_msgpack_vec(&cont.bindings).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("continuation partial_row: {e}"),
                }
            })?;
            let plan = PhysicalPlan::Graph(GraphOp::MatchContinuation {
                query: query_bytes.to_vec(),
                resume_triple_idx: cont.next_triple_idx,
                partial_row,
                source_node: cont.start_node.clone(),
                source_binding: cont.start_binding.clone(),
            });
            match remote_coords {
                None => {
                    // Local: fan the continuation to all local cores and
                    // unwrap each `{rows, frontier}` envelope.
                    futs.push(Box::pin(async move {
                        let outcome = broadcast_match_to_all_cores(
                            state,
                            tenant_id,
                            database_id,
                            plan,
                            TraceId::ZERO,
                        )
                        .await?;
                        Ok::<_, crate::Error>(vec![TaggedShardResult {
                            emitting_node: state.node_id,
                            rows: decode_rows(&outcome.rows_payload)?,
                            frontier: outcome.frontier,
                            truncated: outcome.partial,
                        }])
                    }));
                }
                Some((node_id, vshard_id)) => {
                    let route = TaskRoute {
                        plan,
                        decision: RouteDecision::Remote { node_id, vshard_id },
                        vshard_id: (vshard_id % VShardId::COUNT as u64) as u32,
                    };
                    let version_set = version_set.clone();
                    futs.push(Box::pin(async move {
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
                        collect_remote_envelopes(node_id, payloads)
                    }));
                }
            }
        }
    }

    let results = join_all(futs).await;
    let mut out = Vec::new();
    for res in results {
        out.extend(res?);
    }
    Ok(out)
}
