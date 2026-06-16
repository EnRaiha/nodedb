// SPDX-License-Identifier: BUSL-1.1

//! Per-route dispatch: local SPSC or remote `ExecuteRequest` RPC.
//!
//! The dispatcher takes a single [`TaskRoute`] and executes it:
//!
//! - `RouteDecision::Local` → dispatch through the SPSC bridge via
//!   [`dispatch_to_data_plane`].
//! - `RouteDecision::Remote { node_id, .. }` → encode the plan as
//!   [`ExecuteRequest`] bytes and send via [`NexarTransport::send_rpc`].
//! - `RouteDecision::Broadcast { .. }` → each individual route in the
//!   broadcast list is already split into Local/Remote routes by the router,
//!   so by the time dispatch runs, each element is a concrete Local or Remote.
//!
//! Returns `Vec<u8>` payloads — raw Data Plane response bytes that the fuser
//! can merge.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::rpc_codec::{ExecuteRequest, RaftRpc, TypedClusterError};
use tracing::debug;

use crate::Error;
use crate::control::server::dispatch_utils::dispatch_to_data_plane;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::wire as plan_wire;

use super::route::{RouteDecision, TaskRoute};
use super::version_set::GatewayVersionSet;

/// Dispatch a single route and return the raw payload bytes.
///
/// `tenant_id` — the authenticated tenant for this query.
/// `trace_id` — distributed trace ID propagated from the client request.
/// `deadline_ms` — remaining deadline in milliseconds.
/// `version_set` — descriptor versions for the collections touched by the plan.
pub async fn dispatch_route(
    route: TaskRoute,
    shared: &Arc<SharedState>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
    deadline_ms: u64,
    version_set: &GatewayVersionSet,
) -> Result<Vec<Vec<u8>>, Error> {
    match route.decision {
        RouteDecision::Local => dispatch_local(route, shared, tenant_id, trace_id).await,
        RouteDecision::Remote { node_id, vshard_id } => {
            dispatch_remote(RemoteDispatchArgs {
                plan: route.plan,
                shared,
                node_id,
                vshard_id,
                tenant_id,
                database_id,
                trace_id,
                deadline_ms,
                version_set,
            })
            .await
        }
        RouteDecision::Broadcast { .. } => {
            // Broadcast routes are split into individual Local/Remote routes
            // by the router before dispatch. This arm should not be reached.
            Err(Error::Internal {
                detail: "dispatcher: Broadcast route reached dispatch — should have been split"
                    .into(),
            })
        }
        RouteDecision::LeaderUnknown { vshard_id } => {
            // Cluster mode with no leader currently known for this vShard.
            // Surface as NotLeader so the gateway retry loop sleeps and
            // re-resolves the routing table on the next attempt — never
            // silently serve from a possibly-stale local replica.
            Err(Error::NotLeader {
                vshard_id: VShardId::new(vshard_id as u32),
                leader_node: 0,
                leader_addr: String::new(),
            })
        }
    }
}

/// Local dispatch via SPSC bridge.
async fn dispatch_local(
    route: TaskRoute,
    shared: &Arc<SharedState>,
    tenant_id: TenantId,
    trace_id: TraceId,
) -> Result<Vec<Vec<u8>>, Error> {
    let vshard_id = VShardId::new(route.vshard_id);
    let resp = dispatch_to_data_plane(shared, tenant_id, vshard_id, route.plan, trace_id).await?;
    Ok(vec![resp.payload.to_vec()])
}

/// Arguments for a remote dispatch call (bundles the 8 parameters to stay
/// within clippy's `too_many_arguments` limit).
struct RemoteDispatchArgs<'a> {
    plan: nodedb_physical::physical_plan::PhysicalPlan,
    shared: &'a Arc<SharedState>,
    node_id: u64,
    vshard_id: u64,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
    deadline_ms: u64,
    version_set: &'a GatewayVersionSet,
}

/// Remote dispatch via `ExecuteRequest` RPC.
async fn dispatch_remote(args: RemoteDispatchArgs<'_>) -> Result<Vec<Vec<u8>>, Error> {
    let RemoteDispatchArgs {
        plan,
        shared,
        node_id,
        vshard_id,
        tenant_id,
        database_id,
        trace_id,
        deadline_ms,
        version_set,
    } = args;
    let transport = shared.cluster_transport.as_ref().ok_or(Error::Internal {
        detail: "gateway: cluster transport not available for remote dispatch".into(),
    })?;

    // Resolve any Exchange data-movement nodes BEFORE shipping to the remote
    // node. A Data-Plane core rejects any plan still containing an Exchange, so
    // the coordinator must gather/embed cross-node data here — symmetric with
    // the local path (`dispatch_local` → `dispatch_to_data_plane_with_source`,
    // which already resolves). A self-contained plan (no Exchange) is a no-op.
    // `resolve_exchange_in_plan` is identity-free; catalog materialization is
    // already done upstream on the pgwire/native paths that own the identity.
    // (`Box::pin` breaks the async-recursion cycle: resolving a Broadcast build
    // side calls `gather_all_vshards` → `gateway.execute` → routing → here.)
    let plan = match Box::pin(crate::control::server::exchange::resolve_exchange_in_plan(
        shared,
        database_id,
        tenant_id,
        plan,
        trace_id,
    ))
    .await?
    {
        // A root-level Gather resolved entirely at the coordinator — its merged
        // response is ready; return it instead of shipping anything.
        crate::control::server::exchange::Resolved::Gathered(resp) => {
            return Ok(vec![resp.payload.to_vec()]);
        }
        crate::control::server::exchange::Resolved::Plan(p) => p,
        // Gateway path returns collected bytes: materialize the stream into one
        // merged-array payload. (Single-node streaming never reaches the gateway
        // — `state.gateway.is_none()` gates the Stream branch — but handle it
        // exhaustively and behaviour-preservingly regardless.)
        crate::control::server::exchange::Resolved::Stream(s) => {
            let (merged, _lsn) = crate::control::server::result_stream::materialize(s).await?;
            return Ok(vec![merged]);
        }
    };

    // Encode the plan.
    let plan_bytes = plan_wire::encode(&plan).map_err(|e| Error::Internal {
        detail: format!("gateway: plan encode failed: {e}"),
    })?;

    // Build descriptor version entries.
    let descriptor_versions: Vec<nodedb_cluster::rpc_codec::DescriptorVersionEntry> = version_set
        .iter()
        .map(
            |(name, version)| nodedb_cluster::rpc_codec::DescriptorVersionEntry {
                collection: name.clone(),
                version: *version,
            },
        )
        .collect();

    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id: tenant_id.as_u64(),
        database_id: database_id.as_u64(),
        deadline_remaining_ms: deadline_ms,
        trace_id: trace_id.0,
        descriptor_versions,
    });

    debug!(
        node_id,
        vshard_id,
        tenant_id = tenant_id.as_u64(),
        "gateway: dispatching ExecuteRequest to remote node"
    );

    let resp_rpc = transport.send_rpc(node_id, req).await.map_err(|e| {
        // Transport failure means the target node is unreachable —
        // we do NOT know who the new leader is. Use leader_node = 0
        // so the retry loop does NOT re-entrench the unreachable node
        // as leader in the routing table. The next retry will route
        // locally (leader == 0 → local) and let the local Raft state
        // resolve to the actual leader.
        Error::NotLeader {
            vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
            leader_node: 0,
            leader_addr: format!("node-{node_id} (transport error: {e})"),
        }
    })?;

    match resp_rpc {
        RaftRpc::ExecuteResponse(resp) => {
            if let Some(err) = resp.error {
                Err(map_typed_cluster_error(err, vshard_id))
            } else {
                Ok(resp.payloads)
            }
        }
        other => Err(Error::Internal {
            detail: format!("gateway: unexpected RPC response variant: {other:?}"),
        }),
    }
}

/// Map a [`TypedClusterError`] to an internal [`Error`].
///
/// `NotLeader` is mapped such that the gateway retry loop can extract the
/// hinted leader from `Error::NotLeader.leader_node` and update the routing
/// table before the next attempt.
fn map_typed_cluster_error(err: TypedClusterError, vshard_id: u64) -> Error {
    match err {
        TypedClusterError::NotLeader {
            leader_node_id,
            leader_addr,
            ..
        } => Error::NotLeader {
            vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
            leader_node: leader_node_id.unwrap_or(0),
            leader_addr: leader_addr.unwrap_or_default(),
        },
        TypedClusterError::DescriptorMismatch { collection, .. } => Error::RetryableSchemaChanged {
            descriptor: collection,
        },
        TypedClusterError::DeadlineExceeded { .. } => Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        },
        TypedClusterError::Internal { message, .. } => Error::Internal { detail: message },
    }
}

/// Build the deadline_remaining_ms value from the server's default.
pub fn default_deadline_ms(shared: &SharedState) -> u64 {
    Duration::from_secs(shared.tuning.network.default_deadline_secs).as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::rpc_codec::TypedClusterError;

    #[test]
    fn map_not_leader() {
        let err = TypedClusterError::NotLeader {
            group_id: 0,
            leader_node_id: Some(5),
            leader_addr: Some("10.0.0.5:9400".into()),
            term: 3,
        };
        match map_typed_cluster_error(err, 7) {
            Error::NotLeader { leader_node, .. } => assert_eq!(leader_node, 5),
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    #[test]
    fn map_descriptor_mismatch() {
        let err = TypedClusterError::DescriptorMismatch {
            collection: "orders".into(),
            expected_version: 1,
            actual_version: 2,
        };
        match map_typed_cluster_error(err, 0) {
            Error::RetryableSchemaChanged { descriptor } => assert_eq!(descriptor, "orders"),
            other => panic!("expected RetryableSchemaChanged, got {other:?}"),
        }
    }

    #[test]
    fn map_deadline_exceeded() {
        let err = TypedClusterError::DeadlineExceeded { elapsed_ms: 100 };
        assert!(matches!(
            map_typed_cluster_error(err, 0),
            Error::DeadlineExceeded { .. }
        ));
    }
}
