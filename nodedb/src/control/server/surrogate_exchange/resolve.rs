// SPDX-License-Identifier: BUSL-1.1

//! Coordinator-side routed-surrogate-exchange helper (F1b).
//!
//! [`assign_surrogate_routed`] turns a `(collection, pk)` endpoint key into the
//! AUTHORITATIVE global surrogate, routing the assign to the LEADER of the key's
//! home vShard so the value is the one the home node will store under. It is the
//! public primitive the later graph dual-home write unit (F1b-dualhome) calls; it
//! is NOT yet wired into `insert_edge`.
//!
//! Routing logic:
//! - **Not cluster mode** (no `cluster_transport` / `cluster_routing`): assign
//!   LOCALLY — single-node has no other home, the local allocator is
//!   authoritative.
//! - **Leader is self**: assign LOCALLY — this node already owns the home vShard,
//!   so a self-RPC would be a pointless extra hop; the local assign is
//!   authoritative.
//! - **Leader is a remote node**: register the leader's address from the live
//!   topology (so `send_rpc` to a not-yet-warmed peer does not fail with
//!   `NodeUnreachable`), then send one `AssignSurrogateRequest` and map the reply
//!   to the authoritative surrogate (or a typed error).
//!
//! # Plane discipline
//!
//! Runs on the coordinator's Control Plane (Tokio). The QUIC `send_rpc` call is
//! Control-Plane I/O, which is allowed here. No storage I/O, no io_uring, no
//! Data-Plane access from this module.

use std::collections::BTreeSet;

use nodedb_cluster::{AssignSurrogateRequest, AssignSurrogateResponse, RaftRpc};
use nodedb_types::Surrogate;

use crate::control::server::exchange::resolve::register_peers_from_topology;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

/// Resolve `(collection, pk)` to the authoritative global surrogate, routing the
/// assign to the home vShard's leader when this node is not the leader.
///
/// `vshard` is the endpoint key's home vShard (the caller resolves it from the
/// key, e.g. via [`VShardId::from_key`]). `database_id` / `tenant_id` scope the
/// identity; `trace_id` is propagated to the leader-side handler for tracing.
///
/// Returns the authoritative `Surrogate` (`Surrogate::ZERO` only in the
/// catalog-less local-assign path, mirroring `SurrogateAssigner::assign`) or a
/// typed error if the remote assign failed.
pub async fn assign_surrogate_routed(
    state: &SharedState,
    vshard: VShardId,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    pk: &[u8],
    trace_id: TraceId,
) -> crate::Result<Surrogate> {
    // Not cluster mode — single-node has no peers; the local allocator IS the
    // authoritative source. Assign locally and return.
    let (Some(transport), Some(routing)) = (
        state.cluster_transport.as_ref(),
        state.cluster_routing.as_ref(),
    ) else {
        return state
            .surrogate_assigner
            .assign(database_id, tenant_id, collection, pk);
    };

    // Resolve the home vShard's leader from a routing snapshot.
    let leader = {
        let guard = routing.read().unwrap_or_else(|p| p.into_inner());
        guard
            .leader_for_vshard(vshard.as_u32())
            .map_err(|e| crate::Error::Internal {
                detail: format!(
                    "assign-surrogate: no leader for vshard {} ({collection}): {e}",
                    vshard.as_u32()
                ),
            })?
    };

    // `0` = no leader elected for the home vShard yet. We must NOT fall back to a
    // local assign here: this node may not be the eventual home leader, so a local
    // allocation could bind a surrogate that DIVERGES from the value the home
    // leader later assigns for the same (collection, pk) — exactly the cross-shard
    // identity divergence this routed exchange exists to prevent. Surface a typed
    // error (matching the shuffle resolver's `producer_nodes` "no leader" contract)
    // so the caller retries once an election resolves rather than committing a
    // split identity.
    if leader == 0 {
        return Err(crate::Error::Internal {
            detail: format!(
                "assign-surrogate: no leader elected for home vshard {} ({collection}); \
                 cannot resolve an authoritative surrogate yet",
                vshard.as_u32()
            ),
        });
    }

    // Leader is self: this node owns the home vShard, so a self-RPC would be a
    // pointless extra hop; the local assign is authoritative.
    if leader == state.node_id {
        return state
            .surrogate_assigner
            .assign(database_id, tenant_id, collection, pk);
    }

    // Remote leader: ensure its address is registered before dispatch, then send
    // the one-shot RPC and map the reply to the authoritative surrogate.
    let mut targets = BTreeSet::new();
    targets.insert(leader);
    register_peers_from_topology(state, transport, &targets);

    let deadline_remaining_ms = state
        .tuning
        .network
        .default_deadline_secs
        .saturating_mul(1000)
        .max(1);
    let req = AssignSurrogateRequest {
        vshard_id: vshard.as_u32(),
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        collection: collection.to_string(),
        pk: pk.to_vec(),
        deadline_remaining_ms,
        trace_id: trace_id.0,
    };

    match transport
        .send_rpc(leader, RaftRpc::AssignSurrogateRequest(req))
        .await
    {
        Ok(RaftRpc::AssignSurrogateResponse(AssignSurrogateResponse {
            surrogate,
            error: None,
        })) => Ok(Surrogate::new(surrogate)),
        Ok(RaftRpc::AssignSurrogateResponse(AssignSurrogateResponse {
            error: Some(e), ..
        })) => Err(crate::Error::Internal {
            detail: format!("assign-surrogate failed on leader node {leader}: {e:?}"),
        }),
        Ok(other) => Err(crate::Error::Internal {
            detail: format!("assign-surrogate: unexpected reply from node {leader}: {other:?}"),
        }),
        Err(e) => Err(crate::Error::Internal {
            detail: format!("assign-surrogate RPC to node {leader} failed: {e}"),
        }),
    }
}
