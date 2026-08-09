// SPDX-License-Identifier: BUSL-1.1

//! Coordinator-side routed-surrogate-exchange helpers (F1b).
//!
//! [`assign_surrogate_routed`] turns a `(collection, pk)` endpoint key into the
//! AUTHORITATIVE global surrogate, routing the assign to the LEADER of the key's
//! home vShard so the value is the one the home node will store under.
//!
//! [`lookup_surrogate_routed`] is its READ-ONLY sibling: identical routing, but
//! the home leader runs `SurrogateAssigner::lookup` and never allocates. It
//! answers "which surrogate does this EXISTING row have", returning `None` when
//! the key names no row. A caller whose key must already name a row (a
//! materialized-sum join key pointing at a target row, say) must use this one:
//! an assign there would mint identity for a row that does not exist.
//!
//! Routing logic (shared by both):
//! - **Not cluster mode** (no `cluster_transport` / `cluster_routing`): resolve
//!   LOCALLY — single-node has no other home, the local catalog is
//!   authoritative.
//! - **Leader is self**: resolve LOCALLY — this node already owns the home
//!   vShard, so a self-RPC would be a pointless extra hop.
//! - **Leader is a remote node**: register the leader's address from the live
//!   topology (so `send_rpc` to a not-yet-warmed peer does not fail with
//!   `NodeUnreachable`), then send one `AssignSurrogateRequest` and map the
//!   reply to the authoritative surrogate (or a typed error).
//!
//! # Plane discipline
//!
//! Runs on the coordinator's Control Plane (Tokio). The QUIC `send_rpc` call is
//! Control-Plane I/O, which is allowed here. No storage I/O, no io_uring, no
//! Data-Plane access from this module.

use std::collections::BTreeSet;
use std::sync::Arc;

use nodedb_cluster::{
    AssignSurrogateRequest, AssignSurrogateResponse, NexarTransport, RaftRpc, RoutingTable,
};
use nodedb_types::Surrogate;

use crate::control::server::exchange::resolve::register_peers_from_topology;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

/// Where a routed surrogate exchange for a given home vShard must run.
enum Route<'a> {
    /// Resolve against THIS node's allocator / catalog.
    Local,
    /// Send exactly one RPC to the home vShard's leader.
    Remote {
        leader: u64,
        transport: &'a Arc<NexarTransport>,
    },
}

/// Decide whether the exchange resolves locally or must be routed to a remote
/// leader. Shared by the assign and lookup entry points so the two can never
/// drift into disagreeing about which node is authoritative for a key.
fn route_for<'a>(
    state: &'a SharedState,
    vshard: VShardId,
    collection: &str,
) -> crate::Result<Route<'a>> {
    // Not cluster mode — single-node has no peers; the local catalog IS the
    // authoritative source.
    let (Some(transport), Some(routing)) = (
        state.cluster_transport.as_ref(),
        state.cluster_routing.as_ref(),
    ) else {
        return Ok(Route::Local);
    };

    let leader = leader_for(routing, vshard, collection)?;

    // `0` = no leader elected for the home vShard yet. We must NOT fall back to
    // a local resolution here: this node may not be the eventual home leader, so
    // a local allocation could bind a surrogate that DIVERGES from the value the
    // home leader later assigns for the same (collection, pk) — exactly the
    // cross-shard identity divergence this routed exchange exists to prevent.
    // Surface a typed error (matching the shuffle resolver's `producer_nodes`
    // "no leader" contract) so the caller retries once an election resolves
    // rather than committing a split identity.
    if leader == 0 {
        return Err(crate::Error::Internal {
            detail: format!(
                "surrogate-exchange: no leader elected for home vshard {} ({collection}); \
                 cannot resolve an authoritative surrogate yet",
                vshard.as_u32()
            ),
        });
    }

    // Leader is self: this node owns the home vShard, so a self-RPC would be a
    // pointless extra hop; the local resolution is authoritative.
    if leader == state.node_id {
        return Ok(Route::Local);
    }

    Ok(Route::Remote { leader, transport })
}

/// Read the home vShard's leader from a routing snapshot.
fn leader_for(
    routing: &Arc<std::sync::RwLock<RoutingTable>>,
    vshard: VShardId,
    collection: &str,
) -> crate::Result<u64> {
    let guard = routing.read().unwrap_or_else(|p| p.into_inner());
    guard
        .leader_for_vshard(vshard.as_u32())
        .map_err(|e| crate::Error::Internal {
            detail: format!(
                "surrogate-exchange: no leader for vshard {} ({collection}): {e}",
                vshard.as_u32()
            ),
        })
}

/// The row identity a one-shot exchange resolves.
///
/// Bundled rather than passed as loose parameters because these six travel
/// together to the leader as one request and mean nothing apart: a `pk` without
/// its `collection`, or either without the scoping ids, names no row.
struct ExchangeKey<'a> {
    vshard: VShardId,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &'a str,
    pk: &'a [u8],
    trace_id: TraceId,
}

/// Build the one-shot request sent to the home leader.
fn build_request(
    state: &SharedState,
    key: ExchangeKey<'_>,
    lookup_only: bool,
) -> AssignSurrogateRequest {
    let ExchangeKey {
        vshard,
        database_id,
        tenant_id,
        collection,
        pk,
        trace_id,
    } = key;
    let deadline_remaining_ms = state
        .tuning
        .network
        .default_deadline_secs
        .saturating_mul(1000)
        .max(1);
    AssignSurrogateRequest {
        vshard_id: vshard.as_u32(),
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        collection: collection.to_string(),
        pk: pk.to_vec(),
        deadline_remaining_ms,
        trace_id: trace_id.0,
        lookup_only: Some(lookup_only),
    }
}

/// Dispatch the one-shot RPC to `leader`, returning the reply body or a typed
/// error. Registers the leader's address first so a not-yet-warmed peer does not
/// fail with `NodeUnreachable`.
async fn send_to_leader(
    state: &SharedState,
    transport: &Arc<NexarTransport>,
    leader: u64,
    req: AssignSurrogateRequest,
) -> crate::Result<AssignSurrogateResponse> {
    let mut targets = BTreeSet::new();
    targets.insert(leader);
    register_peers_from_topology(state, transport, &targets);

    match transport
        .send_rpc(leader, RaftRpc::AssignSurrogateRequest(req))
        .await
    {
        Ok(RaftRpc::AssignSurrogateResponse(resp)) => match resp.error {
            None => Ok(resp),
            Some(e) => Err(crate::Error::Internal {
                detail: format!("surrogate-exchange failed on leader node {leader}: {e:?}"),
            }),
        },
        Ok(other) => Err(crate::Error::Internal {
            detail: format!("surrogate-exchange: unexpected reply from node {leader}: {other:?}"),
        }),
        Err(e) => Err(crate::Error::Internal {
            detail: format!("surrogate-exchange RPC to node {leader} failed: {e}"),
        }),
    }
}

/// Resolve `(collection, pk)` to the authoritative global surrogate, ASSIGNING
/// one when the key has no binding yet, and routing the assign to the home
/// vShard's leader when this node is not the leader.
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
    match route_for(state, vshard, collection)? {
        Route::Local => state
            .surrogate_assigner
            .assign(database_id, tenant_id, collection, pk),
        Route::Remote { leader, transport } => {
            let req = build_request(
                state,
                ExchangeKey {
                    vshard,
                    database_id,
                    tenant_id,
                    collection,
                    pk,
                    trace_id,
                },
                false,
            );
            let resp = send_to_leader(state, transport, leader, req).await?;
            Ok(Surrogate::new(resp.surrogate))
        }
    }
}

/// Resolve `(collection, pk)` to the surrogate of an EXISTING row without ever
/// allocating one, routing the lookup to the home vShard's leader when this node
/// is not the leader.
///
/// `Ok(None)` means the key names no row — an answer, not a failure. Callers
/// that require the row to exist turn that into their own typed error naming
/// what was being resolved; callers for whom absence is a legal no-op treat it
/// as one.
///
/// This is the primitive for every resolution whose key is expected to point at
/// a row that already exists. [`assign_surrogate_routed`] would instead bind a
/// fresh surrogate to the missing key, publishing an identity for a row that was
/// never written.
pub async fn lookup_surrogate_routed(
    state: &SharedState,
    vshard: VShardId,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    pk: &[u8],
    trace_id: TraceId,
) -> crate::Result<Option<Surrogate>> {
    match route_for(state, vshard, collection)? {
        Route::Local => state
            .surrogate_assigner
            .lookup(database_id, tenant_id, collection, pk),
        Route::Remote { leader, transport } => {
            let req = build_request(
                state,
                ExchangeKey {
                    vshard,
                    database_id,
                    tenant_id,
                    collection,
                    pk,
                    trace_id,
                },
                true,
            );
            let resp = send_to_leader(state, transport, leader, req).await?;
            // `found` is the discriminator, never `surrogate == 0`: zero is a
            // reserved sentinel that also appears in catalog-less fixtures.
            // A leader that predates this flag answers `None`, which we must
            // NOT silently read as "found" — it means the reply cannot be
            // interpreted as a lookup at all.
            match resp.found {
                Some(true) => Ok(Some(Surrogate::new(resp.surrogate))),
                Some(false) => Ok(None),
                None => Err(crate::Error::Internal {
                    detail: format!(
                        "surrogate-exchange: leader node {leader} answered a lookup for \
                         '{collection}' without a found flag; cannot tell a hit from a miss"
                    ),
                }),
            }
        }
    }
}
