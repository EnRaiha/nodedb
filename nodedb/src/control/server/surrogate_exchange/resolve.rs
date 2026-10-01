// SPDX-License-Identifier: BUSL-1.1

//! Coordinator-side routed-surrogate-exchange helpers (F1b).
//!
//! [`assign_surrogate_routed`] turns a `(collection, pk)` key into the
//! AUTHORITATIVE global surrogate, routing the assign to the LEADER of the key's
//! collection home vShard (`VShardId::from_collection`). That leader is the one
//! place a key's surrogate is minted (`authority`): a document row and a graph
//! edge endpoint with the same key both obtain it there, so every owner stores
//! the one value.
//!
//! [`lookup_surrogate_routed`] is its READ-ONLY sibling: identical routing, but
//! the home leader runs `SurrogateAssigner::lookup_bound` and never allocates. It
//! answers "which surrogate does this EXISTING row have", returning `None` when
//! the key names no row. A caller whose key must already name a row (a
//! materialized-sum join key pointing at a target row, say) must use this one:
//! an assign there will mint identity for a row that does not exist.
//!
//! Routing logic (shared by both):
//! - **Not cluster mode** (no `cluster_transport` / `cluster_routing`): resolve
//!   LOCALLY — single-node has no other home, the local catalog is
//!   authoritative.
//! - **Leader is self**: resolve LOCALLY — this node already owns the home
//!   vShard, so a self-RPC will be a pointless extra hop.
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
use nodedb_types::{CollectionKey, Surrogate};

use crate::control::cluster::warm_peers::register_peers_from_topology;
use crate::control::state::SharedState;
use crate::types::{TenantId, TraceId, VShardId};

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

    let leader = leader_for(routing, vshard, collection, state.node_id)?;

    // `0` = no leader elected for the home vShard yet. We must NOT fall back to
    // a local resolution here: this node is not necessarily the eventual home leader, so
    // a local allocation can bind a surrogate that DIVERGES from the value the
    // home leader later assigns for the same (collection, pk) — exactly the
    // cross-shard identity divergence this routed exchange exists to prevent.
    // Surface the typed, retryable no-leader error so the caller retries once
    // an election resolves rather than committing a split identity.
    if leader == 0 {
        tracing::debug!(
            vshard_id = vshard.as_u32(),
            collection,
            "surrogate-exchange: the home vShard has no known leader; the caller retries"
        );
        return Err(crate::Error::NoLeader { vshard_id: vshard });
    }

    // Leader is self: this node owns the home vShard, so a self-RPC will be a
    // pointless extra hop; the local resolution is authoritative.
    if leader == state.node_id {
        return Ok(Route::Local);
    }

    Ok(Route::Remote { leader, transport })
}

/// Read the home vShard's leader from a routing snapshot. `0` when no leader
/// is known.
///
/// A hint that names `self_id` while this node's view does not list it as a
/// replica of the home group is stale: the node left the group. It reads as
/// `0`, because a local resolution on a node outside the home will bind a
/// surrogate the home never saw.
fn leader_for(
    routing: &Arc<std::sync::RwLock<RoutingTable>>,
    vshard: VShardId,
    collection: &str,
    self_id: u64,
) -> crate::Result<u64> {
    let guard = routing.read().unwrap_or_else(|p| p.into_inner());
    let leader = guard
        .leader_for_vshard(vshard.as_u32())
        .map_err(|e| crate::Error::Internal {
            detail: format!(
                "surrogate-exchange: no leader for vshard {} ({collection}): {e}",
                vshard.as_u32()
            ),
        })?;
    if leader == self_id && !guard.is_replica_of_vshard(vshard.as_u32(), self_id) {
        return Ok(0);
    }
    Ok(leader)
}

/// The row identity a one-shot exchange resolves.
///
/// Bundled rather than passed as loose parameters because these six travel
/// together to the leader as one request and mean nothing apart: a `pk` without
/// its `collection`, or either without the scoping ids, names no row.
struct ExchangeKey<'a> {
    vshard: VShardId,
    collection: CollectionKey<'a>,
    tenant_id: TenantId,
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
        collection,
        tenant_id,
        pk,
        trace_id,
    } = key;
    // What is left of the running statement's budget: a surrogate assignment is
    // work the statement waits on, so it stops when the statement does.
    let deadline_remaining_ms = crate::control::server::shared::session::statement_deadline_ms(
        state.tuning.network.default_deadline_secs,
    );
    AssignSurrogateRequest {
        vshard_id: vshard.as_u32(),
        database_id: collection.database_id().as_u64(),
        tenant_id: tenant_id.as_u64(),
        // The bare catalog name: the leader rebuilds the canonical key from
        // it and `database_id`.
        collection: collection.name().to_string(),
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
/// one when the key has no binding yet, and routing the assign to the leader
/// of the collection's home vShard when this node is not the leader.
///
/// `collection` and `tenant_id` scope the identity; `trace_id` is propagated
/// to the leader-side handler for tracing. A binding this node's catalog holds
/// answers without a request, and a winner the home answers is kept in this
/// node's catalog (see [`assign_surrogates_routed`]).
pub async fn assign_surrogate_routed(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    pk: &[u8],
    trace_id: TraceId,
) -> crate::Result<Surrogate> {
    let mut bound = assign_surrogates_routed(state, collection, tenant_id, &[pk], trace_id).await?;
    bound.pop().ok_or_else(|| crate::Error::Internal {
        detail: format!(
            "surrogate-exchange: an assign in '{}' returned no surrogate",
            collection.name()
        ),
    })
}

/// [`assign_surrogate_routed`] for many keys of one collection, in `pks`
/// order.
///
/// A binding this node's catalog holds is already the log-order winner (a
/// replica's apply, or a winner kept earlier), so it answers without a
/// request. The other keys go to the home: on the home leader they are minted
/// in one Raft entry; from any other node they go to the leader as concurrent
/// requests, at most [`MAX_ROUTED_IN_FLIGHT`] at a time. Every winner a remote
/// home answers is bound in this node's catalog first-wins, so a later lookup
/// here makes no request.
pub async fn assign_surrogates_routed(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    pks: &[&[u8]],
    trace_id: TraceId,
) -> crate::Result<Vec<Surrogate>> {
    use futures::StreamExt;

    let LocalBindings { mut out, missing } = local_bindings(state, collection, tenant_id, pks)?;
    if missing.is_empty() {
        return finish(out, collection);
    }
    let vshard = VShardId::from_collection(collection);
    let winners: Vec<Surrogate> = match route_for(state, vshard, collection.name())? {
        Route::Local => {
            super::authority::assign_many_at_home(state, vshard, collection, tenant_id, &missing)
                .await?
        }
        Route::Remote { leader, transport } => {
            // Built in a loop, not by an iterator closure: a closure that
            // takes a borrowed key, held across the await below, makes every
            // caller's future fail its `Send` check.
            let mut requests = Vec::with_capacity(missing.len());
            for &pk in &missing {
                let key = ExchangeKey {
                    vshard,
                    collection,
                    tenant_id,
                    pk,
                    trace_id,
                };
                requests.push(assign_remote(state, transport, leader, key));
            }
            let answered: Vec<Surrogate> = futures::stream::iter(requests)
                .buffered(MAX_ROUTED_IN_FLIGHT)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<crate::Result<_>>()?;
            keep_winners(state, collection, tenant_id, &missing, &answered)?
        }
    };
    fill(&mut out, pks, &missing, winners.into_iter().map(Some));
    finish(out, collection)
}

/// [`lookup_surrogate_routed`] for many keys of one collection, in `pks`
/// order, local bindings first, at most [`MAX_ROUTED_IN_FLIGHT`] requests in
/// flight at a time. Every winner a remote home answers is bound in this
/// node's catalog first-wins.
pub async fn lookup_surrogates_routed(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    pks: &[&[u8]],
    trace_id: TraceId,
) -> crate::Result<Vec<Option<Surrogate>>> {
    use futures::StreamExt;

    let LocalBindings { mut out, missing } = local_bindings(state, collection, tenant_id, pks)?;
    if missing.is_empty() {
        return Ok(out);
    }
    let vshard = VShardId::from_collection(collection);
    let Route::Remote { leader, transport } = route_for(state, vshard, collection.name())? else {
        // This node is the home: its catalog miss is the home's answer.
        return Ok(out);
    };
    // Built in a loop for the reason given in `assign_surrogates_routed`.
    let mut lookups = Vec::with_capacity(missing.len());
    for &pk in &missing {
        let key = ExchangeKey {
            vshard,
            collection,
            tenant_id,
            pk,
            trace_id,
        };
        lookups.push(lookup_remote(state, transport, leader, key));
    }
    let answered: Vec<Option<Surrogate>> = futures::stream::iter(lookups)
        .buffered(MAX_ROUTED_IN_FLIGHT)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<crate::Result<_>>()?;
    let mut kept = Vec::with_capacity(answered.len());
    for (pk, answer) in missing.iter().zip(answered) {
        kept.push(match answer {
            Some(winner) => Some(
                state
                    .surrogate_assigner
                    .bind(collection, tenant_id, pk, winner)?,
            ),
            None => None,
        });
    }
    fill(&mut out, pks, &missing, kept.into_iter());
    Ok(out)
}

/// Routed requests one batch keeps in flight to a remote home leader.
const MAX_ROUTED_IN_FLIGHT: usize = 64;

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
/// a row that already exists. [`assign_surrogate_routed`] will instead bind a
/// fresh surrogate to the missing key, publishing an identity for a row that was
/// never written.
pub async fn lookup_surrogate_routed(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    pk: &[u8],
    trace_id: TraceId,
) -> crate::Result<Option<Surrogate>> {
    let mut found = lookup_surrogates_routed(state, collection, tenant_id, &[pk], trace_id).await?;
    Ok(found.pop().flatten())
}

/// What this node's catalog answers for a batch of keys.
struct LocalBindings<'k> {
    /// The binding of each key, in `pks` order. `None` for an unbound key.
    out: Vec<Option<Surrogate>>,
    /// The distinct keys the catalog binds none of, in first-seen order.
    missing: Vec<&'k [u8]>,
}

/// This node's catalog binding of each of `pks`.
fn local_bindings<'k>(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    pks: &[&'k [u8]],
) -> crate::Result<LocalBindings<'k>> {
    let mut out = Vec::with_capacity(pks.len());
    let mut missing: Vec<&[u8]> = Vec::new();
    let mut asked: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    for &pk in pks {
        let hit = state
            .surrogate_assigner
            .lookup_bound(collection, tenant_id, pk)?;
        if hit.is_none() && asked.insert(pk) {
            missing.push(pk);
        }
        out.push(hit);
    }
    Ok(LocalBindings { out, missing })
}

/// Bind each remote winner in this node's catalog first-wins, and return the
/// kept values in `missing` order.
fn keep_winners(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    missing: &[&[u8]],
    winners: &[Surrogate],
) -> crate::Result<Vec<Surrogate>> {
    missing
        .iter()
        .zip(winners)
        .map(|(pk, winner)| {
            state
                .surrogate_assigner
                .bind(collection, tenant_id, pk, *winner)
        })
        .collect()
}

/// Write the answers for `missing` (in its order) into every unfilled slot of
/// `out` whose key they answer.
fn fill(
    out: &mut [Option<Surrogate>],
    pks: &[&[u8]],
    missing: &[&[u8]],
    answers: impl Iterator<Item = Option<Surrogate>>,
) {
    let by_key: std::collections::HashMap<&[u8], Option<Surrogate>> =
        missing.iter().copied().zip(answers).collect();
    for (slot, pk) in out.iter_mut().zip(pks) {
        if slot.is_none()
            && let Some(answer) = by_key.get(pk)
        {
            *slot = *answer;
        }
    }
}

/// Every slot filled, or the typed error naming the collection.
fn finish(
    out: Vec<Option<Surrogate>>,
    collection: CollectionKey<'_>,
) -> crate::Result<Vec<Surrogate>> {
    out.into_iter()
        .map(|slot| {
            slot.ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "surrogate-exchange: a key in '{}' stayed unresolved",
                    collection.name()
                ),
            })
        })
        .collect()
}

/// One assign at the remote home leader.
async fn assign_remote(
    state: &SharedState,
    transport: &Arc<NexarTransport>,
    leader: u64,
    key: ExchangeKey<'_>,
) -> crate::Result<Surrogate> {
    let req = build_request(state, key, false);
    let resp = send_to_leader(state, transport, leader, req).await?;
    Ok(Surrogate::new(resp.surrogate))
}

/// One lookup at the remote home leader.
async fn lookup_remote(
    state: &SharedState,
    transport: &Arc<NexarTransport>,
    leader: u64,
    key: ExchangeKey<'_>,
) -> crate::Result<Option<Surrogate>> {
    let name = key.collection.name().to_string();
    let req = build_request(state, key, true);
    let resp = send_to_leader(state, transport, leader, req).await?;
    // `found` is the discriminator, never `surrogate == 0`: zero is a reserved
    // sentinel that also appears in catalog-less fixtures. A reply with no
    // flag cannot be read as a lookup at all.
    match resp.found {
        Some(true) => Ok(Some(Surrogate::new(resp.surrogate))),
        Some(false) => Ok(None),
        None => Err(crate::Error::Internal {
            detail: format!(
                "surrogate-exchange: leader node {leader} answered a lookup for '{name}' \
                 without a found flag; cannot tell a hit from a miss"
            ),
        }),
    }
}
