// SPDX-License-Identifier: BUSL-1.1

//! The routed exchange for keys this node's catalog already missed.
//!
//! The caller reads this node's catalog first and passes the keys it binds
//! none of. These functions run no local read of their own. Every answer is
//! bound in this node's catalog on return, so the caller writes nothing.

use std::sync::Arc;

use nodedb_cluster::NexarTransport;
use nodedb_types::{CollectionKey, Surrogate};

use crate::control::state::SharedState;
use crate::types::{TenantId, TraceId, VShardId};

use super::resolve::{ExchangeKey, Route, build_request, route_for, send_to_leader};

/// Routed requests one batch keeps in flight to a remote home leader.
pub(super) const MAX_ROUTED_IN_FLIGHT: usize = 64;

/// The home's winner for each key of `unbound`, in `unbound` order. Every
/// winner is bound in this node's catalog on return.
///
/// `unbound` holds distinct keys this node's catalog bound none of when the
/// caller read it. A key bound since keeps its binding: the home binds
/// first-wins and answers the earlier value.
pub(crate) async fn assign_unbound_routed(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    unbound: &[&[u8]],
    trace_id: TraceId,
) -> crate::Result<Vec<Surrogate>> {
    use futures::StreamExt;

    if unbound.is_empty() {
        return Ok(Vec::new());
    }
    let vshard = VShardId::from_collection(collection);
    match route_for(state, vshard, collection.name())? {
        // This node leads the home: its own apply binds every key.
        Route::Local => {
            super::authority::mint_many_at_home(state, vshard, collection, tenant_id, unbound).await
        }
        Route::Remote { leader, transport } => {
            // Built in a loop, not by an iterator closure: a closure that
            // takes a borrowed key, held across the await below, makes every
            // caller's future fail its `Send` check.
            let mut requests = Vec::with_capacity(unbound.len());
            for &pk in unbound {
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
            keep_winners(state, collection, tenant_id, unbound, &answered)
        }
    }
}

/// The home's binding of each key of `unbound`, in `unbound` order, `None`
/// for a key the home binds none of. Every found winner is bound in this
/// node's catalog on return.
///
/// `unbound` holds distinct keys this node's catalog bound none of when the
/// caller read it. A node that leads the home answers from that read.
pub(crate) async fn lookup_unbound_routed(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    unbound: &[&[u8]],
    trace_id: TraceId,
) -> crate::Result<Vec<Option<Surrogate>>> {
    use futures::StreamExt;

    if unbound.is_empty() {
        return Ok(Vec::new());
    }
    let vshard = VShardId::from_collection(collection);
    let Route::Remote { leader, transport } = route_for(state, vshard, collection.name())? else {
        // This node is the home: its catalog miss is the home's answer.
        return Ok(vec![None; unbound.len()]);
    };
    // Built in a loop for the reason given in `assign_unbound_routed`.
    let mut lookups = Vec::with_capacity(unbound.len());
    for &pk in unbound {
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
    // Every answered winner binds first-wins in one catalog transaction.
    let found: Vec<(&[u8], Surrogate)> = unbound
        .iter()
        .zip(&answered)
        .filter_map(|(&pk, answer)| answer.map(|winner| (pk, winner)))
        .collect();
    let mut bound = state
        .surrogate_assigner
        .bind_many(collection, tenant_id, &found)?
        .into_iter();
    let mut kept = Vec::with_capacity(answered.len());
    for answer in &answered {
        kept.push(match answer {
            Some(_) => Some(bound.next().ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "a found key of '{}' stayed unbound after bind",
                    collection.name()
                ),
            })?),
            None => None,
        });
    }
    Ok(kept)
}

/// Bind each remote winner in this node's catalog first-wins, in one catalog
/// transaction, and return the kept values in `missing` order.
fn keep_winners(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    missing: &[&[u8]],
    winners: &[Surrogate],
) -> crate::Result<Vec<Surrogate>> {
    let rows: Vec<(&[u8], Surrogate)> = missing
        .iter()
        .copied()
        .zip(winners.iter().copied())
        .collect();
    state
        .surrogate_assigner
        .bind_many(collection, tenant_id, &rows)
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
