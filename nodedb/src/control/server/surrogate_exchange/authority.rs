// SPDX-License-Identifier: BUSL-1.1

//! The one place a key's surrogate is minted: the leader of the key's
//! collection home vShard.
//!
//! A document row lives on its collection's home vShard, and a graph edge
//! binds each endpoint on the endpoint's key vShard. Both obtain the key's
//! surrogate here, so every owner holds the one value:
//!
//! 1. A binding the home leader's catalog holds is the answer.
//! 2. Otherwise the leader draws a fresh value and proposes a
//!    `ReplicatedWrite::SurrogateBind` for the key to the home vShard's group.
//!    Every replica binds it first-wins in log order, the leader included. A
//!    document write already in the log for the same key wins over it on every
//!    replica alike.
//! 3. The leader reads the key's binding back after its entry applied, and
//!    answers that.
//!
//! The bind never enters the leader's catalog outside the log, so a replica
//! and the leader cannot disagree, and the binding survives a change of
//! leader. Without a data-group Raft (a single node) the local catalog is the
//! home, and binds directly.

use nodedb_types::{CollectionKey, Surrogate};

use crate::control::state::SharedState;
use crate::control::wal_replication::{
    ReplicatedEntry, ReplicatedIdentity, ReplicatedWrite, propose_replicated_entry,
};
use crate::types::{TenantId, VShardId};

/// The surrogate of `(collection, pk)`, minted and bound on `home`'s group
/// when the key has none. Runs on the leader of `home`, the key's collection
/// home vShard.
pub(crate) async fn assign_at_home(
    state: &SharedState,
    home: VShardId,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    pk: &[u8],
) -> crate::Result<Surrogate> {
    let mut bound = assign_many_at_home(state, home, collection, tenant_id, &[pk]).await?;
    bound.pop().ok_or_else(|| crate::Error::Internal {
        detail: format!(
            "surrogate bind of a key in '{}' on vShard {} returned no surrogate",
            collection.name(),
            home.as_u32()
        ),
    })
}

/// [`assign_at_home`] for many keys of one collection, in `pks` order. Every
/// key the home binds none of is minted in one `SurrogateBind` entry.
pub(crate) async fn assign_many_at_home(
    state: &SharedState,
    home: VShardId,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    pks: &[&[u8]],
) -> crate::Result<Vec<Surrogate>> {
    let assigner = &state.surrogate_assigner;
    let proposer = state.async_raft_proposer()?;
    let local = assigner.lookup_bound_many(collection, tenant_id, pks)?;
    let mut unbound: Vec<&[u8]> = Vec::new();
    let mut proposed: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    for (&pk, hit) in pks.iter().zip(&local) {
        if hit.is_none() && proposed.insert(pk) {
            unbound.push(pk);
        }
    }
    if !unbound.is_empty() {
        let candidates = assigner.mint_candidates(unbound.len()).await?;
        let identities: Vec<ReplicatedIdentity> = unbound
            .iter()
            .zip(candidates)
            .map(|(pk, candidate)| ReplicatedIdentity {
                collection: collection.name().to_string(),
                pk_bytes: pk.to_vec(),
                surrogate: candidate.as_u32(),
            })
            .collect();
        let entry = ReplicatedEntry::new(
            tenant_id.as_u64(),
            collection.database_id().as_u64(),
            home.as_u32(),
            ReplicatedWrite::SurrogateBind { identities },
        );
        let deadline =
            crate::control::server::shared::session::StatementDeadline::of_statement(state)
                .tokio_instant();
        propose_replicated_entry(state, proposer, entry, deadline).await?;
    }
    let bound = assigner.lookup_bound_many(collection, tenant_id, pks)?;
    bound
        .into_iter()
        .map(|hit| {
            hit.ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "surrogate bind of a key in '{}' applied on vShard {} but the key is \
                         unbound",
                    collection.name(),
                    home.as_u32()
                ),
            })
        })
        .collect()
}
