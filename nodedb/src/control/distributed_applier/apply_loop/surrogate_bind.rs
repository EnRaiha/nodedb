// SPDX-License-Identifier: BUSL-1.1

//! Apply path for a committed `ReplicatedWrite::SurrogateBind` entry.
//!
//! The leader of a key's collection home mints the key's surrogate by
//! proposing this entry (`surrogate_exchange::authority`). Every replica binds
//! each key first-wins, in Raft log order. A key an earlier entry of the log
//! already bound keeps that binding on every replica alike, so the proposer
//! reads the winner back after its own entry applies. The binds land in the
//! fsync-committed catalog before the apply reports.

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::AppliedWrite;
use crate::control::wal_replication::ReplicatedIdentity;
use crate::types::{DatabaseId, TenantId};

use super::context::ApplyContext;
use super::proposal_gate::EntryOutcome;

/// Bind every identity of the entry and resolve its waiter.
pub(super) fn apply_surrogate_bind(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    tenant_id: TenantId,
    database_id: DatabaseId,
    identities: &[ReplicatedIdentity],
) -> EntryOutcome {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
        ..
    } = pos;
    let result = bind_identities(&ctx, tenant_id, database_id, identities);
    let durable = result.is_ok();
    ctx.tracker.complete(
        group_id,
        log_index,
        applied_key,
        result.map(|()| AppliedWrite::unversioned(Vec::new())),
    );
    EntryOutcome::Applied {
        durable,
        result: None,
    }
}

/// Bind the entry's identities first-wins, one catalog transaction per
/// collection the entry names. Keys of one collection bind in entry order.
fn bind_identities(
    ctx: &ApplyContext<'_>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    identities: &[ReplicatedIdentity],
) -> crate::Result<()> {
    let mut collections: Vec<&str> = Vec::new();
    for identity in identities {
        if !collections.contains(&identity.collection.as_str()) {
            collections.push(&identity.collection);
        }
    }
    for collection in collections {
        let rows: Vec<(&[u8], nodedb_types::Surrogate)> = identities
            .iter()
            .filter(|identity| identity.collection == collection)
            .map(|identity| {
                (
                    identity.pk_bytes.as_slice(),
                    nodedb_types::Surrogate::new(identity.surrogate),
                )
            })
            .collect();
        ctx.state.surrogate_assigner.bind_many(
            nodedb_types::CollectionKey::from_bare(database_id, collection),
            tenant_id,
            &rows,
        )?;
    }
    Ok(())
}
