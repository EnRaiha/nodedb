// SPDX-License-Identifier: BUSL-1.1

//! Durable drain start and end on this node.
//!
//! The metadata applier and the single-node fallback both go through these
//! functions. Each writes or deletes the drain's `SystemCatalog` row before
//! it changes the tracker, so boot seeds the same drains a restart dropped.

use nodedb_cluster::{DescriptorId, DrainOwner};
use nodedb_types::Hlc;

use crate::control::security::catalog::StoredDrain;
use crate::control::state::SharedState;

/// Persist and install `owner`'s drain of `descriptor_id`, then release an
/// idle lease on it: no new statement can take the lease now.
pub fn apply_drain_start(
    shared: &SharedState,
    descriptor_id: &DescriptorId,
    owner: &DrainOwner,
    up_to_version: u64,
    expires_at: Hlc,
    proposer_node_id: u64,
) -> crate::Result<()> {
    shared
        .credentials
        .catalog()
        .put_descriptor_drain(&StoredDrain {
            descriptor_id: descriptor_id.clone(),
            owner: owner.clone(),
            up_to_version,
            expires_at,
            proposer_node_id,
        })?;
    {
        // Shares plan admission's gate: an admission completes before this
        // start installs, or this drain wins and admission fails closed.
        let _admission_gate = shared
            .lease_admission_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        shared.lease_drain.install_start(
            descriptor_id.clone(),
            owner.clone(),
            up_to_version,
            expires_at,
            proposer_node_id,
        );
    }
    super::release::release_idle_on_drain(shared, descriptor_id);
    Ok(())
}

/// Delete the rows of `drains`, then end them in the tracker. Other owners'
/// drains of the same descriptors stay.
pub fn apply_drain_ends(
    shared: &SharedState,
    drains: &[(DescriptorId, DrainOwner)],
) -> crate::Result<()> {
    if drains.is_empty() {
        return Ok(());
    }
    shared
        .credentials
        .catalog()
        .remove_descriptor_drains(drains)?;
    for (id, owner) in drains {
        shared.lease_drain.install_end(id, owner);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::DescriptorKind;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    /// A single-node drain writes its row on start and deletes only its own
    /// owner's row on end.
    #[tokio::test]
    async fn local_drain_rows_follow_start_and_end() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("drain.wal")).unwrap());
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).unwrap();
        let id = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders");
        let moving = DrainOwner::MoveTenant {
            tenant_id: 1,
            source_db_id: 0,
        };

        apply_drain_start(&state, &id, &DrainOwner::Ddl, 4, Hlc::new(9, 0), 1).unwrap();
        apply_drain_start(&state, &id, &moving, 5, Hlc::new(9, 0), 1).unwrap();
        apply_drain_ends(&state, &[(id.clone(), DrainOwner::Ddl)]).unwrap();

        let rows = state
            .credentials
            .catalog()
            .load_descriptor_drains()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].owner, moving);
        assert!(state.lease_drain.is_draining(&id, 5));
        assert!(
            state.lease_drain.is_draining(&id, 4),
            "the move owner still covers 4"
        );
    }
}
