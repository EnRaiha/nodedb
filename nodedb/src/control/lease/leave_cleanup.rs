// SPDX-License-Identifier: BUSL-1.1

//! Durable lease and drain cleanup for a node that left the cluster.
//!
//! The `TopologyChange::Leave` apply writes a `_system.pending_leave_cleanup`
//! row before it returns. [`drive_leave_cleanup`] releases the node's leases
//! and ends the drains it proposed, from the singleton worker, and removes
//! the row once the metadata cache holds neither. The Leave post-apply, the
//! boot drain, and the retry worker all call it, so a cleanup a crash or a
//! failed proposal interrupted is driven again.

use crate::control::state::SharedState;

/// Whether this node's metadata state holds no lease of `node_id` and no
/// drain it proposed.
fn cleanup_done(shared: &SharedState, node_id: u64) -> bool {
    let holds_lease = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .leases
        .keys()
        .any(|(_, holder)| *holder == node_id);
    !holds_lease && shared.lease_drain.proposed_by(node_id).is_empty()
}

/// Drive the cleanup `node_id` owes. Returns whether its row is gone.
///
/// Only the singleton worker proposes. Every node removes its row once the
/// release and drain-end entries applied here. Awaits the local applied
/// watermark while it proposes.
pub async fn drive_leave_cleanup(shared: &SharedState, node_id: u64) -> crate::Result<bool> {
    if shared.is_singleton_worker() && !cleanup_done(shared, node_id) {
        if let Err(error) = super::gc::gc_leases_for_node(shared, node_id).await {
            tracing::warn!(
                node_id,
                %error,
                "lease release for a node that left did not apply; the retry worker re-drives it"
            );
        }
        super::gc::end_drains_for_node(shared, node_id).await;
    }
    if !cleanup_done(shared, node_id) {
        return Ok(false);
    }
    shared
        .credentials
        .catalog()
        .remove_pending_leave_cleanup(node_id)?;
    Ok(true)
}

/// Drive every owed leave cleanup. Returns how many rows remain.
///
/// `Err` only when the rows cannot be read.
pub async fn drain_pending_leave_cleanups(shared: &SharedState) -> crate::Result<usize> {
    let mut remaining = 0usize;
    for node_id in shared.credentials.catalog().load_pending_leave_cleanups()? {
        match drive_leave_cleanup(shared, node_id).await {
            Ok(true) => {}
            Ok(false) => remaining += 1,
            Err(error) => {
                remaining += 1;
                tracing::warn!(node_id, %error, "leave cleanup row could not be removed");
            }
        }
    }
    Ok(remaining)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::{DescriptorId, DescriptorKind, DescriptorLease, DrainOwner};
    use nodedb_types::Hlc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("leave.wal")).unwrap());
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        (SharedState::new(dispatcher, wal).unwrap(), dir)
    }

    /// A row stays while the left node still holds a lease or a drain, and
    /// goes once neither remains.
    #[tokio::test]
    async fn row_stays_until_leases_and_drains_are_gone() {
        let (state, _dir) = state();
        let id = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders");
        state
            .credentials
            .catalog()
            .enqueue_pending_leave_cleanup(7, 3)
            .unwrap();
        state.metadata_cache.write().unwrap().leases.insert(
            (id.clone(), 7),
            DescriptorLease {
                descriptor_id: id.clone(),
                version: 1,
                node_id: 7,
                expires_at: Hlc::new(1, 0),
            },
        );
        state
            .lease_drain
            .install_start(id.clone(), DrainOwner::Ddl, 1, Hlc::new(1, 0), 7);
        assert!(!cleanup_done(&state, 7));

        state.metadata_cache.write().unwrap().leases.clear();
        state.lease_drain.install_end(&id, &DrainOwner::Ddl);
        assert_eq!(drain_pending_leave_cleanups(&state).await.unwrap(), 0);
        assert!(
            state
                .credentials
                .catalog()
                .load_pending_leave_cleanups()
                .unwrap()
                .is_empty()
        );
    }
}
