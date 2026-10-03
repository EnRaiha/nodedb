// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease garbage collection for nodes that left the cluster.
//!
//! A crashed node's leases are never TTL-pruned from `MetadataCache.leases`
//! (only a `DescriptorLeaseRelease` entry removes them), so every DDL drain
//! on those descriptors times out forever. The durable leave cleanup
//! (`lease::leave_cleanup`) runs this module. The metadata leader's periodic sweep in
//! `nodedb-cluster` is the safety net for non-member and SWIM-Dead holders.

use nodedb_cluster::DescriptorId;

use crate::control::lease::release::LeaseReleaseHandle;
use crate::control::state::SharedState;

/// Propose `DescriptorLeaseRelease` for every lease held by `node_id`.
/// No-op if the cache has no entries for that node (idempotent vs. the
/// periodic sweep). Awaits the local applied watermark like the normal
/// release path.
pub(crate) async fn gc_leases_for_node(
    shared: &SharedState,
    node_id: u64,
) -> Result<(), crate::Error> {
    let ids: Vec<DescriptorId> = {
        let cache = shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .keys()
            .filter(|(_, holder)| *holder == node_id)
            .map(|(id, _)| id.clone())
            .collect()
    };
    if ids.is_empty() {
        return Ok(());
    }
    LeaseReleaseHandle::from_shared(shared)?
        .release_for_node(node_id, ids)
        .await
}

/// End every active drain whose proposer is no longer in the topology.
///
/// The periodic backstop for the `TopologyChange::Leave` hook: a drain end
/// that hook failed to apply is retried here. A node that is only suspected
/// Dead stays in the topology, so its drains stay: it can still be running
/// the DDL they protect. A state no boot wired has no topology and no foreign
/// proposer.
pub(crate) async fn end_orphaned_drains(shared: &SharedState) {
    let Some(topology) = shared.cluster_topology.as_ref() else {
        return;
    };
    let orphaned: Vec<u64> = {
        let topology = topology.read().unwrap_or_else(|p| p.into_inner());
        let mut proposers: Vec<u64> = shared
            .lease_drain
            .snapshot()
            .into_iter()
            .map(|(_, _, entry)| entry.proposer_node_id)
            .filter(|node_id| !topology.contains(*node_id))
            .collect();
        proposers.sort_unstable();
        proposers.dedup();
        proposers
    };
    for node_id in orphaned {
        end_drains_for_node(shared, node_id).await;
    }
}

/// Propose `DescriptorDrainEnd` for every active drain `node_id` proposed.
///
/// Runs only for a node that left the topology: it can never end its own
/// drains, and it runs no DDL any more. A drain whose end does not apply
/// stays active and is logged.
pub(crate) async fn end_drains_for_node(shared: &SharedState, node_id: u64) {
    for (id, owner) in shared.lease_drain.proposed_by(node_id) {
        if let Err(error) = super::end_drain_async(shared, id.clone(), owner.clone()).await {
            tracing::warn!(
                node_id,
                descriptor = ?id,
                ?owner,
                %error,
                "drain end for a node that left did not apply"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::{
        AppliedIndexWatcher, DescriptorId, DescriptorKind, MetadataEntry, decode_entry,
    };

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create lease gc test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("lease-gc.wal"))
                .expect("open lease gc test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct lease gc state");
        (state, directory)
    }

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    fn insert_lease(state: &SharedState, descriptor: &DescriptorId, holder: u64) {
        let now = state.hlc_clock.peek();
        state
            .metadata_cache
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .leases
            .insert(
                (descriptor.clone(), holder),
                nodedb_cluster::DescriptorLease {
                    descriptor_id: descriptor.clone(),
                    version: 1,
                    node_id: holder,
                    expires_at: nodedb_types::Hlc::new(
                        now.wall_ns.saturating_add(60_000_000_000),
                        0,
                    ),
                },
            );
    }

    /// Fake metadata raft handle: records proposed entries and bumps the
    /// applied watcher so the release path's apply wait returns at once.
    struct RecordingProposer {
        proposed: std::sync::Mutex<Vec<Vec<u8>>>,
        watcher: Arc<AppliedIndexWatcher>,
    }

    impl crate::control::metadata_proposer::MetadataRaftHandle for RecordingProposer {
        fn propose_async<'a>(
            &'a self,
            bytes: Vec<u8>,
        ) -> crate::control::metadata_proposer::ProposeFuture<'a> {
            self.proposed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(bytes);
            self.watcher.bump(1);
            Box::pin(std::future::ready(Ok(1)))
        }
    }

    #[tokio::test]
    async fn gc_leases_for_node_proposes_descriptor_lease_release() {
        let (state, _directory) = test_state();
        let watcher = state.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID);
        let proposer = Arc::new(RecordingProposer {
            proposed: std::sync::Mutex::new(Vec::new()),
            watcher: Arc::clone(&watcher),
        });
        state
            .metadata_raft
            .set(proposer.clone())
            .unwrap_or_else(|_| panic!("metadata raft handle already set in test"));

        let descriptor = id("orders");
        insert_lease(&state, &descriptor, 2);

        gc_leases_for_node(&state, 2)
            .await
            .expect("gc release for node 2");

        let proposed = proposer.proposed.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(proposed.len(), 1);
        let entry = decode_entry(&proposed[0]).expect("decode proposed entry");
        assert!(matches!(
            entry,
            MetadataEntry::DescriptorLeaseRelease {
                node_id: 2,
                ref descriptor_ids,
            } if descriptor_ids == &vec![descriptor]
        ));
    }

    #[tokio::test]
    async fn gc_leases_for_node_noop_when_no_entries() {
        let (state, _directory) = test_state();
        let watcher = state.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID);
        let proposer = Arc::new(RecordingProposer {
            proposed: std::sync::Mutex::new(Vec::new()),
            watcher: Arc::clone(&watcher),
        });
        state
            .metadata_raft
            .set(proposer.clone())
            .unwrap_or_else(|_| panic!("metadata raft handle already set in test"));

        // No leases at all for node 2 (or anyone): must not propose.
        gc_leases_for_node(&state, 2).await.expect("gc noop");
        assert!(
            proposer
                .proposed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
        );

        // Leases held by OTHER nodes are also not this node's GC target.
        insert_lease(&state, &id("other"), 3);
        gc_leases_for_node(&state, 2)
            .await
            .expect("gc noop for foreign-only leases");
        assert!(
            proposer
                .proposed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
        );
    }
}
