// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease garbage collection for nodes that left the cluster.
//!
//! A crashed node's leases are never TTL-pruned from `MetadataCache.leases`
//! (only a `DescriptorLeaseRelease` entry removes them), so every DDL drain
//! on those descriptors times out forever. Two triggers run this module:
//! the `TopologyChange::Leave` apply hook (immediate) and the metadata
//! leader's periodic sweep (safety net).

use nodedb_cluster::DescriptorId;

use crate::control::lease::release::LeaseReleaseHandle;
use crate::control::state::SharedState;

/// Collect `(node_id, descriptor_ids)` for every lease holder that is no
/// longer a cluster member. Missing topology → empty (never GC on guesswork).
///
/// Host-side sibling of the cluster-side sweep's pure collector
/// (`nodedb-cluster::raft_loop::lease_gc::collect_non_member_lease_releases`);
/// kept here for the GC API surface and exercised by unit tests — the
/// production sweep path lives in the cluster crate, which cannot depend on
/// this one.
#[allow(dead_code)]
pub(crate) fn collect_non_member_leases(
    shared: &SharedState,
) -> Vec<(u64, Vec<DescriptorId>)> {
    let Some(topo) = &shared.cluster_topology else {
        return Vec::new();
    };
    let cache = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let topo = topo.read().unwrap_or_else(|p| p.into_inner());

    let mut by_holder: std::collections::HashMap<u64, Vec<DescriptorId>> =
        std::collections::HashMap::new();
    for (id, holder) in cache.leases.keys() {
        if !topo.contains(*holder) {
            by_holder.entry(*holder).or_default().push(id.clone());
        }
    }
    let mut out: Vec<(u64, Vec<DescriptorId>)> = by_holder.into_iter().collect();
    out.sort_by_key(|(node_id, _)| *node_id);
    out
}

/// Propose `DescriptorLeaseRelease` for every lease held by `node_id`.
/// No-op if the cache has no entries for that node (idempotent vs. the
/// periodic sweep). Blocks on the local applied watermark like the
/// normal release path; callers on hot paths must spawn this.
pub(crate) fn gc_leases_for_node(
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
    LeaseReleaseHandle::from_shared(shared).release_for_node(node_id, ids)
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

    fn topo_with(ids: &[u64]) -> nodedb_cluster::ClusterTopology {
        let mut t = nodedb_cluster::ClusterTopology::new();
        for (i, id) in ids.iter().enumerate() {
            let addr: std::net::SocketAddr =
                format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            t.add_node(nodedb_cluster::NodeInfo::new(
                *id,
                addr,
                nodedb_cluster::NodeState::Active,
            ));
        }
        t
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
                    expires_at: nodedb_types::Hlc::new(now.wall_ns.saturating_add(60_000_000_000), 0),
                },
            );
    }

    #[test]
    fn collect_non_member_leases_returns_only_foreign_holders() {
        let (state, _directory) = test_state();
        let mut state = state;
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = id("orders");

        insert_lease(&state, &descriptor, 1); // member — must be kept
        insert_lease(&state, &descriptor, 2); // not in topology — GC target

        let collected = collect_non_member_leases(&state);
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].0, 2);
        assert_eq!(collected[0].1, vec![descriptor]);
    }

    #[test]
    fn collect_non_member_leases_empty_without_topology() {
        let (state, _directory) = test_state();
        // `cluster_topology` is None in single-node mode: never GC on guesswork.
        let descriptor = id("orders");
        insert_lease(&state, &descriptor, 99);

        assert!(collect_non_member_leases(&state).is_empty());
    }

    /// Fake metadata raft handle: records proposed entries and bumps the
    /// applied watcher so the release path's `wait_for` returns immediately.
    struct RecordingProposer {
        proposed: std::sync::Mutex<Vec<Vec<u8>>>,
        watcher: Arc<AppliedIndexWatcher>,
    }

    impl crate::control::metadata_proposer::MetadataRaftHandle for RecordingProposer {
        fn propose(&self, bytes: Vec<u8>) -> Result<u64, crate::Error> {
            self.proposed.lock().unwrap_or_else(|p| p.into_inner()).push(bytes);
            self.watcher.bump(1);
            Ok(1)
        }
    }

    #[test]
    fn gc_leases_for_node_proposes_descriptor_lease_release() {
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

        gc_leases_for_node(&state, 2).expect("gc release for node 2");

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

    #[test]
    fn gc_leases_for_node_noop_when_no_entries() {
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
        gc_leases_for_node(&state, 2).expect("gc noop");
        assert!(proposer.proposed.lock().unwrap_or_else(|p| p.into_inner()).is_empty());

        // Leases held by OTHER nodes are also not this node's GC target.
        insert_lease(&state, &id("other"), 3);
        gc_leases_for_node(&state, 2).expect("gc noop for foreign-only leases");
        assert!(proposer.proposed.lock().unwrap_or_else(|p| p.into_inner()).is_empty());
    }
}
