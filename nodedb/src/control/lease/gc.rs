// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease garbage collection for nodes that left the cluster.
//!
//! A crashed node's leases are never TTL-pruned from `MetadataCache.leases`
//! (only a `DescriptorLeaseRelease` entry removes them), so every DDL drain
//! on those descriptors times out forever. Three triggers run this module:
//! the SWIM-Dead subscriber hook (immediate on confirmed crash),
//! the `TopologyChange::Leave` apply hook (immediate on graceful leave) and
//! the metadata leader's periodic sweep (safety net).

use std::sync::{Arc, Weak};

use nodedb_cluster::{DescriptorId, DescriptorLease, MemberState, MembershipSubscriber};
use nodedb_types::NodeId;
use tracing::{debug, warn};

use crate::control::lease::drain_propose::MAX_SKEW_NS;
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
pub(crate) fn collect_non_member_leases(shared: &SharedState) -> Vec<(u64, Vec<DescriptorId>)> {
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
pub(crate) fn gc_leases_for_node(shared: &SharedState, node_id: u64) -> Result<(), crate::Error> {
    gc_leases_for_node_if(shared, node_id, |_| true)
}

/// Like [`gc_leases_for_node`], but only for leases accepted by `filter`.
///
/// The filter runs on each lease value before the release proposal, so a
/// caller (e.g. the SWIM-Dead hook) can apply a grace window without
/// duplicating the collection logic. [`gc_leases_for_node`] is the
/// always-true case.
pub(crate) fn gc_leases_for_node_if<F>(
    shared: &SharedState,
    node_id: u64,
    filter: F,
) -> Result<(), crate::Error>
where
    F: Fn(&DescriptorLease) -> bool,
{
    let ids: Vec<DescriptorId> = {
        let cache = shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .iter()
            .filter(|(_, l)| l.node_id == node_id && filter(l))
            .map(|((id, _), _)| id.clone())
            .collect()
    };
    if ids.is_empty() {
        return Ok(());
    }
    LeaseReleaseHandle::from_shared(shared).release_for_node(node_id, ids)
}

// ---------------------------------------------------------------------------
// SWIM-Dead → lease release hook
// ---------------------------------------------------------------------------

/// Parse a SWIM `NodeId` (a decimal string in production) into its numeric
/// form. `seed:…` placeholder ids and any other non-numeric id are skipped
/// with a debug log — they cannot name a lease holder.
fn parse_swim_node_id(node_id: &NodeId) -> Option<u64> {
    match node_id.as_str().parse::<u64>() {
        Ok(n) => Some(n),
        Err(_) => {
            debug!(node_id = %node_id, "lease GC: skipping non-numeric SWIM node id");
            None
        }
    }
}

/// Grace-window predicate: release only leases expiring within [`MAX_SKEW_NS`]
/// of `now` (including already-expired ones). A false-positive Dead (a
/// live-but-partitioned node) must not yank a fresh lease; leases beyond the
/// window are left to the periodic sweep, which only acts once the node
/// remains a non-member.
fn near_expiry_filter(now_wall_ns: u64, expires_at: &nodedb_types::Hlc) -> bool {
    expires_at.wall_ns <= now_wall_ns.saturating_add(MAX_SKEW_NS)
}

/// Subscriber that triggers lease GC the moment SWIM confirms a member Dead.
///
/// Mirrors the `TopologyChange::Leave` apply hook in `dispatch.rs`: the GC is
/// spawned, never run inline (an inline propose-and-wait would deadlock the
/// applied-index watcher), gated on [`SharedState::is_singleton_worker`], and
/// the hook holds only a `Weak<SharedState>` so it never keeps the process
/// alive at shutdown.
pub(crate) struct LeaseGcOnCrashHook {
    shared: Weak<SharedState>,
}

impl LeaseGcOnCrashHook {
    pub(crate) fn new(shared: &Arc<SharedState>) -> Self {
        Self {
            shared: Arc::downgrade(shared),
        }
    }

    /// Pure decision: which node, if any, this transition should trigger GC
    /// for. Split out for deterministic unit testing without spawning.
    fn should_release(&self, node_id: &NodeId, new: MemberState) -> Option<u64> {
        if new != MemberState::Dead {
            return None;
        }
        parse_swim_node_id(node_id)
    }
}

impl MembershipSubscriber for LeaseGcOnCrashHook {
    fn on_state_change(&self, node_id: &NodeId, _old: Option<MemberState>, new: MemberState) {
        let Some(node_id) = self.should_release(node_id, new) else {
            return;
        };
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        tokio::spawn(async move {
            // Only the metadata-leader-or-standalone worker performs GC
            // (mirrors the Leave hook's guard).
            if !shared.is_singleton_worker() {
                return;
            }
            let now_wall_ns = crate::control::lease::wall_now_ns();
            if let Err(e) = gc_leases_for_node_if(&shared, node_id, |l| {
                near_expiry_filter(now_wall_ns, &l.expires_at)
            }) {
                warn!(
                    node_id,
                    error = %e,
                    "lease GC after SWIM Dead failed; periodic sweep will retry"
                );
            }
        });
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

    fn topo_with(ids: &[u64]) -> nodedb_cluster::ClusterTopology {
        let mut t = nodedb_cluster::ClusterTopology::new();
        for (i, id) in ids.iter().enumerate() {
            let addr: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
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
                    expires_at: nodedb_types::Hlc::new(
                        now.wall_ns.saturating_add(60_000_000_000),
                        0,
                    ),
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
            self.proposed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(bytes);
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
        assert!(
            proposer
                .proposed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
        );

        // Leases held by OTHER nodes are also not this node's GC target.
        insert_lease(&state, &id("other"), 3);
        gc_leases_for_node(&state, 2).expect("gc noop for foreign-only leases");
        assert!(
            proposer
                .proposed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
        );
    }

    #[test]
    fn parse_swim_node_id_numeric_only() {
        let numeric = nodedb_types::NodeId::try_new("42").expect("valid id");
        assert_eq!(parse_swim_node_id(&numeric), Some(42));

        let seed = nodedb_types::NodeId::try_new("seed:127.0.0.1:9000").expect("valid id");
        assert_eq!(parse_swim_node_id(&seed), None);

        let junk = nodedb_types::NodeId::try_new("not-a-number").expect("valid id");
        assert_eq!(parse_swim_node_id(&junk), None);
    }

    #[test]
    fn near_expiry_filter_uses_max_skew_window() {
        let now = 1_000_000_000_000_000u64;
        // Already expired → within the window → release.
        assert!(near_expiry_filter(now, &nodedb_types::Hlc::new(now - 100_000_000, 0)));
        // Expiring exactly at the window edge → release.
        assert!(near_expiry_filter(now, &nodedb_types::Hlc::new(now + MAX_SKEW_NS, 0)));
        // Expiring beyond the window → spare (sweep's job).
        assert!(!near_expiry_filter(now, &nodedb_types::Hlc::new(now + MAX_SKEW_NS + 1, 0)));
    }

    #[test]
    fn hook_releases_only_on_dead() {
        let (state, _directory) = test_state();
        let hook = LeaseGcOnCrashHook::new(&state);
        let node = nodedb_types::NodeId::try_new("42").expect("valid id");

        assert_eq!(hook.should_release(&node, MemberState::Alive), None);
        assert_eq!(hook.should_release(&node, MemberState::Suspect), None);
        assert_eq!(hook.should_release(&node, MemberState::Left), None);
        assert_eq!(hook.should_release(&node, MemberState::Dead), Some(42));

        // Dead with a non-numeric id (seed placeholder) → no target.
        let seed = nodedb_types::NodeId::try_new("seed:127.0.0.1:9000").expect("valid id");
        assert_eq!(hook.should_release(&seed, MemberState::Dead), None);
    }

    /// The grace-window filter is applied at collection time: a near-expiry
    /// lease of the dead node is released, a fresh lease held by the same
    /// node is spared.
    #[test]
    fn gc_leases_for_node_if_applies_grace_window() {
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

        let now_wall_ns = crate::control::lease::wall_now_ns();
        let near = id("near");
        state
            .metadata_cache
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .leases
            .insert(
                (near.clone(), 42),
                nodedb_cluster::DescriptorLease {
                    descriptor_id: near.clone(),
                    version: 1,
                    node_id: 42,
                    // Already expired in wall time → inside the grace window.
                    expires_at: nodedb_types::Hlc::new(now_wall_ns, 0),
                },
            );
        let fresh = id("fresh");
        state
            .metadata_cache
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .leases
            .insert(
                (fresh.clone(), 42),
                nodedb_cluster::DescriptorLease {
                    descriptor_id: fresh.clone(),
                    version: 1,
                    node_id: 42,
                    // 1000s ahead → beyond MAX_SKEW → spared.
                    expires_at: nodedb_types::Hlc::new(now_wall_ns.saturating_add(1_000_000_000_000), 0),
                },
            );

        gc_leases_for_node_if(&state, 42, |l| near_expiry_filter(now_wall_ns, &l.expires_at))
            .expect("grace-window GC");

        let proposed = proposer.proposed.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(proposed.len(), 1);
        let entry = decode_entry(&proposed[0]).expect("decode proposed entry");
        assert!(matches!(
            entry,
            MetadataEntry::DescriptorLeaseRelease {
                node_id: 42,
                ref descriptor_ids,
            } if descriptor_ids == &vec![near]
        ));
    }
}
