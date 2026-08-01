// SPDX-License-Identifier: BUSL-1.1

//! Batched descriptor lease release for explicit shutdown and query-scope drop.

use std::sync::{Arc, Mutex, RwLock};

use nodedb_cluster::{AppliedIndexWatcher, DescriptorId, MetadataCache, MetadataEntry};

use crate::control::lease::LeaseRefCount;
use crate::control::state::SharedState;
use crate::error::Error;

/// Owned release capability captured when a query scope is admitted.
///
/// It deliberately contains only the immutable node identity and cloneable
/// metadata handles needed for release. Query scopes therefore do not keep the
/// whole `SharedState` alive and do not require a `Weak<SharedState>` (which
/// would prevent bootstrap-time `Arc::get_mut` wiring).
pub(crate) struct LeaseReleaseHandle {
    node_id: u64,
    metadata_cache: Arc<RwLock<MetadataCache>>,
    metadata_raft: Option<Arc<dyn crate::control::metadata_proposer::MetadataRaftHandle>>,
    applied_watcher: Arc<AppliedIndexWatcher>,
    grant_gate: Arc<Mutex<()>>,
    refcounts: Arc<LeaseRefCount>,
}

impl LeaseReleaseHandle {
    pub(crate) fn from_shared(shared: &SharedState) -> Self {
        Self {
            node_id: shared.node_id,
            metadata_cache: Arc::clone(&shared.metadata_cache),
            metadata_raft: shared.metadata_raft.get().cloned(),
            applied_watcher: shared.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID),
            grant_gate: Arc::clone(&shared.lease_grant_gate),
            refcounts: Arc::clone(&shared.lease_refcount),
        }
    }

    /// Explicit release for shutdown, the public API, and tests. It is
    /// unconditional, but cannot race a grant because both operations hold the
    /// same gate through metadata apply.
    pub(crate) fn release(&self, descriptor_ids: Vec<DescriptorId>) -> Result<(), Error> {
        let _grant_gate = self
            .grant_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        self.release_raw(descriptor_ids)
    }

    /// Release only descriptors that remain unheld when the grant gate is
    /// acquired. A new admission reserves its refcount before taking this gate,
    /// so a queued release skips that descriptor. Conversely, an admission that
    /// arrives after release waits for the gate, cache-rechecks, and re-grants.
    ///
    /// This is called from `QueryLeaseScope`'s blocking worker, so its applied
    /// watcher wait is intentionally direct rather than wrapped in
    /// `tokio::task::block_in_place`.
    pub(crate) fn release_if_unheld(&self, descriptor_ids: Vec<DescriptorId>) -> Result<(), Error> {
        let _grant_gate = self
            .grant_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let unheld = descriptor_ids
            .into_iter()
            .filter(|id| self.refcounts.current(id) == 0)
            .collect();
        self.release_raw(unheld)
    }

    /// Raw metadata release. The caller must hold `grant_gate`.
    fn release_raw(&self, descriptor_ids: Vec<DescriptorId>) -> Result<(), Error> {
        if descriptor_ids.is_empty() {
            return Ok(());
        }

        let Some(metadata_raft) = &self.metadata_raft else {
            let mut cache = self
                .metadata_cache
                .write()
                .unwrap_or_else(|poison| poison.into_inner());
            for id in descriptor_ids {
                cache.leases.remove(&(id, self.node_id));
            }
            return Ok(());
        };

        let entry = MetadataEntry::DescriptorLeaseRelease {
            node_id: self.node_id,
            descriptor_ids,
        };
        let raw = nodedb_cluster::encode_entry(&entry).map_err(|error| Error::Config {
            detail: format!("descriptor lease release encode: {error}"),
        })?;
        let log_index = metadata_raft.propose(raw)?;
        let outcome = self
            .applied_watcher
            .wait_for(log_index, super::PROPOSE_TIMEOUT);
        if !outcome.is_reached() {
            return Err(Error::Config {
                detail: format!(
                    "descriptor lease release did not apply within {:?} \
                     (log index {log_index}, current: {}, outcome: {outcome:?})",
                    super::PROPOSE_TIMEOUT,
                    self.applied_watcher.current()
                ),
            });
        }
        Ok(())
    }
}

/// Release every lease this node currently holds against any of
/// `descriptor_ids`. Empty input is a no-op.
///
/// Cluster mode proposes one `DescriptorLeaseRelease` entry and waits for the
/// local applied watermark. Single-node mode removes the local cache entries.
pub fn release_leases(
    shared: &SharedState,
    descriptor_ids: Vec<DescriptorId>,
) -> Result<(), Error> {
    let releaser = LeaseReleaseHandle::from_shared(shared);
    if shared.metadata_raft.get().is_none() {
        return releaser.release(descriptor_ids);
    }
    // `AppliedIndexWatcher::wait_for` parks on a Condvar. Preserve the prior
    // cluster-path behavior by yielding this Tokio worker while it waits.
    tokio::task::block_in_place(|| releaser.release(descriptor_ids))
}

/// Conditionally release descriptors that have no remaining query admission.
/// This synchronous rollback path preserves the public release wrapper's Tokio
/// behavior; query-scope drop invokes `LeaseReleaseHandle::release_if_unheld`
/// from its own blocking worker instead.
pub(crate) fn release_unheld_leases(
    shared: &SharedState,
    descriptor_ids: Vec<DescriptorId>,
) -> Result<(), Error> {
    let releaser = LeaseReleaseHandle::from_shared(shared);
    if shared.metadata_raft.get().is_none() {
        return releaser.release_if_unheld(descriptor_ids);
    }
    tokio::task::block_in_place(|| releaser.release_if_unheld(descriptor_ids))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::{DescriptorId, DescriptorKind};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::lease::{DEFAULT_LEASE_DURATION, acquire_lease_after_admission};
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create lease release test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("lease-release.wal"))
                .expect("open lease release test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct lease release state");
        (state, directory)
    }

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    #[tokio::test]
    async fn last_scope_release_removes_unheld_lease() {
        let (state, _directory) = test_state();
        let descriptor = id("last-scope");
        state.lease_refcount.increment(&descriptor, 1);
        acquire_lease_after_admission(&state, descriptor.clone(), 1, DEFAULT_LEASE_DURATION)
            .expect("install single-node lease");

        assert_eq!(state.lease_refcount.decrement(&descriptor, 1), 0);
        LeaseReleaseHandle::from_shared(&state)
            .release_if_unheld(vec![descriptor.clone()])
            .expect("release last scope lease");

        assert!(state.lookup_lease_for_self(&descriptor).is_none());
    }

    #[tokio::test]
    async fn readmission_before_release_gate_check_preserves_lease() {
        let (state, _directory) = test_state();
        let descriptor = id("readmitted");
        state
            .acquire_descriptor_lease(descriptor.clone(), 1, DEFAULT_LEASE_DURATION)
            .expect("install single-node lease");

        let gate = state
            .lease_grant_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let release_state = Arc::clone(&state);
        let release_descriptor = descriptor.clone();
        let release = std::thread::spawn(move || {
            LeaseReleaseHandle::from_shared(&release_state)
                .release_if_unheld(vec![release_descriptor])
        });

        // The release cannot inspect refcounts until the held grant gate is
        // dropped; this reservation is therefore visible to its gate check.
        state.lease_refcount.increment(&descriptor, 1);
        drop(gate);
        release
            .join()
            .expect("release thread panicked")
            .expect("conditional release failed");

        assert!(state.lookup_lease_for_self(&descriptor).is_some());
        state.lease_refcount.decrement(&descriptor, 1);
    }

    #[tokio::test]
    async fn release_first_requires_later_admission_to_regrant() {
        let (state, _directory) = test_state();
        let descriptor = id("regrant");
        state
            .acquire_descriptor_lease(descriptor.clone(), 1, DEFAULT_LEASE_DURATION)
            .expect("install single-node lease");
        LeaseReleaseHandle::from_shared(&state)
            .release_if_unheld(vec![descriptor.clone()])
            .expect("release unheld lease");
        assert!(state.lookup_lease_for_self(&descriptor).is_none());

        // This mirrors an admission that follows release: it reserves before
        // the grant path, which cache-rechecks under the same gate and grants.
        state.lease_refcount.increment(&descriptor, 1);
        acquire_lease_after_admission(&state, descriptor.clone(), 1, DEFAULT_LEASE_DURATION)
            .expect("regrant after release");

        assert!(state.lookup_lease_for_self(&descriptor).is_some());
        state.lease_refcount.decrement(&descriptor, 1);
    }
}
