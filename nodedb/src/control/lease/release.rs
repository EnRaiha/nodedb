// SPDX-License-Identifier: BUSL-1.1

//! Batched descriptor lease release for explicit shutdown, admission
//! rollback, lease GC, and the background releaser.

use std::sync::Arc;

use nodedb_cluster::{AppliedIndexWatcher, DescriptorId, MetadataEntry};

use crate::control::lease::LeaseRefCount;
use crate::control::state::SharedState;
use crate::error::Error;

/// Owned release capability: the node identity and the cloneable metadata
/// handles a release needs, without the whole `SharedState`.
pub(crate) struct LeaseReleaseHandle {
    node_id: u64,
    metadata_raft: Arc<dyn crate::control::metadata_proposer::MetadataRaftHandle>,
    applied_watcher: Arc<AppliedIndexWatcher>,
    grant_gate: Arc<tokio::sync::Mutex<()>>,
    refcounts: Arc<LeaseRefCount>,
}

impl LeaseReleaseHandle {
    /// Capture the release capability of `shared`. Fails before `start_raft`
    /// installed the metadata raft handle.
    pub(crate) fn from_shared(shared: &SharedState) -> Result<Self, Error> {
        Ok(Self {
            node_id: shared.node_id,
            metadata_raft: Arc::clone(shared.metadata_raft_handle()?),
            applied_watcher: shared.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID),
            grant_gate: Arc::clone(&shared.lease_grant_gate),
            refcounts: Arc::clone(&shared.lease_refcount),
        })
    }

    /// Explicit release for shutdown, the public API, and tests. It is
    /// unconditional, but cannot race a grant because both operations hold the
    /// same gate through metadata apply.
    pub(crate) async fn release(&self, descriptor_ids: Vec<DescriptorId>) -> Result<(), Error> {
        let _grant_gate = self.grant_gate.lock().await;
        self.release_raw_for_node(self.node_id, descriptor_ids)
            .await
    }

    /// Release only descriptors that remain unheld when the grant gate is
    /// acquired. A new admission reserves its refcount before taking this gate,
    /// so a queued release skips that descriptor. Conversely, an admission that
    /// arrives after release waits for the gate, cache-rechecks, and re-grants.
    pub(crate) async fn release_if_unheld(
        &self,
        descriptor_ids: Vec<DescriptorId>,
    ) -> Result<(), Error> {
        let _grant_gate = self.grant_gate.lock().await;
        let unheld = descriptor_ids
            .into_iter()
            .filter(|id| self.refcounts.current(id) == 0)
            .collect();
        self.release_raw_for_node(self.node_id, unheld).await
    }

    /// Release leases held by an ARBITRARY node. Used by lease GC for
    /// nodes that left the topology (crashed/decommissioned). Does NOT take
    /// `grant_gate` (no contention with local grants — the foreign holder
    /// cannot grant anymore).
    pub(crate) async fn release_for_node(
        &self,
        node_id: u64,
        descriptor_ids: Vec<DescriptorId>,
    ) -> Result<(), Error> {
        self.release_raw_for_node(node_id, descriptor_ids).await
    }

    /// Raw metadata release for `node_id`. The self path holds `grant_gate`
    /// around it.
    async fn release_raw_for_node(
        &self,
        node_id: u64,
        descriptor_ids: Vec<DescriptorId>,
    ) -> Result<(), Error> {
        if descriptor_ids.is_empty() {
            return Ok(());
        }

        let entry = MetadataEntry::DescriptorLeaseRelease {
            node_id,
            descriptor_ids,
        };
        let raw = nodedb_cluster::encode_entry(&entry).map_err(|error| Error::Config {
            detail: format!("descriptor lease release encode: {error}"),
        })?;
        let log_index = self.metadata_raft.propose_async(raw).await?;
        let outcome = crate::control::metadata_proposer::wait::wait_applied(
            Arc::clone(&self.applied_watcher),
            log_index,
            super::PROPOSE_TIMEOUT,
        )
        .await?;
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
/// Proposes one `DescriptorLeaseRelease` entry and awaits the local applied
/// watermark.
pub async fn release_leases(
    shared: &SharedState,
    descriptor_ids: Vec<DescriptorId>,
) -> Result<(), Error> {
    LeaseReleaseHandle::from_shared(shared)?
        .release(descriptor_ids)
        .await
}

/// Release this node's lease on `id` when no statement holds it, because a
/// drain on `id` has started.
///
/// An idle lease stays granted after its last statement, so without this the
/// drain waits for it to expire. A held lease stays: the drain waits for
/// its statement. Called from the drain-start apply, which must not block, so
/// the background releaser proposes the release.
///
/// The check reads the raw cache entry. The self-fence and the expiry filter
/// of `lookup_lease_for_self` gate the reuse of a lease, never its release.
/// A follower applying this entry has not yet advanced its applied index past
/// it, so the fence always reports it behind the leader here.
pub(crate) fn release_idle_on_drain(shared: &SharedState, id: &DescriptorId) {
    let granted_here = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .leases
        .contains_key(&(id.clone(), shared.node_id));
    if !granted_here || shared.lease_refcount.current(id) > 0 {
        return;
    }
    shared
        .lease_runtime
        .releaser
        .submit(super::releaser::ReleaseRequest::UnheldDescriptors(vec![
            id.clone(),
        ]));
}

/// Conditionally release descriptors that have no remaining query admission.
/// Used by admission rollback, by the background releaser, and by the renewal
/// loop's release of an idle lease at expiry.
pub(crate) async fn release_unheld_leases(
    shared: &SharedState,
    descriptor_ids: Vec<DescriptorId>,
) -> Result<(), Error> {
    if descriptor_ids.is_empty() {
        return Ok(());
    }
    LeaseReleaseHandle::from_shared(shared)?
        .release_if_unheld(descriptor_ids)
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::{DescriptorId, DescriptorKind};

    use super::*;
    use crate::control::cluster::test_one_node;
    use crate::control::lease::{DEFAULT_LEASE_DURATION, acquire_lease_after_admission};

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn last_scope_release_removes_unheld_lease() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let descriptor = id("last-scope");
        state.lease_refcount.increment(&descriptor, 1);
        acquire_lease_after_admission(state, descriptor.clone(), 1, DEFAULT_LEASE_DURATION)
            .await
            .expect("grant the lease through the metadata group");

        assert_eq!(state.lease_refcount.decrement(&descriptor, 1), 0);
        LeaseReleaseHandle::from_shared(state)
            .expect("release handle")
            .release_if_unheld(vec![descriptor.clone()])
            .await
            .expect("release last scope lease");

        assert!(state.lookup_lease_for_self(&descriptor).is_none());
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn readmission_before_release_gate_check_preserves_lease() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let descriptor = id("readmitted");
        state
            .acquire_descriptor_lease(descriptor.clone(), 1, DEFAULT_LEASE_DURATION)
            .await
            .expect("grant the lease through the metadata group");

        let gate = state.lease_grant_gate.lock().await;
        let release_state = Arc::clone(state);
        let release_descriptor = descriptor.clone();
        let release = tokio::spawn(async move {
            LeaseReleaseHandle::from_shared(&release_state)?
                .release_if_unheld(vec![release_descriptor])
                .await
        });

        // The release cannot inspect refcounts until the held grant gate is
        // dropped; this reservation is therefore visible to its gate check.
        state.lease_refcount.increment(&descriptor, 1);
        drop(gate);
        release
            .await
            .expect("release task panicked")
            .expect("conditional release failed");

        assert!(state.lookup_lease_for_self(&descriptor).is_some());
        state.lease_refcount.decrement(&descriptor, 1);
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn release_first_requires_later_admission_to_regrant() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let descriptor = id("regrant");
        state
            .acquire_descriptor_lease(descriptor.clone(), 1, DEFAULT_LEASE_DURATION)
            .await
            .expect("grant the lease through the metadata group");
        LeaseReleaseHandle::from_shared(state)
            .expect("release handle")
            .release_if_unheld(vec![descriptor.clone()])
            .await
            .expect("release unheld lease");
        assert!(state.lookup_lease_for_self(&descriptor).is_none());

        // This mirrors an admission that follows release: it reserves before
        // the grant path, which cache-rechecks under the same gate and grants.
        state.lease_refcount.increment(&descriptor, 1);
        acquire_lease_after_admission(state, descriptor.clone(), 1, DEFAULT_LEASE_DURATION)
            .await
            .expect("regrant after release");

        assert!(state.lookup_lease_for_self(&descriptor).is_some());
        state.lease_refcount.decrement(&descriptor, 1);
        cluster.shutdown().await;
    }
}
