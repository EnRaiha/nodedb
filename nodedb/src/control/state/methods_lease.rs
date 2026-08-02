// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease acquisition and release methods for `SharedState`.

use super::SharedState;

impl SharedState {
    /// Acquire (or re-confirm) a descriptor lease at the given
    /// version, valid for `duration` from now. This is the public
    /// API the planner and tests use to obtain a lease before reading
    /// a descriptor.
    ///
    /// Fast path returns immediately if a non-expired lease at the
    /// requested version (or higher) is already held by this node.
    /// Slow path proposes a `MetadataEntry::DescriptorLeaseGrant`
    /// through the metadata raft group and blocks on the local
    /// applied watermark. Single-node fallback writes directly to
    /// the in-memory cache. See
    /// [`crate::control::lease::propose::acquire_lease`] for the
    /// full semantics.
    pub fn acquire_descriptor_lease(
        &self,
        descriptor_id: nodedb_cluster::DescriptorId,
        version: u64,
        duration: std::time::Duration,
    ) -> crate::Result<nodedb_cluster::DescriptorLease> {
        crate::control::lease::acquire_lease(self, descriptor_id, version, duration)
    }

    /// Release every lease this node currently holds against any
    /// of `descriptor_ids`. Used on `SIGTERM` drain and by tests.
    /// Empty input is a no-op.
    pub fn release_descriptor_leases(
        &self,
        descriptor_ids: Vec<nodedb_cluster::DescriptorId>,
    ) -> crate::Result<()> {
        crate::control::lease::release_leases(self, descriptor_ids)
    }

    /// Acquire the descriptor leases needed to execute a plan
    /// that reads the descriptors in `version_set`. Returns a
    /// [`crate::control::lease::QueryLeaseScope`] whose drop
    /// decrements each refcount and triggers a background
    /// release for any descriptor whose count hits zero.
    ///
    /// This is called by the pgwire handler AFTER planning
    /// (fresh or cache hit) and held through the query's
    /// execute phase. Multiple concurrent queries that share
    /// a descriptor all pay a single raft acquire (on the
    /// first-holder call) and a single raft release (when the
    /// last holder drops its scope).
    ///
    /// Admission is fail-closed: under the process-wide admission gate every
    /// descriptor is checked for an active drain and receives an exact-version
    /// refcount reservation. Every requested version is verified only after the
    /// gate is released, so the metadata applier can install a queued drain
    /// while a grant is waiting for raft. Any error rolls back the whole
    /// attempted admission; callers never receive a partial or unleased scope.
    ///
    /// A rejection caused by an active drain surfaces as
    /// `Error::RetryableSchemaChanged`, so client-facing callers can wrap this
    /// call and their planning call in one `retry_on_schema_change` unit and
    /// absorb a drain that starts between them. Every other failure keeps its
    /// own type and is terminal.
    pub fn acquire_plan_lease_scope(
        &self,
        version_set: &crate::control::planner::descriptor_set::DescriptorVersionSet,
    ) -> crate::Result<crate::control::lease::QueryLeaseScope> {
        use crate::control::lease::{DEFAULT_LEASE_DURATION, QueryLeaseScope};
        if version_set.is_empty() {
            return Ok(QueryLeaseScope::empty());
        }

        let mut held_versions = Vec::with_capacity(version_set.len());
        {
            // This gate only establishes admission order. It must be released
            // before any raft proposal, apply wait, or local lease installation.
            let _admission_gate = self
                .lease_admission_gate
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            for (id, version) in version_set.iter() {
                if let Err(error) = crate::control::lease::ensure_not_draining(self, id, version) {
                    drop(_admission_gate);
                    self.rollback_plan_lease_admission(held_versions);
                    return Err(error);
                }

                self.lease_refcount.increment(id, version);
                held_versions.push((id.clone(), version));
            }
        }

        // Every admitted descriptor verifies its requested version. The grant
        // gate inside this helper makes concurrent cache misses/upgrades safe.
        for (id, version) in &held_versions {
            if let Err(error) = crate::control::lease::acquire_lease_after_admission(
                self,
                id.clone(),
                *version,
                DEFAULT_LEASE_DURATION,
            ) {
                self.rollback_plan_lease_admission(held_versions.clone());
                return Err(error);
            }
        }
        Ok(QueryLeaseScope::new(held_versions, self))
    }

    /// Undo an unsuccessful multi-descriptor admission after its admission gate
    /// reservation has been released. Releasing zero-refcount descriptors
    /// synchronously prevents a failed first-holder attempt from leaving a
    /// local lease behind.
    fn rollback_plan_lease_admission(
        &self,
        descriptor_versions: Vec<(nodedb_cluster::DescriptorId, u64)>,
    ) {
        let mut to_release = Vec::new();
        for (id, version) in descriptor_versions {
            self.lease_refcount.decrement(&id, version);
            if self.lease_refcount.current(&id) == 0 {
                to_release.push(id);
            }
        }
        if let Err(error) = crate::control::lease::release::release_unheld_leases(self, to_release)
        {
            tracing::warn!(
                error = %error,
                "acquire_plan_lease_scope: rollback lease release failed"
            );
        }
    }

    /// Look up a single lease by `(descriptor_id, this_node_id)`,
    /// filtering expired records. Used by tests and by the planner
    /// to short-circuit when a fresh lease already exists. Returns
    /// `None` if absent or past expiry.
    pub fn lookup_lease_for_self(
        &self,
        descriptor_id: &nodedb_cluster::DescriptorId,
    ) -> Option<nodedb_cluster::DescriptorLease> {
        let now = self.hlc_clock.peek();
        let cache = self
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .get(&(descriptor_id.clone(), self.node_id))
            .filter(|l| l.expires_at > now)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use nodedb_cluster::{DescriptorId, DescriptorKind};
    use nodedb_types::Hlc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::lease::DEFAULT_LEASE_DURATION;
    use crate::control::planner::descriptor_set::DescriptorVersionSet;
    use crate::wal::WalManager;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create lease admission test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("lease-admission.wal"))
                .expect("open lease admission test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct lease admission state");
        (state, directory)
    }

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    fn install_drain(state: &SharedState, descriptor_id: DescriptorId, up_to_version: u64) {
        let _gate = state
            .lease_admission_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state
            .lease_drain
            .install_start(descriptor_id, up_to_version, Hlc::new(u64::MAX, 0));
    }

    #[tokio::test]
    async fn cached_valid_lease_with_active_drain_rejects() {
        let (state, _directory) = test_state();
        let descriptor = id("cached");
        let lease = state.acquire_descriptor_lease(descriptor.clone(), 1, DEFAULT_LEASE_DURATION);
        assert!(lease.is_ok());
        install_drain(&state, descriptor.clone(), 1);

        let result = state.acquire_descriptor_lease(descriptor, 1, Duration::from_secs(1));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn non_first_holder_drain_rejects_without_count_change() {
        let (state, _directory) = test_state();
        let descriptor = id("shared");
        state.lease_refcount.increment(&descriptor, 1);
        install_drain(&state, descriptor.clone(), 1);

        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);
        assert!(state.acquire_plan_lease_scope(&versions).is_err());
        assert_eq!(state.lease_refcount.current(&descriptor), 1);
    }

    #[tokio::test]
    async fn drain_rejection_is_typed_retryable() {
        use crate::control::server::shared::retry::RetryableSchemaChange;

        let (state, _directory) = test_state();
        let descriptor = id("draining");
        install_drain(&state, descriptor.clone(), 1);

        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);
        let error = state
            .acquire_plan_lease_scope(&versions)
            .err()
            .expect("drain rejects admission");
        let retryable = error
            .retryable_descriptor()
            .expect("drain rejection must be retryable");
        assert!(
            retryable.contains("draining"),
            "descriptor identity lost: {retryable}"
        );
        assert_eq!(state.lease_refcount.current(&descriptor), 0);
    }

    #[tokio::test]
    async fn admission_without_drain_is_not_retryable() {
        use crate::control::server::shared::retry::RetryableSchemaChange;

        let (state, _directory) = test_state();
        let descriptor = id("healthy");
        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);

        // No drain installed: admission succeeds, so nothing is reclassified
        // as a retryable schema change.
        let scope = state
            .acquire_plan_lease_scope(&versions)
            .expect("admission succeeds without a drain");
        assert_eq!(scope.len(), 1);
        assert!(
            crate::Error::Config {
                detail: "unrelated lease failure".into(),
            }
            .retryable_descriptor()
            .is_none()
        );
    }

    #[tokio::test]
    async fn multi_descriptor_partial_failure_restores_counts() {
        let (state, _directory) = test_state();
        let admitted = id("admitted");
        let drained = id("drained");
        install_drain(&state, drained.clone(), 1);
        let mut versions = DescriptorVersionSet::new();
        versions.record(admitted.clone(), 1);
        versions.record(drained.clone(), 1);

        assert!(state.acquire_plan_lease_scope(&versions).is_err());
        assert_eq!(state.lease_refcount.current(&admitted), 0);
        assert_eq!(state.lease_refcount.current(&drained), 0);
        assert!(state.lookup_lease_for_self(&admitted).is_none());
    }

    #[tokio::test]
    async fn concurrent_version_admissions_upgrade_the_held_lease() {
        let (state, _directory) = test_state();
        let descriptor = id("upgrading");
        let mut v1 = DescriptorVersionSet::new();
        v1.record(descriptor.clone(), 1);
        let v1_scope = state
            .acquire_plan_lease_scope(&v1)
            .expect("admit version-one plan");

        let mut v2 = DescriptorVersionSet::new();
        v2.record(descriptor.clone(), 2);
        let v2_scope = state
            .acquire_plan_lease_scope(&v2)
            .expect("admit version-two plan while version one is held");

        let lease = state
            .lookup_lease_for_self(&descriptor)
            .expect("upgraded lease installed");
        assert!(lease.version >= 2);
        assert_eq!(state.lease_refcount.current(&descriptor), 2);

        drop(v2_scope);
        assert_eq!(state.lease_refcount.current(&descriptor), 1);
        drop(v1_scope);
    }

    #[tokio::test]
    async fn drain_gate_wins_over_waiting_admission_without_refcount() {
        let (state, _directory) = test_state();
        let descriptor = id("race");
        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);

        let gate = state
            .lease_admission_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let waiting_state = Arc::clone(&state);
        let admission =
            std::thread::spawn(move || waiting_state.acquire_plan_lease_scope(&versions));
        state
            .lease_drain
            .install_start(descriptor.clone(), 1, Hlc::new(u64::MAX, 0));
        drop(gate);

        match admission.join() {
            Ok(result) => assert!(result.is_err()),
            Err(_) => panic!("waiting admission thread panicked"),
        }
        assert_eq!(state.lease_refcount.current(&descriptor), 0);
        assert!(state.lookup_lease_for_self(&descriptor).is_none());
    }
}
