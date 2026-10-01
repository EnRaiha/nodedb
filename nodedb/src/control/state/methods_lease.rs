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
    /// through the metadata raft group and awaits the local
    /// applied watermark. See
    /// [`crate::control::lease::propose::acquire_lease`] for the
    /// full semantics.
    pub async fn acquire_descriptor_lease(
        &self,
        descriptor_id: nodedb_cluster::DescriptorId,
        version: u64,
        duration: std::time::Duration,
    ) -> crate::Result<nodedb_cluster::DescriptorLease> {
        crate::control::lease::acquire_lease(self, descriptor_id, version, duration).await
    }

    /// Release every lease this node currently holds against any
    /// of `descriptor_ids`. Used on `SIGTERM` drain and by tests.
    /// Empty input is a no-op.
    pub async fn release_descriptor_leases(
        &self,
        descriptor_ids: Vec<nodedb_cluster::DescriptorId>,
    ) -> crate::Result<()> {
        crate::control::lease::release_leases(self, descriptor_ids).await
    }

    /// Acquire the descriptor leases needed to execute a plan
    /// that reads the descriptors in `version_set`. Returns a
    /// [`crate::control::lease::QueryLeaseScope`] whose drop
    /// decrements each refcount. The lease stays granted for reuse.
    ///
    /// This is called by the pgwire handler AFTER planning
    /// (fresh or cache hit) and held through the query's
    /// execute phase. Multiple concurrent queries that share
    /// a descriptor all pay a single raft acquire (on the
    /// first-holder call).
    ///
    /// Admission is fail-closed: under the process-wide admission gate every
    /// descriptor is checked for an active drain and receives an exact-version
    /// refcount reservation. Every requested version is verified only after the
    /// gate is released, so the metadata applier can install a queued drain
    /// while a grant is waiting for raft. Any error rolls back the whole
    /// attempted admission; callers never receive a partial or unleased scope.
    /// A cancelled admission gives its reservations back and hands the release
    /// of every idle lease to the background releaser.
    ///
    /// A rejection caused by an active drain surfaces as
    /// `Error::RetryableSchemaChanged`, so client-facing callers can wrap this
    /// call and their planning call in one `retry_on_schema_change` unit and
    /// absorb a drain that starts between them. Every other failure keeps its
    /// own type and is terminal.
    pub async fn acquire_plan_lease_scope(
        &self,
        version_set: &crate::control::planner::descriptor_set::DescriptorVersionSet,
    ) -> crate::Result<crate::control::lease::QueryLeaseScope> {
        use crate::control::lease::admission::PendingAdmission;
        use crate::control::lease::{DEFAULT_LEASE_DURATION, QueryLeaseScope};
        if version_set.is_empty() {
            return Ok(QueryLeaseScope::empty());
        }

        let mut admission = PendingAdmission::new(self);
        let drained = {
            // This gate only establishes admission order. It is released
            // before any raft proposal, apply wait, or local lease installation.
            let _admission_gate = self
                .lease_admission_gate
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let mut drained = None;
            for (id, version) in version_set.iter() {
                if let Err(error) = crate::control::lease::ensure_not_draining(self, id, version) {
                    drained = Some(error);
                    break;
                }
                admission.reserve(id, version);
            }
            drained
        };
        if let Some(error) = drained {
            admission.rollback().await;
            return Err(error);
        }

        // Every admitted descriptor verifies its requested version. The grant
        // gate inside this helper makes concurrent cache misses/upgrades safe.
        let held = admission.held().to_vec();
        for (id, version) in held {
            if let Err(error) = crate::control::lease::acquire_lease_after_admission(
                self,
                id,
                version,
                DEFAULT_LEASE_DURATION,
            )
            .await
            {
                admission.rollback().await;
                return Err(error);
            }
        }
        admission.into_scope().await
    }

    /// Look up a single lease by `(descriptor_id, this_node_id)`,
    /// filtering expired records. Used by tests and by the planner
    /// to short-circuit when a fresh lease already exists. Returns
    /// `None` if absent, past expiry, or self-fenced: metadata-leader
    /// contact is older than `LEASE_SELF_FENCE_WINDOW`.
    pub fn lookup_lease_for_self(
        &self,
        descriptor_id: &nodedb_cluster::DescriptorId,
    ) -> Option<nodedb_cluster::DescriptorLease> {
        if crate::control::lease::lease_use_is_fenced(self) {
            return None;
        }
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
        state.lease_drain.install_start(
            descriptor_id,
            nodedb_cluster::DrainOwner::Ddl,
            up_to_version,
            Hlc::new(u64::MAX, 0),
            state.node_id,
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cached_valid_lease_with_active_drain_rejects() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let descriptor = id("cached");
        let lease = state
            .acquire_descriptor_lease(descriptor.clone(), 1, DEFAULT_LEASE_DURATION)
            .await;
        assert!(lease.is_ok());
        install_drain(state, descriptor.clone(), 1);

        let result = state
            .acquire_descriptor_lease(descriptor, 1, Duration::from_secs(1))
            .await;
        assert!(result.is_err());
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn non_first_holder_drain_rejects_without_count_change() {
        let (state, _directory) = test_state();
        let descriptor = id("shared");
        state.lease_refcount.increment(&descriptor, 1);
        install_drain(&state, descriptor.clone(), 1);

        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);
        assert!(state.acquire_plan_lease_scope(&versions).await.is_err());
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
            .await
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admission_without_drain_is_not_retryable() {
        use crate::control::server::shared::retry::RetryableSchemaChange;

        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let descriptor = id("healthy");
        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);

        // No drain installed: admission succeeds, so nothing is reclassified
        // as a retryable schema change.
        let scope = state
            .acquire_plan_lease_scope(&versions)
            .await
            .expect("admission succeeds without a drain");
        assert_eq!(scope.len(), 1);
        assert!(
            crate::Error::Config {
                detail: "unrelated lease failure".into(),
            }
            .retryable_descriptor()
            .is_none()
        );
        drop(scope);
        cluster.shutdown().await;
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

        assert!(state.acquire_plan_lease_scope(&versions).await.is_err());
        assert_eq!(state.lease_refcount.current(&admitted), 0);
        assert_eq!(state.lease_refcount.current(&drained), 0);
        assert!(state.lookup_lease_for_self(&admitted).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_version_admissions_upgrade_the_held_lease() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let descriptor = id("upgrading");
        let mut v1 = DescriptorVersionSet::new();
        v1.record(descriptor.clone(), 1);
        let v1_scope = state
            .acquire_plan_lease_scope(&v1)
            .await
            .expect("admit version-one plan");

        let mut v2 = DescriptorVersionSet::new();
        v2.record(descriptor.clone(), 2);
        let v2_scope = state
            .acquire_plan_lease_scope(&v2)
            .await
            .expect("admit version-two plan while version one is held");

        let lease = state
            .lookup_lease_for_self(&descriptor)
            .expect("upgraded lease installed");
        assert!(lease.version >= 2);
        assert_eq!(state.lease_refcount.current(&descriptor), 2);

        drop(v2_scope);
        assert_eq!(state.lease_refcount.current(&descriptor), 1);
        drop(v1_scope);
        cluster.shutdown().await;
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
        // A plain thread blocks on the held admission gate, as a worker does.
        let admission = std::thread::spawn(move || {
            futures::executor::block_on(waiting_state.acquire_plan_lease_scope(&versions))
        });
        state.lease_drain.install_start(
            descriptor.clone(),
            nodedb_cluster::DrainOwner::Ddl,
            1,
            Hlc::new(u64::MAX, 0),
            state.node_id,
        );
        drop(gate);

        match admission.join() {
            Ok(result) => assert!(result.is_err()),
            Err(_) => panic!("waiting admission thread panicked"),
        }
        assert_eq!(state.lease_refcount.current(&descriptor), 0);
        assert!(state.lookup_lease_for_self(&descriptor).is_none());
    }

    /// Wait until `done` holds, for at most five seconds.
    async fn eventually(what: &str, done: impl Fn() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Admission awaits its grant, so a current-thread runtime admits a plan.
    #[tokio::test]
    async fn a_current_thread_runtime_admits_a_plan() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = Arc::clone(&cluster.state);
        let descriptor = id("current-thread");
        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);

        let scope = state
            .acquire_plan_lease_scope(&versions)
            .await
            .expect("admission succeeds on a current-thread runtime");
        assert_eq!(scope.len(), 1);
        assert!(state.lookup_lease_for_self(&descriptor).is_some());
        drop(scope);
        assert_eq!(state.lease_refcount.current(&descriptor), 0);
        drop(state);
        cluster.shutdown().await;
    }

    /// The idle lease a dropped scope leaves is released by the background
    /// releaser once a drain start hands it over.
    #[tokio::test]
    async fn a_dropped_scopes_lease_is_released_in_the_background() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = Arc::clone(&cluster.state);
        let descriptor = id("dropped-scope");
        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);
        let scope = state
            .acquire_plan_lease_scope(&versions)
            .await
            .expect("admit the plan");
        drop(scope);
        assert!(
            state.lookup_lease_for_self(&descriptor).is_some(),
            "a dropped scope leaves its lease granted for reuse"
        );

        crate::control::lease::release::release_idle_on_drain(&state, &descriptor);
        eventually("the background releaser releases the idle lease", || {
            state.lookup_lease_for_self(&descriptor).is_none()
        })
        .await;
        drop(state);
        cluster.shutdown().await;
    }

    /// A cancelled admission gives its reservations back without blocking.
    #[tokio::test]
    async fn a_cancelled_admission_gives_its_reservations_back() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = Arc::clone(&cluster.state);
        let descriptor = id("cancelled");
        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);

        // Hold the grant gate so the admission parks after it reserved.
        let gate = state.lease_grant_gate.lock().await;
        {
            let admission = state.acquire_plan_lease_scope(&versions);
            tokio::pin!(admission);
            assert!(
                futures::poll!(admission.as_mut()).is_pending(),
                "the admission waits on the grant gate"
            );
            assert_eq!(state.lease_refcount.current(&descriptor), 1);
        }
        drop(gate);
        assert_eq!(state.lease_refcount.current(&descriptor), 0);
        drop(state);
        cluster.shutdown().await;
    }
}
