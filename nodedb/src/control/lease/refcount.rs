// SPDX-License-Identifier: BUSL-1.1

//! Per-query descriptor lease refcount + scope guard.
//!
//! Descriptor leases are acquired at plan time and held
//! through execute. Two concurrent queries touching the same
//! descriptor version share a single underlying raft lease — we track
//! per-node exact-version refcounts so only a missing or lower-version lease
//! pays an acquire round-trip and only the last query across every version to
//! finish pays the release round-trip. Intermediate queries hit the fast-path
//! increment / decrement with no raft traffic.
//!
//! The DDL drain path relies on this: when every in-flight
//! query using a descriptor finishes, the refcount hits zero,
//! the lease is actually released via a
//! `DescriptorLeaseRelease` raft entry, and drain's poll loop
//! observes the lease clear. Long-running queries naturally
//! bound the drain window — if a query exceeds
//! `DEFAULT_DRAIN_TIMEOUT` the ALTER fails with a drain-timeout
//! error and the operator retries.
//!
//! ## Guard semantics
//!
//! `QueryLeaseScope` is the owned collection of leases a
//! single query accumulated during planning. The scope drops
//! when the query's pgwire handler finishes executing (after
//! every response has been returned). Drop walks the scope,
//! decrements each exact-version refcount, and — when no version of a
//! descriptor remains held — spawns a background task to propose the release
//! entry. The spawn is mandatory because `Drop` cannot
//! be async; the drop handler itself returns immediately.
//!
//! A dropped `QueryLeaseScope` therefore schedules (but does
//! not await) the release. Drain's poll loop observes the
//! release after the raft round-trip lands on the leader —
//! sub-10ms in a healthy cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_cluster::DescriptorId;
use tracing::warn;

use super::release::LeaseReleaseHandle;
use crate::control::state::SharedState;

/// Host-side lease reference counts. One entry per descriptor id and
/// descriptor version this node currently holds; the value is the number of
/// in-flight queries or admissions holding that exact version.
#[derive(Debug, Default)]
pub struct LeaseRefCount {
    counts: Mutex<HashMap<(DescriptorId, u64), u32>>,
}

impl LeaseRefCount {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the refcount for exact `(id, version)`. Returns the new
    /// exact-version count, saturating rather than overflowing.
    pub fn increment(&self, id: &DescriptorId, version: u64) -> u32 {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let entry = map.entry((id.clone(), version)).or_insert(0);
        *entry = entry.saturating_add(1);
        *entry
    }

    /// Decrement the refcount for exact `(id, version)`. Returns the new
    /// exact-version count and removes its entry when it reaches zero.
    pub fn decrement(&self, id: &DescriptorId, version: u64) -> u32 {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let key = (id.clone(), version);
        if let Some(entry) = map.get_mut(&key) {
            *entry = entry.saturating_sub(1);
            let count = *entry;
            if count == 0 {
                map.remove(&key);
            }
            count
        } else {
            0
        }
    }

    /// Read the total refcount across every held version of `id`.
    pub fn current(&self, id: &DescriptorId) -> u32 {
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.iter()
            .filter(|((held_id, _), _)| held_id == id)
            .fold(0_u32, |total, (_, count)| total.saturating_add(*count))
    }

    /// Read the total refcount for `id` at versions no greater than
    /// `up_to_version`.
    pub fn current_at_or_below(&self, id: &DescriptorId, up_to_version: u64) -> u32 {
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.iter()
            .filter(|((held_id, version), _)| held_id == id && *version <= up_to_version)
            .fold(0_u32, |total, (_, count)| total.saturating_add(*count))
    }

    /// Total number of exact descriptor-version entries with a non-zero refcount.
    pub fn distinct_count(&self) -> usize {
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.len()
    }
}

/// Owned collection of lease holds for one query.
///
/// Created by `OriginCatalog::take_lease_scope()` after
/// planning finishes; held by the pgwire handler through the
/// execute phase; released on drop.
pub struct QueryLeaseScope {
    /// Exact descriptor-version refcounts this query holds.
    descriptor_versions: Vec<(DescriptorId, u64)>,
    /// Refcount state shared independently of the process-wide state.
    refcounts: Option<Arc<LeaseRefCount>>,
    /// Minimal owned capability needed to release the underlying lease.
    releaser: Option<LeaseReleaseHandle>,
}

impl QueryLeaseScope {
    /// Create an empty scope that releases nothing on drop.
    /// Used as a default / placeholder when the caller does
    /// not need lease tracking (e.g., internal sub-planners).
    pub fn empty() -> Self {
        Self {
            descriptor_versions: Vec::new(),
            refcounts: None,
            releaser: None,
        }
    }

    /// Build a scope from exact descriptor-version holds already incremented
    /// on the node's `lease_refcount`. Only cloneable release capabilities are
    /// retained, so the scope neither owns nor weak-references `SharedState`.
    pub fn new(descriptor_versions: Vec<(DescriptorId, u64)>, shared: &SharedState) -> Self {
        Self {
            descriptor_versions,
            refcounts: Some(Arc::clone(&shared.lease_refcount)),
            releaser: Some(LeaseReleaseHandle::from_shared(shared)),
        }
    }

    /// Number of descriptors held in this scope.
    pub fn len(&self) -> usize {
        self.descriptor_versions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.descriptor_versions.is_empty()
    }
}

impl Drop for QueryLeaseScope {
    fn drop(&mut self) {
        if self.descriptor_versions.is_empty() {
            return;
        }
        let Some(refcounts) = self.refcounts.take() else {
            return;
        };
        let Some(releaser) = self.releaser.take() else {
            return;
        };
        // Decrement exact-version refcounts and collect ids whose total across
        // every version just hit zero — only those need metadata release.
        let mut to_release = Vec::new();
        for (id, version) in self.descriptor_versions.drain(..) {
            refcounts.decrement(&id, version);
            if refcounts.current(&id) == 0 {
                to_release.push(id);
            }
        }
        if to_release.is_empty() {
            return;
        }
        // Release is synchronous, so run it on Tokio's blocking pool when a
        // runtime owns this drop. Drops can also occur on non-Tokio threads
        // (notably teardown paths); then use an independent OS thread rather
        // than silently retaining the metadata lease. Both paths call the same
        // conditional release, which serializes with admissions on grant_gate.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let result =
                    tokio::task::spawn_blocking(move || releaser.release_if_unheld(to_release))
                        .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        warn!(error = %error, "QueryLeaseScope drop: background release failed");
                    }
                    Err(error) => {
                        warn!(error = %error, "QueryLeaseScope drop: spawn_blocking panicked");
                    }
                }
            });
        } else if let Err(error) = std::thread::Builder::new()
            .name("nodedb-lease-release".into())
            .spawn(move || {
                if let Err(error) = releaser.release_if_unheld(to_release) {
                    warn!(error = %error, "QueryLeaseScope drop: fallback release failed");
                }
            })
        {
            warn!(error = %error, "QueryLeaseScope drop: failed to spawn fallback release");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use nodedb_cluster::DescriptorKind;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::lease::{DEFAULT_LEASE_DURATION, acquire_lease_after_admission};
    use crate::wal::WalManager;

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    #[test]
    fn first_increment_returns_one() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        assert_eq!(rc.increment(&a, 1), 1);
    }

    #[test]
    fn second_increment_returns_two() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a, 1);
        assert_eq!(rc.increment(&a, 1), 2);
    }

    #[test]
    fn decrement_to_zero_removes_entry() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a, 1);
        assert_eq!(rc.decrement(&a, 1), 0);
        assert_eq!(rc.current(&a), 0);
        assert_eq!(rc.distinct_count(), 0);
    }

    #[test]
    fn decrement_preserves_shared_lease() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a, 1);
        rc.increment(&a, 1);
        assert_eq!(rc.decrement(&a, 1), 1);
        assert_eq!(rc.current(&a), 1);
        assert_eq!(rc.distinct_count(), 1);
    }

    #[test]
    fn distinct_descriptors_track_independently() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        let b = id("b");
        rc.increment(&a, 1);
        rc.increment(&b, 1);
        assert_eq!(rc.distinct_count(), 2);
        rc.decrement(&a, 1);
        assert_eq!(rc.distinct_count(), 1);
        assert_eq!(rc.current(&a), 0);
        assert_eq!(rc.current(&b), 1);
    }

    #[test]
    fn decrement_on_unknown_id_is_safe() {
        let rc = LeaseRefCount::new();
        assert_eq!(rc.decrement(&id("nothing"), 1), 0);
    }

    #[test]
    fn exact_version_decrement_preserves_other_version() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a, 1);
        rc.increment(&a, 2);

        assert_eq!(rc.decrement(&a, 2), 0);
        assert_eq!(rc.current(&a), 1);
        assert_eq!(rc.current_at_or_below(&a, 1), 1);
        assert_eq!(rc.current_at_or_below(&a, 2), 1);
    }

    #[test]
    fn empty_scope_drops_cleanly() {
        let scope = QueryLeaseScope::empty();
        drop(scope); // should not panic even without a runtime
    }

    #[test]
    fn no_runtime_drop_releases_last_unheld_lease() {
        let (state, descriptor, scope, _directory) = {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build temporary runtime");
            let values = runtime.block_on(async {
                let directory = tempfile::tempdir().expect("create lease release test directory");
                let wal = Arc::new(
                    WalManager::open_for_testing(&directory.path().join("lease-release.wal"))
                        .expect("open lease release test WAL"),
                );
                let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
                let state = crate::control::state::SharedState::new(dispatcher, wal)
                    .expect("construct lease release state");
                let descriptor = id("no-runtime-drop");
                state.lease_refcount.increment(&descriptor, 1);
                acquire_lease_after_admission(
                    &state,
                    descriptor.clone(),
                    1,
                    DEFAULT_LEASE_DURATION,
                )
                .expect("install single-node lease");
                let scope = QueryLeaseScope::new(vec![(descriptor.clone(), 1)], &state);

                (state, descriptor, scope, directory)
            });
            drop(runtime);
            values
        };
        assert!(tokio::runtime::Handle::try_current().is_err());

        drop(scope);

        for _ in 0..100 {
            if state.lookup_lease_for_self(&descriptor).is_none() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("no-runtime fallback did not release the unheld descriptor lease");
    }
}
