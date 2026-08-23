// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease drain state.
//!
//! While a descriptor is being drained, any new lease acquire at
//! `version <= up_to_version` must be rejected cluster-wide so the
//! in-flight DDL that bumps the version can make progress.
//!
//! **State ownership**: the canonical drain state is replicated
//! through the metadata raft group via
//! `MetadataEntry::DescriptorDrainStart` / `DescriptorDrainEnd`
//! entries. Every node's `MetadataCommitApplier` decodes those
//! entries and calls `install_start` / `install_end` on a local
//! `DescriptorDrainTracker` mounted on `SharedState.lease_drain`.
//! Reads of the tracker happen on every lease acquire (the
//! `is_draining` check in `force_refresh_lease`) and during the
//! proposer's drain wait loop. This file owns the in-memory
//! state only; the propose-side orchestration (including the
//! rolling-upgrade gate and the wait-for-leases-to-release loop)
//! lives in `drain_propose.rs`.
//!
//! **TTL semantics**: every drain entry carries an `expires_at`
//! HLC, but `is_draining` never compares it against a local
//! wall clock — a node never judges another node's deadline by
//! its own clock, since nothing bounds clock skew across nodes.
//! A drain is active on every node until an explicit
//! `DescriptorDrainEnd` clears it (`install_end`). The liveness
//! backstop for a crashed proposer lives in `drain_propose.rs`:
//! `wait_for_lease_drain` bounds its own wait with a same-node
//! `Instant` deadline and, on timeout, proposes
//! `DescriptorDrainEnd` explicitly — that replicated entry, not
//! `expires_at`, is what clears a stale drain everywhere. We do
//! NOT run a periodic GC task. If nothing ever re-writes the
//! key, the entry sits in the map until the next `install_end`
//! on the same id or until process restart (drain state is not
//! persisted to redb — it's raft-log-derived and rebuilds on
//! replay).

use std::collections::HashMap;
use std::sync::RwLock;

use nodedb_cluster::DescriptorId;
use nodedb_types::Hlc;

/// One drain entry: "this descriptor is draining leases at
/// versions <= `up_to_version`, active until an explicit
/// `DescriptorDrainEnd`".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainEntry {
    pub up_to_version: u64,
    /// HLC the proposer stamped when it started the drain.
    /// Observability only — `is_draining` never reads this field,
    /// so it does not bound how long the drain stays active. See
    /// the module doc for why a wall-clock comparison is unsafe
    /// here.
    pub expires_at: Hlc,
}

/// In-memory drain state for descriptors being altered.
///
/// All public mutations (`install_start`, `install_end`) are
/// called by the metadata applier's decode path. All public
/// reads (`is_draining`, `snapshot`, `count`) are called by the
/// lease acquire path and the drain wait loop.
#[derive(Debug, Default)]
pub struct DescriptorDrainTracker {
    active: RwLock<HashMap<DescriptorId, DrainEntry>>,
}

impl DescriptorDrainTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the start of a drain for `id` at `up_to_version`,
    /// stamped with `expires_at` for observability (see
    /// [`DrainEntry::expires_at`]). Overwrites any prior entry
    /// for the same key — a subsequent start with a higher
    /// `up_to_version` extends the drain rather than creating a
    /// conflicting record.
    ///
    /// Called by the metadata applier on every node when a
    /// `DescriptorDrainStart` raft entry commits.
    pub fn install_start(&self, id: DescriptorId, up_to_version: u64, expires_at: Hlc) {
        tracing::debug!(
            ?id,
            up_to_version,
            expires_wall_ns = expires_at.wall_ns,
            "drain: install_start"
        );
        let mut map = self.active.write().unwrap_or_else(|p| p.into_inner());
        map.insert(
            id,
            DrainEntry {
                up_to_version,
                expires_at,
            },
        );
    }

    /// Remove the drain entry for `id`, if any. Called by the
    /// metadata applier both on explicit `DescriptorDrainEnd`
    /// raft entries AND on the implicit clear path that runs
    /// after a successful `Put*` apply.
    pub fn install_end(&self, id: &DescriptorId) {
        tracing::debug!(?id, "drain: install_end");
        let mut map = self.active.write().unwrap_or_else(|p| p.into_inner());
        map.remove(id);
    }

    /// Whether an acquire on `(id, requested_version)` must be
    /// rejected because a drain is active that covers this
    /// version.
    ///
    /// Returns `true` iff an entry exists for `id` with
    /// `requested_version <= entry.up_to_version` (i.e. the
    /// requested version is inside the drain range). Drain state
    /// is raft-replicated, so presence of an entry is authoritative
    /// on every node — a node never judges another node's deadline
    /// (`expires_at`, stamped by whichever node proposed the drain)
    /// against its own wall clock, since nothing bounds clock skew
    /// between nodes. An entry stays active until an explicit
    /// `DescriptorDrainEnd` clears it via `install_end`.
    pub fn is_draining(&self, id: &DescriptorId, requested_version: u64) -> bool {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        match map.get(id) {
            Some(entry) => requested_version <= entry.up_to_version,
            None => false,
        }
    }

    /// Snapshot the full (id, entry) set for diagnostics and tests.
    pub fn snapshot(&self) -> Vec<(DescriptorId, DrainEntry)> {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        map.iter().map(|(id, e)| (id.clone(), *e)).collect()
    }

    /// Count of active drain entries. Every installed entry is
    /// active until `install_end` clears it (see `is_draining`),
    /// so this is equivalent to [`Self::total_count`]; kept as a
    /// distinct name for callers that mean "active" semantically.
    pub fn count_active(&self) -> usize {
        self.total_count()
    }

    /// Total count of installed drain entries. Mainly for
    /// debugging.
    pub fn total_count(&self) -> usize {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::DescriptorKind;

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    fn hlc(wall_ns: u64) -> Hlc {
        Hlc::new(wall_ns, 0)
    }

    #[test]
    fn install_then_is_draining_true_for_versions_in_range() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("orders");
        tracker.install_start(d.clone(), 5, hlc(1_000_000));
        // Versions 1..=5 are inside the drain range; version 6 is
        // outside.
        assert!(tracker.is_draining(&d, 1));
        assert!(tracker.is_draining(&d, 3));
        assert!(tracker.is_draining(&d, 5));
        assert!(!tracker.is_draining(&d, 6));
        assert!(!tracker.is_draining(&d, 100));
    }

    #[test]
    fn install_end_clears_entry() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("orders");
        tracker.install_start(d.clone(), 5, hlc(1_000_000));
        assert!(tracker.is_draining(&d, 5));

        tracker.install_end(&d);
        assert!(!tracker.is_draining(&d, 5));
        assert_eq!(tracker.total_count(), 0);
    }

    /// Pins the fix for the cross-node clock-skew bug: a node must
    /// never judge another node's drain deadline by its own wall
    /// clock. An entry whose `expires_at` is far in the local past
    /// stays active — only an explicit `install_end` clears it.
    #[test]
    fn is_draining_stays_active_past_local_wall_clock_expiry() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("stale-clock");
        // expires_at is stamped far in the past relative to any
        // wall clock a checking node could plausibly read.
        tracker.install_start(d.clone(), 5, hlc(1_000));
        assert!(tracker.is_draining(&d, 1));
        assert!(tracker.is_draining(&d, 5));
        assert!(!tracker.is_draining(&d, 6));

        // Only an explicit end clears it.
        tracker.install_end(&d);
        assert!(!tracker.is_draining(&d, 1));
    }

    #[test]
    fn multiple_descriptors_are_independent() {
        let tracker = DescriptorDrainTracker::new();
        let a = id("a");
        let b = id("b");
        tracker.install_start(a.clone(), 1, hlc(1_000_000));
        tracker.install_start(b.clone(), 10, hlc(1_000_000));

        assert!(tracker.is_draining(&a, 1));
        assert!(!tracker.is_draining(&a, 2));
        assert!(tracker.is_draining(&b, 5));
        assert!(tracker.is_draining(&b, 10));
        assert!(!tracker.is_draining(&b, 11));
    }

    #[test]
    fn install_start_overwrites_prior_entry() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("orders");
        tracker.install_start(d.clone(), 5, hlc(1_000_000));
        // Start again with a higher up_to_version — the new
        // entry extends the drain range.
        tracker.install_start(d.clone(), 10, hlc(2_000_000));

        assert!(tracker.is_draining(&d, 10));
        assert_eq!(tracker.total_count(), 1);
        let snap = tracker.snapshot();
        assert_eq!(snap[0].1.up_to_version, 10);
        assert_eq!(snap[0].1.expires_at.wall_ns, 2_000_000);
    }

    /// `count_active` no longer filters by wall-clock expiry, so it
    /// pins to the same value as `total_count` for any installed
    /// set, regardless of how far in the past `expires_at` is.
    #[test]
    fn count_active_matches_total_count_regardless_of_expiry() {
        let tracker = DescriptorDrainTracker::new();
        let a = id("live");
        let b = id("expired-by-wall-clock");
        tracker.install_start(a, 1, hlc(10_000_000));
        tracker.install_start(b, 1, hlc(100));

        assert_eq!(tracker.total_count(), 2);
        assert_eq!(tracker.count_active(), 2);

        tracker.install_end(&id("live"));
        assert_eq!(tracker.count_active(), 1);
        assert_eq!(tracker.count_active(), tracker.total_count());
    }
}
