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
//! wait-for-leases-to-release loop) lives in `drain_propose.rs`.
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
//! `expires_at`, is what clears a stale drain everywhere.
//!
//! A proposer that crashes holding a drain ends it itself on restart,
//! through its own recovery. A proposer that leaves the topology never
//! restarts: the `TopologyChange::Leave` apply hook ends every drain that
//! node proposed (`lease::gc::end_drains_for_node`). A node that is only
//! suspected Dead keeps its drains, because it can still be running the DDL
//! the drain protects.
//!
//! **Durability**: `drain_apply` writes one `SystemCatalog` row per
//! (descriptor, owner) before it changes the tracker, for the metadata
//! applier and the single-node fallback alike. Boot seeds the tracker from
//! those rows.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use nodedb_cluster::{DescriptorId, DrainOwner};
use nodedb_types::Hlc;
use tokio::sync::{Notify, futures::Notified};

/// One owner's drain: "this descriptor is draining leases at
/// versions <= `up_to_version`, active until an explicit
/// `DescriptorDrainEnd` for this owner".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainEntry {
    pub up_to_version: u64,
    /// HLC the proposer stamped when it started the drain.
    /// Observability only — `is_draining` never reads this field,
    /// so it does not bound how long the drain stays active. See
    /// the module doc for why a wall-clock comparison is unsafe
    /// here.
    pub expires_at: Hlc,
    /// Node that proposed the drain.
    pub proposer_node_id: u64,
}

/// In-memory drain state for descriptors being altered.
///
/// Each descriptor keeps one entry per active owner. It is draining while
/// any owner's entry covers the requested version.
///
/// All public mutations (`install_start`, `install_end`) are
/// called by the metadata applier's decode path. All public
/// reads (`is_draining`, `snapshot`, `count`) are called by the
/// lease acquire path and the drain wait loop.
///
/// Two wake-ups replace fixed polling on both sides of a drain:
///
/// - `holds_changed` wakes a drain waiting for this node's holds and leases.
///   A hold given back and a lease release applied both fire it.
/// - `ended` wakes a statement waiting out a drain. It fires once the
///   metadata entry that ended the drain has applied all its effects, so the
///   retried statement plans against the new descriptor.
#[derive(Debug, Default)]
pub struct DescriptorDrainTracker {
    active: RwLock<HashMap<DescriptorId, HashMap<DrainOwner, DrainEntry>>>,
    holds_changed: Notify,
    ended: Notify,
    /// A drain ended since the last [`Self::settle`].
    ended_unsettled: AtomicBool,
}

impl DescriptorDrainTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wake every drain waiting for holds and leases to go.
    pub fn wake_drain_waiters(&self) {
        self.holds_changed.notify_waiters();
    }

    /// Resolves at the next [`Self::wake_drain_waiters`]. The caller enables
    /// it before it counts, so a wake during the count is not lost.
    pub fn holds_changed(&self) -> Notified<'_> {
        self.holds_changed.notified()
    }

    /// Wake every statement waiting out a drain, when a drain ended since the
    /// last call. The metadata applier calls it after each entry's effects.
    pub fn settle(&self) {
        if self.ended_unsettled.swap(false, Ordering::AcqRel) {
            self.ended.notify_waiters();
        }
    }

    /// Resolves at the next [`Self::settle`] that follows a drain end. The
    /// caller enables it before its attempt, so an end during the attempt is
    /// not lost.
    pub fn drain_ended(&self) -> Notified<'_> {
        self.ended.notified()
    }

    /// Drop every drain. Boot and a metadata snapshot install call this
    /// before loading the persisted drains.
    pub fn clear(&self) {
        let mut map = self.active.write().unwrap_or_else(|p| p.into_inner());
        if !map.is_empty() {
            self.ended_unsettled.store(true, Ordering::Release);
        }
        map.clear();
    }

    /// Record `owner`'s drain of `id` at `up_to_version`, stamped with
    /// `expires_at` for observability (see [`DrainEntry::expires_at`]).
    /// Overwrites that owner's prior entry only: a restart by the same owner
    /// replaces its range, and other owners' entries stay.
    ///
    /// Called by the metadata applier on every node when a
    /// `DescriptorDrainStart` raft entry commits.
    pub fn install_start(
        &self,
        id: DescriptorId,
        owner: DrainOwner,
        up_to_version: u64,
        expires_at: Hlc,
        proposer_node_id: u64,
    ) {
        tracing::debug!(
            ?id,
            ?owner,
            up_to_version,
            expires_wall_ns = expires_at.wall_ns,
            proposer_node_id,
            "drain: install_start"
        );
        let mut map = self.active.write().unwrap_or_else(|p| p.into_inner());
        map.entry(id).or_default().insert(
            owner,
            DrainEntry {
                up_to_version,
                expires_at,
                proposer_node_id,
            },
        );
    }

    /// Every `(descriptor, owner)` drain `node_id` proposed.
    pub fn proposed_by(&self, node_id: u64) -> Vec<(DescriptorId, DrainOwner)> {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        map.iter()
            .flat_map(|(id, owners)| {
                owners
                    .iter()
                    .filter(|(_, entry)| entry.proposer_node_id == node_id)
                    .map(|(owner, _)| (id.clone(), owner.clone()))
            })
            .collect()
    }

    /// Remove `owner`'s drain of `id`, if any. Other owners' drains stay.
    /// Called by the metadata applier both on explicit `DescriptorDrainEnd`
    /// raft entries AND on the implicit clear path that runs
    /// after a successful `Put*` apply.
    pub fn install_end(&self, id: &DescriptorId, owner: &DrainOwner) {
        tracing::debug!(?id, ?owner, "drain: install_end");
        let mut map = self.active.write().unwrap_or_else(|p| p.into_inner());
        if let Some(owners) = map.get_mut(id) {
            if owners.remove(owner).is_some() {
                self.ended_unsettled.store(true, Ordering::Release);
            }
            if owners.is_empty() {
                map.remove(id);
            }
        }
    }

    /// Whether an acquire on `(id, requested_version)` must be
    /// rejected because a drain is active that covers this
    /// version.
    ///
    /// Returns `true` iff any owner's entry for `id` has
    /// `requested_version <= entry.up_to_version` (i.e. the
    /// requested version is inside that drain's range). Drain state
    /// is raft-replicated, so presence of an entry is authoritative
    /// on every node — a node never judges another node's deadline
    /// (`expires_at`, stamped by whichever node proposed the drain)
    /// against its own wall clock, since nothing bounds clock skew
    /// between nodes. An entry stays active until an explicit
    /// `DescriptorDrainEnd` for its owner clears it via `install_end`.
    pub fn is_draining(&self, id: &DescriptorId, requested_version: u64) -> bool {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        map.get(id).is_some_and(|owners| {
            owners
                .values()
                .any(|entry| requested_version <= entry.up_to_version)
        })
    }

    /// Every owner whose drain on `id` covers `requested_version`, named in a
    /// stable order. Empty when `is_draining` is false.
    pub fn draining_owners(&self, id: &DescriptorId, requested_version: u64) -> Vec<DrainOwner> {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        let mut owners: Vec<DrainOwner> = map
            .get(id)
            .map(|owners| {
                owners
                    .iter()
                    .filter(|(_, entry)| requested_version <= entry.up_to_version)
                    .map(|(owner, _)| owner.clone())
                    .collect()
            })
            .unwrap_or_default();
        owners.sort_by_cached_key(ToString::to_string);
        owners
    }

    /// Snapshot every `(id, owner, entry)` for diagnostics and tests.
    pub fn snapshot(&self) -> Vec<(DescriptorId, DrainOwner, DrainEntry)> {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        map.iter()
            .flat_map(|(id, owners)| {
                owners
                    .iter()
                    .map(|(owner, entry)| (id.clone(), owner.clone(), *entry))
            })
            .collect()
    }

    /// Count of active drain entries. Every installed entry is
    /// active until `install_end` clears it (see `is_draining`),
    /// so this is equivalent to [`Self::total_count`]; kept as a
    /// distinct name for callers that mean "active" semantically.
    pub fn count_active(&self) -> usize {
        self.total_count()
    }

    /// Total count of installed `(descriptor, owner)` drain entries.
    /// Mainly for debugging.
    pub fn total_count(&self) -> usize {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        map.values().map(HashMap::len).sum()
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

    const PROPOSER: u64 = 1;
    const DDL: DrainOwner = DrainOwner::Ddl;

    fn materializer(clone_collection: &str) -> DrainOwner {
        DrainOwner::CloneMaterialize {
            clone_database: 1025,
            tenant_id: 1,
            clone_collection: clone_collection.to_string(),
        }
    }

    #[test]
    fn install_then_is_draining_true_for_versions_in_range() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("orders");
        tracker.install_start(d.clone(), DDL, 5, hlc(1_000_000), PROPOSER);
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
        tracker.install_start(d.clone(), DDL, 5, hlc(1_000_000), PROPOSER);
        assert!(tracker.is_draining(&d, 5));

        tracker.install_end(&d, &DDL);
        assert!(!tracker.is_draining(&d, 5));
        assert_eq!(tracker.total_count(), 0);
    }

    /// A node never judges another node's drain deadline by its own wall
    /// clock, so cross-node clock skew cannot end a drain. An entry whose
    /// `expires_at` is far in the local past stays active. Only an explicit
    /// `install_end` clears it.
    #[test]
    fn is_draining_stays_active_past_local_wall_clock_expiry() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("stale-clock");
        // expires_at is stamped far in the past relative to any
        // wall clock a checking node can plausibly read.
        tracker.install_start(d.clone(), DDL, 5, hlc(1_000), PROPOSER);
        assert!(tracker.is_draining(&d, 1));
        assert!(tracker.is_draining(&d, 5));
        assert!(!tracker.is_draining(&d, 6));

        // Only an explicit end clears it.
        tracker.install_end(&d, &DDL);
        assert!(!tracker.is_draining(&d, 1));
    }

    #[test]
    fn multiple_descriptors_are_independent() {
        let tracker = DescriptorDrainTracker::new();
        let a = id("a");
        let b = id("b");
        tracker.install_start(a.clone(), DDL, 1, hlc(1_000_000), PROPOSER);
        tracker.install_start(b.clone(), DDL, 10, hlc(1_000_000), PROPOSER);

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
        tracker.install_start(d.clone(), DDL, 5, hlc(1_000_000), PROPOSER);
        // Start again with a higher up_to_version — the new
        // entry extends the drain range.
        tracker.install_start(d.clone(), DDL, 10, hlc(2_000_000), PROPOSER);

        assert!(tracker.is_draining(&d, 10));
        assert_eq!(tracker.total_count(), 1);
        let snap = tracker.snapshot();
        assert_eq!(snap[0].2.up_to_version, 10);
        assert_eq!(snap[0].2.expires_at.wall_ns, 2_000_000);
    }

    /// `count_active` does not filter by wall-clock expiry, so it equals
    /// `total_count` for any installed set, however far in the past
    /// `expires_at` is.
    #[test]
    fn count_active_matches_total_count_regardless_of_expiry() {
        let tracker = DescriptorDrainTracker::new();
        let a = id("live");
        let b = id("expired-by-wall-clock");
        tracker.install_start(a, DDL, 1, hlc(10_000_000), PROPOSER);
        tracker.install_start(b, DDL, 1, hlc(100), PROPOSER);

        assert_eq!(tracker.total_count(), 2);
        assert_eq!(tracker.count_active(), 2);

        tracker.install_end(&id("live"), &DDL);
        assert_eq!(tracker.count_active(), 1);
        assert_eq!(tracker.count_active(), tracker.total_count());
    }

    #[test]
    fn proposed_by_names_only_that_nodes_drains() {
        let tracker = DescriptorDrainTracker::new();
        tracker.install_start(id("a"), DDL, 1, hlc(1), 7);
        tracker.install_start(id("b"), DDL, 1, hlc(1), 8);
        tracker.install_start(id("c"), DDL, 1, hlc(1), 7);

        let mut mine = tracker.proposed_by(7);
        mine.sort_by(|x, y| format!("{x:?}").cmp(&format!("{y:?}")));
        assert_eq!(mine, vec![(id("a"), DDL), (id("c"), DDL)]);
        assert!(tracker.proposed_by(9).is_empty());
    }

    /// Ending one owner's drain leaves the descriptor drained while another
    /// owner's drain remains.
    #[test]
    fn descriptor_stays_drained_while_any_owner_remains() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("orders");
        tracker.install_start(d.clone(), materializer("a"), u64::MAX, hlc(1), PROPOSER);
        tracker.install_start(d.clone(), DDL, 3, hlc(1), PROPOSER);
        tracker.install_start(d.clone(), materializer("b"), u64::MAX, hlc(1), PROPOSER);
        assert_eq!(tracker.total_count(), 3);

        assert_eq!(
            tracker.draining_owners(&d, 4),
            vec![materializer("a"), materializer("b")],
            "the DDL's version-bounded drain does not cover version 4"
        );
        tracker.install_end(&d, &DDL);
        assert!(
            tracker.is_draining(&d, 4),
            "the DDL's end leaves both holders"
        );
        tracker.install_end(&d, &materializer("a"));
        assert!(tracker.is_draining(&d, 4), "one holder is left");
        tracker.install_end(&d, &materializer("a"));
        assert!(tracker.is_draining(&d, 4), "a repeated end is a no-op");
        tracker.install_end(&d, &materializer("b"));
        assert!(!tracker.is_draining(&d, 4));
        assert_eq!(tracker.total_count(), 0);
    }
}
