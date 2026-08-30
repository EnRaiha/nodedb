// SPDX-License-Identifier: BUSL-1.1

//! Retention-window resolution for the collection GC sweeper.
//!
//! Resolution order (highest precedence first):
//!
//! 1. Per-tenant `tenant_config.deactivated_collection_retention_days`
//!    — an operator override set via `ALTER TENANT ... SET ...`.
//! 2. System-wide `server.retention.deactivated_collection_retention_days`.
//!
//! The resolver is pure (no I/O); the sweeper supplies the inputs.

use std::time::Duration;

use crate::control::security::catalog::StoredCollection;

/// Outcome of evaluating a single soft-deleted collection against its
/// retention policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PurgeDecision {
    /// Retention has elapsed; propose `PurgeCollection`.
    Purge,
    /// Still within retention; skip this sweep, re-evaluate next tick.
    Wait { remaining: Duration },
    /// Stored row is active (defensive: sweeper should have filtered
    /// these out, but treat as not-purgable for safety).
    NotDeactivated,
    /// Deactivated, but `deactivated_at_ns` is the sentinel `0` — either
    /// dropped before this field existed, or (defensively) some other path
    /// that never stamped it. Drop time is unknown, so the row is never
    /// purgable until the sweeper adopts a first-observed time for it.
    AdoptDeactivationTime,
}

/// Resolve whether `coll` should be purged given the `now` wall-clock
/// time (Unix-epoch nanoseconds) and the effective retention window.
pub fn resolve_retention(
    coll: &StoredCollection,
    now_ns: u64,
    retention: Duration,
) -> PurgeDecision {
    if coll.is_active {
        return PurgeDecision::NotDeactivated;
    }
    if coll.deactivated_at_ns == 0 {
        return PurgeDecision::AdoptDeactivationTime;
    }
    let retention_ns = retention.as_nanos() as u64;
    let purge_at = coll.deactivated_at_ns.saturating_add(retention_ns);
    if now_ns >= purge_at {
        PurgeDecision::Purge
    } else {
        PurgeDecision::Wait {
            remaining: Duration::from_nanos(purge_at - now_ns),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Hlc;

    /// A collection whose drop time is `wall_ns`. Both timestamps are set to
    /// the same value, which is what the apply path produces for a real DROP;
    /// `coll_with_deactivation` is the helper for pulling them apart.
    fn coll(is_active: bool, wall_ns: u64) -> StoredCollection {
        let mut c = StoredCollection::new(1, "c", "u");
        c.is_active = is_active;
        c.modification_hlc = Hlc::new(wall_ns, 0);
        if !is_active {
            c.deactivated_at_ns = wall_ns;
        }
        c
    }

    /// Like `coll`, but sets `deactivated_at_ns` independently of
    /// `modification_hlc` — needed to prove `resolve_retention` reads the
    /// dedicated field rather than falling back to the HLC.
    fn coll_with_deactivation(
        is_active: bool,
        modification_wall_ns: u64,
        deactivated_at_ns: u64,
    ) -> StoredCollection {
        let mut c = StoredCollection::new(1, "c", "u");
        c.is_active = is_active;
        c.modification_hlc = Hlc::new(modification_wall_ns, 0);
        c.deactivated_at_ns = deactivated_at_ns;
        c
    }

    #[test]
    fn active_collection_is_never_purgable() {
        let c = coll(true, 0);
        assert_eq!(
            resolve_retention(&c, u64::MAX, Duration::ZERO),
            PurgeDecision::NotDeactivated
        );
    }

    #[test]
    fn soft_deleted_past_retention_is_purgable() {
        let c = coll(false, 1_000_000_000); // 1s
        let decision = resolve_retention(
            &c,
            2_000_000_000 + Duration::from_secs(5).as_nanos() as u64,
            Duration::from_secs(5),
        );
        assert_eq!(decision, PurgeDecision::Purge);
    }

    #[test]
    fn soft_deleted_within_retention_waits() {
        let c = coll(false, 1_000_000_000);
        let decision = resolve_retention(
            &c,
            1_000_000_000 + Duration::from_secs(1).as_nanos() as u64,
            Duration::from_secs(5),
        );
        match decision {
            PurgeDecision::Wait { remaining } => {
                assert!(remaining <= Duration::from_secs(4));
                assert!(remaining > Duration::from_secs(3));
            }
            other => panic!("expected Wait, got {other:?}"),
        }
    }

    #[test]
    fn zero_retention_is_immediately_purgable() {
        let c = coll(false, 1_000);
        assert_eq!(
            resolve_retention(&c, 1_000, Duration::ZERO),
            PurgeDecision::Purge
        );
    }

    /// The retention clock must start at the DROP, not the original CREATE.
    /// Seeds a collection with an ancient `modification_hlc` (its CREATE
    /// stamp), then drops it through the exact production path
    /// (`descriptor_stamp::stamp` then `apply_to`, the same as
    /// `CatalogEntry::DeactivateCollection` dispatch) and checks the
    /// retention decision immediately after. A collection created long ago
    /// but dropped moments before the sweep must not be purgable under a
    /// retention window that has not elapsed since the drop.
    #[test]
    fn retention_window_is_measured_from_deactivation_not_creation() {
        use crate::control::catalog_entry::CatalogEntry;
        use crate::control::catalog_entry::apply::apply_to;
        use crate::control::catalog_entry::descriptor_stamp::stamp;
        use crate::control::security::credential::store::CredentialStore;
        use nodedb_types::{DatabaseId, HlcClock};

        let tmp = tempfile::tempdir().expect("tmpdir");
        let store =
            CredentialStore::open(&tmp.path().join("system.redb")).expect("open credential store");
        let catalog = store.catalog();
        let clock = HlcClock::new();

        // Created long ago: an ancient `modification_hlc`, far outside any
        // retention window measured against the real clock.
        let mut old = StoredCollection::new(1, "ancient", "tester");
        old.modification_hlc = Hlc::new(1_000, 0);
        old.descriptor_version = 1;
        // Seeded through `apply_to` rather than `put_collection` so the
        // StoredOwner row lands too — the integrity guard rejects a
        // collection row with no owner.
        apply_to(&CatalogEntry::PutCollection(Box::new(old)), catalog)
            .expect("apply put_collection");

        // Drop it "now", through the same path production DROP COLLECTION
        // uses: stamp the entry, then apply it.
        let deactivate = stamp(
            CatalogEntry::DeactivateCollection {
                database_id: 0,
                tenant_id: 1,
                name: "ancient".into(),
                descriptor_version: 0,
                modification_hlc: Hlc::ZERO,
            },
            &clock,
            catalog,
        );
        apply_to(&deactivate, catalog).expect("apply deactivate_collection");

        let dropped = catalog
            .get_collection(DatabaseId::DEFAULT, 1, "ancient")
            .unwrap()
            .expect("row preserved after soft delete");

        // The sweep runs immediately after the drop. A one-hour retention
        // window has not elapsed since the drop, even though the collection
        // was created long ago.
        let sweep_now_ns = clock.now().wall_ns;
        let decision = resolve_retention(&dropped, sweep_now_ns, Duration::from_secs(3600));
        match decision {
            PurgeDecision::Wait { remaining } => {
                assert!(remaining <= Duration::from_secs(3600));
            }
            other => panic!(
                "expected Wait: retention must be measured from the DROP, not the original \
                 CREATE time — got {other:?} for a collection created long ago but dropped only \
                 moments before the sweep"
            ),
        }
    }

    /// Upgrade-safety case: a collection deactivated before
    /// `deactivated_at_ns` existed carries the sentinel `0` (no known drop
    /// time) alongside an ancient `modification_hlc` left over from its
    /// CREATE. `purge_at` computed from that ancient HLC has long since
    /// elapsed, so a resolver that still reads `modification_hlc` would
    /// purge it on the very first sweep after the upgrade — destroying an
    /// UNDROP-able row irreversibly. `deactivated_at_ns == 0` must never be
    /// purgable, no matter how far `now` is pushed out.
    #[test]
    fn deactivated_at_zero_is_never_purgable_even_at_max_now() {
        let c = coll_with_deactivation(false, 1_000, 0);
        let decision = resolve_retention(&c, u64::MAX, Duration::ZERO);
        assert_ne!(
            decision,
            PurgeDecision::Purge,
            "a row with unknown deactivation time (deactivated_at_ns == 0) must never be \
             purged — got {decision:?} for now = u64::MAX and zero retention, the worst case"
        );
    }

    /// Same upgrade-safety case, swept at an ordinary `now_ns` and a
    /// realistic 7-day default retention — not just the `u64::MAX` /
    /// zero-retention extreme above.
    #[test]
    fn deactivated_at_zero_is_never_purgable_under_default_retention() {
        let c = coll_with_deactivation(false, 1_000, 0);
        let seven_days = Duration::from_secs(7 * 24 * 60 * 60);
        let decision = resolve_retention(&c, 10_000_000_000_000, seven_days);
        assert_ne!(
            decision,
            PurgeDecision::Purge,
            "a row with unknown deactivation time must never be purged under the default \
             7-day retention — got {decision:?}"
        );
    }

    /// A real `deactivated_at_ns` inside the retention window waits, and the
    /// window is measured from `deactivated_at_ns`, not `modification_hlc`.
    /// `modification_hlc` is seeded far in the past — if `resolve_retention`
    /// mistakenly reads it instead of `deactivated_at_ns`, this collection
    /// would already be past its window and the test would observe `Purge`
    /// instead of `Wait`.
    #[test]
    fn deactivated_at_within_window_waits_measured_from_deactivated_at() {
        let deactivated_at_ns = 1_000_000_000_000; // far after modification_hlc
        let c = coll_with_deactivation(false, 1_000, deactivated_at_ns);
        let retention = Duration::from_secs(5);
        let now_ns = deactivated_at_ns + Duration::from_secs(1).as_nanos() as u64;
        let decision = resolve_retention(&c, now_ns, retention);
        match decision {
            PurgeDecision::Wait { remaining } => {
                assert!(remaining <= Duration::from_secs(4));
                assert!(remaining > Duration::from_secs(3));
            }
            other => panic!(
                "expected Wait measured from deactivated_at_ns, got {other:?} — if this fired \
                 Purge, resolve_retention likely read the ancient modification_hlc instead of \
                 deactivated_at_ns"
            ),
        }
    }

    /// The window's other edge: once `now` passes `deactivated_at_ns +
    /// retention`, the row is purgable — again measured from
    /// `deactivated_at_ns`, with `modification_hlc` seeded so that reading
    /// it instead would still (wrongly) report `Wait` rather than `Purge`.
    #[test]
    fn deactivated_at_past_window_is_purgable_measured_from_deactivated_at() {
        let deactivated_at_ns = 1_000_000_000_000;
        // If `modification_hlc` were read instead, `now` would still be
        // within 5s of it, so the wrong-field bug would report `Wait`.
        let modification_wall_ns = deactivated_at_ns + Duration::from_secs(100).as_nanos() as u64;
        let c = coll_with_deactivation(false, modification_wall_ns, deactivated_at_ns);
        let retention = Duration::from_secs(5);
        let now_ns = deactivated_at_ns + Duration::from_secs(6).as_nanos() as u64;
        assert_eq!(
            resolve_retention(&c, now_ns, retention),
            PurgeDecision::Purge,
            "row is 6s past its 5s window measured from deactivated_at_ns and must be purgable"
        );
    }

    /// An active row is never purgable, regardless of what either time
    /// field holds — even a `deactivated_at_ns` that (if read) would look
    /// long expired.
    #[test]
    fn active_collection_is_never_purgable_regardless_of_deactivated_at() {
        let c = coll_with_deactivation(true, 0, 1);
        assert_eq!(
            resolve_retention(&c, u64::MAX, Duration::ZERO),
            PurgeDecision::NotDeactivated
        );
    }
}
