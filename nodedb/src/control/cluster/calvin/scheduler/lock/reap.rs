// SPDX-License-Identifier: BUSL-1.1

//! Deterministic lease-based reaping of orphaned Calvin read reservations.
//!
//! A read reservation installs a SHARED lock owned by a reservation-band
//! [`TxnId`] (see [`TxnId::is_reservation`]). If the coordinator that owns it
//! crashes during think-time before committing or aborting, the shared lock
//! leaks and can block younger writers forever. [`LockManager::reap_expired_shared`]
//! releases any such reservation whose owner epoch has fallen behind a
//! replicated logical threshold — no wall clock, so every replica reaps
//! identically given the same input order.

use super::lock_entry::LockMode;
use super::lock_key::TxnId;
use super::manager::LockManager;

impl LockManager {
    /// Reap every SHARED reservation whose owner mint-epoch is older than
    /// `epoch_threshold`, releasing it through the normal `release` path (so any
    /// waiter queued behind the freed key is promoted exactly like a live
    /// release). Returns the promoted waiter ids, ready for `dispatch_promoted`.
    ///
    /// Restricted to owners in the reservation band (`is_reservation()`) — a real
    /// transaction's lock owner is never in this band, so a real txn's locks can
    /// never be reaped. Further restricted to owners ALL of whose held keys are
    /// still `Shared`: an owner that self-upgraded a key to `Exclusive` (a commit
    /// in flight under that reservation) is left alone.
    pub fn reap_expired_shared(&mut self, epoch_threshold: u64) -> Vec<TxnId> {
        let expired: Vec<TxnId> = self
            .held_locks
            .iter()
            .filter(|(owner, keys)| {
                owner.is_reservation()
                    && owner.epoch < epoch_threshold
                    && keys.iter().all(|k| {
                        self.table
                            .get(k)
                            .is_some_and(|e| e.mode == LockMode::Shared)
                    })
            })
            .map(|(owner, _)| *owner)
            .collect();

        let mut promoted = Vec::new();
        for owner in expired {
            tracing::debug!(
                epoch = owner.epoch,
                position = owner.position,
                "calvin: reaping lease-expired shared reservation"
            );
            promoted.extend(self.release(owner));
        }
        promoted
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::*;
    use crate::control::cluster::calvin::scheduler::lock::lock_entry::AcquireOutcome;
    use crate::control::cluster::calvin::scheduler::lock::lock_key::LockKey;

    fn key(name: &str) -> LockKey {
        LockKey::Surrogate {
            collection: Arc::from(name),
            surrogate: 1,
        }
    }

    fn keyset(names: &[&str]) -> BTreeSet<LockKey> {
        names.iter().map(|n| key(n)).collect()
    }

    fn txn(epoch: u64, pos: u32) -> TxnId {
        TxnId::new(epoch, pos)
    }

    fn resv(epoch: u64, pos_offset: u32) -> TxnId {
        TxnId::new(epoch, TxnId::RESERVATION_POSITION_BAND + pos_offset)
    }

    #[test]
    fn reap_releases_expired_shared_reservation() {
        let mut lm = LockManager::new();
        let r = resv(1, 0);
        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.lock_count(), 1);

        let promoted = lm.reap_expired_shared(100);
        assert!(promoted.is_empty(), "no waiter queued behind the key");
        assert_eq!(lm.lock_count(), 0, "the reservation's key is freed");
        assert_eq!(lm.holder_count(), 0, "the reservation is released");
    }

    #[test]
    fn reap_ignores_within_lease() {
        let mut lm = LockManager::new();
        let r = resv(90, 0);
        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);

        let promoted = lm.reap_expired_shared(50);
        assert!(promoted.is_empty());
        assert_eq!(lm.lock_count(), 1, "still within the lease, not reaped");
        assert!(lm.table.get(&key("k")).unwrap().holders.contains(&r));
    }

    #[test]
    fn reap_ignores_non_reservation_owner() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);
        assert_eq!(lm.acquire(t, keyset(&["k"])), AcquireOutcome::Ready);

        let promoted = lm.reap_expired_shared(100);
        assert!(promoted.is_empty());
        assert_eq!(
            lm.lock_count(),
            1,
            "a real txn's exclusive lock is never reaped"
        );
        assert!(lm.table.get(&key("k")).unwrap().holders.contains(&t));
    }

    #[test]
    fn reap_skips_self_upgraded_reservation() {
        let mut lm = LockManager::new();
        let r = resv(1, 0);
        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);
        // Self-upgrade to exclusive: a commit in flight under this reservation.
        assert_eq!(lm.acquire(r, keyset(&["k"])), AcquireOutcome::Ready);

        let promoted = lm.reap_expired_shared(100);
        assert!(promoted.is_empty());
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(
            entry.mode,
            LockMode::Exclusive,
            "mid-commit reservation is left alone"
        );
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], r);
    }

    #[test]
    fn reap_promotes_waiter() {
        let mut lm = LockManager::new();
        let r = resv(1, 0);
        let writer = txn(5, 0);

        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);
        // A younger writer waits behind the shared reservation (wound-wait
        // younger-waits, since the writer's epoch is greater than r's).
        assert_eq!(lm.acquire(writer, keyset(&["k"])), AcquireOutcome::Blocked);
        assert!(
            lm.table
                .get(&key("k"))
                .unwrap()
                .waiters
                .iter()
                .any(|(w, _)| *w == writer)
        );

        let promoted = lm.reap_expired_shared(100);
        assert_eq!(
            promoted,
            vec![writer],
            "reaping the stuck reservation unblocks the younger writer"
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], writer);
    }
}
