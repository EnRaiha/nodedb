// SPDX-License-Identifier: BUSL-1.1

//! Deterministic lock manager for the Calvin scheduler.
//!
//! # Design
//!
//! The lock manager provides a deterministic, totally-ordered lock table over
//! per-key entries keyed by [`LockKey`]. Locks come in two modes: `Exclusive`
//! (one holder, excludes all others) and `Shared` (many compatible holders).
//! The Calvin batch acquire path takes every key in a transaction's
//! `read_set ∪ write_set` as an `Exclusive` lock; single-key `Shared` locks are
//! available via [`LockManager::acquire_shared`].
//!
//! # Determinism
//!
//! `BTreeMap` is used throughout (not `HashMap`) so that iteration order is
//! deterministic and reproducible across replicas.  This is a correctness
//! requirement, not a style preference.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use smallvec::smallvec;

use super::lock_entry::{AcquireOutcome, LockEntry, LockMode};
use super::lock_key::{LockKey, TxnId};

// ── LockManager ───────────────────────────────────────────────────────────────

/// Deterministic Calvin lock manager for one vshard.
///
/// Manages an in-memory lock table keyed by [`LockKey`].  The table is held in
/// a `BTreeMap` so iteration is always deterministic.
///
/// # Key sets tracked per transaction
///
/// - `held_locks`: key sets for transactions that are a current holder on ALL
///   their keys and are actively executing (i.e. dispatched to the Data Plane).
/// - `pending_keys`: key sets for transactions that are blocked waiting for at
///   least one key.  When `release` promotes a blocked txn to holder on every
///   one of its keys, the entry moves from `pending_keys` to `held_locks`.
pub struct LockManager {
    /// Per-key lock entries.  Uses `BTreeMap` for deterministic iteration.
    /// `pub(super)` so the sibling `reap` module can scan entries for
    /// lease-expired reservations without a public accessor.
    pub(super) table: BTreeMap<LockKey, LockEntry>,
    /// Per-transaction set of currently held keys for **dispatched** txns.
    /// Used by `release` to iterate the key set without a full table scan.
    /// `pub(super)` — see `table`.
    pub(super) held_locks: BTreeMap<TxnId, BTreeSet<LockKey>>,
    /// Key sets for **blocked** (not-yet-dispatched) txns.  Populated when
    /// `acquire` returns `Blocked`; cleared (moved to `held_locks`) when all
    /// keys have been acquired on the promotion path inside `release`.
    pending_keys: BTreeMap<TxnId, BTreeSet<LockKey>>,
}

/// Outcome of inspecting a single key during [`LockManager::acquire_shared`].
enum SharedGrant {
    /// The shared lock was granted (key was free or already held shared).
    Granted,
    /// The key is held exclusively by another txn; the request was enqueued.
    Blocked,
}

/// The wound-wait decision for an exclusive requester that meets a conflict.
enum ExclusiveWait {
    /// Every conflicting holder is a shared reservation and the requester is
    /// older than all of them: wound (revoke) those shared holders and proceed.
    Wound,
    /// The requester must block: a conflicting holder is exclusive, or the
    /// requester is younger than some conflicting shared holder.
    Block,
}

/// The waiters promoted off one key when its holders drained, together with the
/// action to take on the now-empty entry.
enum Promotion {
    /// No waiters remained; the entry should be removed entirely.
    Freed,
    /// These waiters were installed as the new holders.
    Promoted(Vec<TxnId>),
}

impl LockManager {
    /// Create an empty lock manager.
    pub fn new() -> Self {
        Self {
            table: BTreeMap::new(),
            held_locks: BTreeMap::new(),
            pending_keys: BTreeMap::new(),
        }
    }

    /// Attempt to acquire **exclusive** locks on all keys for `txn`.
    ///
    /// If every key is free, already held exclusively by `txn` (promoted from
    /// waiter), or held **shared solely by `txn`** (a reservation this txn placed
    /// earlier, now upgraded in place to exclusive), records `txn` as sole holder
    /// of each key and returns [`AcquireOutcome::Ready`].
    ///
    /// Otherwise the whole key set is classified under the WOUND-WAIT discipline
    /// (see [`Self::wound_or_block`]).  `TxnId` order (`(epoch, position)`) is
    /// the replicated total order, so "older" means a smaller id and the
    /// decision is a pure function of the lock table plus the replicated ids —
    /// every replica computes it identically:
    /// - Any conflicting holder is **exclusive** → `txn` waits (an exclusive
    ///   holder is executing/applied work and is never wounded).
    /// - All conflicting holders are **shared** reservations and `txn` is older
    ///   than every one of them → **wound** them all (revoke to plain OCC, no
    ///   notification) and take every key. Wounding is silent.
    /// - Otherwise (`txn` younger than some conflicting shared holder) → wait.
    ///
    /// On the wait path `txn` is enqueued as an exclusive waiter on every
    /// conflicting key and holds none; its key set is stored in `pending_keys`
    /// so `release` can promote it atomically when all keys become available.
    /// The whole key set is evaluated before any mutation, so acquisition stays
    /// all-keys-or-none — the manager never partially wounds and then blocks.
    pub fn acquire(&mut self, txn: TxnId, keys: BTreeSet<LockKey>) -> AcquireOutcome {
        // First pass: determine whether any key is held by a *different* txn.
        // A key already held exclusively by `txn` (promoted via release) or held
        // shared solely by `txn` (an earlier reservation) counts as available —
        // the former is a no-op re-acquire, the latter a self-upgrade to exclusive.
        let all_available = keys.iter().all(|k| {
            self.table.get(k).is_none_or(|entry| {
                entry.held_exclusively_by(txn) || entry.held_shared_solely_by(txn)
            })
        });

        if all_available {
            // Acquire all keys.  For keys not yet in the table (free), insert a
            // new exclusive entry.  For keys already held by this txn (promoted
            // waiter), leave the entry unchanged — the waiter queue is intact.
            for key in &keys {
                match self.table.get_mut(key) {
                    None => {
                        self.table.insert(
                            key.clone(),
                            LockEntry {
                                mode: LockMode::Exclusive,
                                holders: smallvec![txn],
                                waiters: VecDeque::new(),
                            },
                        );
                    }
                    Some(entry) => {
                        // A key this txn already holds shared-solely is upgraded
                        // to exclusive in place (holders is exactly `[txn]`, so no
                        // holder change and any waiter queue stays intact). A key
                        // already held exclusively by `txn` is left unchanged.
                        if entry.held_shared_solely_by(txn) {
                            entry.mode = LockMode::Exclusive;
                        }
                    }
                }
            }
            // Move out of pending (if the txn was previously blocked on this
            // same key set) and into held_locks.
            self.pending_keys.remove(&txn);
            self.held_locks.insert(txn, keys);
            return AcquireOutcome::Ready;
        }

        // A conflict exists. Classify the WHOLE key set before mutating so the
        // wound / block decision is atomic (never partially wound then block).
        match self.wound_or_block(txn, &keys) {
            ExclusiveWait::Wound => {
                // Take every key exclusively. A conflicting shared entry has its
                // holders revoked (the wounded readers, all younger than `txn`,
                // degrade to plain OCC); `txn` becomes the sole holder while any
                // existing waiters remain queued behind it.
                for key in &keys {
                    match self.table.get_mut(key) {
                        Some(entry) => {
                            entry.mode = LockMode::Exclusive;
                            entry.holders.clear();
                            entry.holders.push(txn);
                        }
                        None => {
                            self.table.insert(
                                key.clone(),
                                LockEntry {
                                    mode: LockMode::Exclusive,
                                    holders: smallvec![txn],
                                    waiters: VecDeque::new(),
                                },
                            );
                        }
                    }
                }
                self.pending_keys.remove(&txn);
                self.held_locks.insert(txn, keys);
                AcquireOutcome::Ready
            }
            ExclusiveWait::Block => {
                // Enqueue as an exclusive waiter on every key held by a
                // different txn.
                for key in &keys {
                    if let Some(entry) = self.table.get_mut(key) {
                        // No conflict on this key means it is held solely by `txn`
                        // (a shared reservation to be upgraded, or an exclusive
                        // re-acquire) — leave it untouched; `txn` keeps the key and
                        // upgrades it once its conflicting keys are free. Same
                        // predicate as the `all_available` check above.
                        if entry.held_exclusively_by(txn) || entry.held_shared_solely_by(txn) {
                            continue;
                        }
                        // Real conflict on this key. If `txn` also holds it shared
                        // (an upgrade that must wait behind an OLDER shared holder),
                        // drop its own shared hold — degrading that read to plain
                        // OCC, never worse than today — so the key can drain to
                        // empty and normal promotion can grant `txn` the exclusive
                        // lock later. Without this, `txn` would occupy the key
                        // forever and its own exclusive request could never fire.
                        entry.holders.retain(|h| *h != txn);
                        if !entry.has_waiter(txn) {
                            entry.waiters.push_back((txn, LockMode::Exclusive));
                        }
                    }
                    // Free keys: no entry exists; the txn acquires them on the
                    // re-acquire path after all conflicting keys are released.
                }
                // Store the full key set so that release can promote this txn
                // atomically once all its keys become available.
                self.pending_keys.insert(txn, keys);
                AcquireOutcome::Blocked
            }
        }
    }

    /// Classify the wound-wait decision for an exclusive requester `txn` over
    /// `keys`, given that at least one key already conflicts.
    ///
    /// Pure read over the lock table: any exclusive conflict forces
    /// [`ExclusiveWait::Block`] (an exclusive holder is never wounded, so a mix
    /// of exclusive and shared conflicts blocks too). Otherwise all conflicting
    /// holders are shared reservations, and `txn` wounds them only when it is
    /// older than every one (`txn < h` for each conflicting shared holder `h`);
    /// if it is younger than any, it blocks. A key held only by `txn` itself is
    /// not a conflict.
    fn wound_or_block(&self, txn: TxnId, keys: &BTreeSet<LockKey>) -> ExclusiveWait {
        let mut shared_conflicts: Vec<TxnId> = Vec::new();
        for key in keys {
            if let Some(entry) = self.table.get(key) {
                match entry.mode {
                    LockMode::Exclusive => {
                        // Exclusive entries have exactly one holder; a holder
                        // other than `txn` is an exclusive conflict.
                        if !entry.holders.contains(&txn) {
                            return ExclusiveWait::Block;
                        }
                    }
                    LockMode::Shared => {
                        for holder in &entry.holders {
                            if *holder != txn {
                                shared_conflicts.push(*holder);
                            }
                        }
                    }
                }
            }
        }
        // Wound only when there is a shared conflict AND `txn` is older than
        // every conflicting shared holder; otherwise block. `shared_conflicts`
        // only ever holds *other* txns' shared holders (a key held shared solely
        // by `txn` never reaches here — it takes the self-upgrade path in
        // `acquire`), so an empty set here means every conflict was exclusive.
        if !shared_conflicts.is_empty() && shared_conflicts.iter().all(|holder| txn < *holder) {
            ExclusiveWait::Wound
        } else {
            ExclusiveWait::Block
        }
    }

    /// Attempt to acquire a **shared** lock on a single `key` for `txn`.
    ///
    /// - Key free → create a shared entry holding `txn`, return
    ///   [`AcquireOutcome::Ready`].
    /// - Key held shared → add `txn` to the holders, return
    ///   [`AcquireOutcome::Ready`].
    /// - Key held exclusively by another txn → enqueue `txn` as a shared waiter
    ///   (FIFO) and return [`AcquireOutcome::Blocked`].
    ///
    /// A shared request that meets an exclusive holder blocks FIFO for now;
    /// wound-wait priority resolution lands in a following change.
    pub fn acquire_shared(&mut self, txn: TxnId, key: LockKey) -> AcquireOutcome {
        // Inspect / mutate the entry via the `Entry` API (which takes the key by
        // value, sidestepping a get-then-insert borrow conflict) inside a scoped
        // borrow so the map-level bookkeeping below can re-borrow `self`.
        let grant = match self.table.entry(key.clone()) {
            Entry::Vacant(slot) => {
                slot.insert(LockEntry {
                    mode: LockMode::Shared,
                    holders: smallvec![txn],
                    waiters: VecDeque::new(),
                });
                SharedGrant::Granted
            }
            Entry::Occupied(mut slot) => {
                let entry = slot.get_mut();
                if entry.mode == LockMode::Shared {
                    if !entry.holders.contains(&txn) {
                        entry.holders.push(txn);
                    }
                    SharedGrant::Granted
                } else {
                    // Held exclusively by another txn: block FIFO.
                    if !entry.has_waiter(txn) {
                        entry.waiters.push_back((txn, LockMode::Shared));
                    }
                    SharedGrant::Blocked
                }
            }
        };

        match grant {
            SharedGrant::Granted => {
                self.pending_keys.remove(&txn);
                self.held_locks.entry(txn).or_default().insert(key);
                AcquireOutcome::Ready
            }
            SharedGrant::Blocked => {
                let mut pending = BTreeSet::new();
                pending.insert(key);
                self.pending_keys.insert(txn, pending);
                AcquireOutcome::Blocked
            }
        }
    }

    /// Non-blocking exclusive acquire: take all `keys` for `txn` iff every one is
    /// free (or already held by `txn`), returning `true`; otherwise return
    /// `false` WITHOUT enqueuing a waiter or recording any pending state.
    ///
    /// This is the fast path's probe. Unlike [`acquire`](Self::acquire), the
    /// contended (`false`) path touches NOTHING — no holder, no `pending_keys`,
    /// no waiter `VecDeque` — so a caller that does not intend to block (an
    /// autocommit point write that will instead route to the scheduler) never
    /// leaves an orphaned waiter that a later `release` would promote to an
    /// unowned holder. It also never perturbs the FIFO ordering that Calvin
    /// transactions depend on.
    pub fn try_acquire(&mut self, txn: TxnId, keys: BTreeSet<LockKey>) -> bool {
        if !self.is_ready(txn, &keys) {
            // Contended: leave the table, waiter queues, and pending_keys
            // completely untouched.
            return false;
        }
        // Every key is free or already held by `txn`, so `acquire` takes its
        // all-available path — it inserts the holder and never enqueues.
        let outcome = self.acquire(txn, keys);
        debug_assert_eq!(
            outcome,
            AcquireOutcome::Ready,
            "try_acquire: is_ready was true but acquire returned Blocked"
        );
        true
    }

    /// Release all locks held by `txn`.
    ///
    /// `txn` is removed from every entry's holder set.  When an entry's holders
    /// drain to empty, its FIFO waiters are promoted mode-aware: a leading run
    /// of shared waiters is promoted together, or a single leading exclusive
    /// waiter is promoted alone.  A waiter that becomes holder on ALL its
    /// pending keys is moved from `pending_keys` to `held_locks` immediately.
    ///
    /// Returns the set of `TxnId`s that have been fully promoted (i.e. moved
    /// into `held_locks`).  The caller may use this list to dispatch those
    /// transactions.
    pub fn release(&mut self, txn: TxnId) -> Vec<TxnId> {
        let held = match self.held_locks.remove(&txn) {
            Some(h) => h,
            None => return Vec::new(),
        };

        let mut newly_promoted: BTreeSet<TxnId> = BTreeSet::new();

        for key in &held {
            // Drop `txn` from this key's holders. If other (shared) holders
            // remain, the key stays held and there is nothing to promote.
            let now_empty = match self.table.get_mut(key) {
                Some(entry) => {
                    entry.holders.retain(|h| *h != txn);
                    entry.holders.is_empty()
                }
                None => continue,
            };
            if now_empty {
                self.promote_waiters(key, &mut newly_promoted);
            }
        }

        newly_promoted.into_iter().collect()
    }

    /// Promote the front of `key`'s waiter queue after its holders drained.
    ///
    /// A leading run of shared waiters is granted together; a single leading
    /// exclusive waiter is granted alone; an empty queue frees the entry. Any
    /// promoted txn that is now holder on all of its pending keys is moved into
    /// `held_locks` and recorded in `newly_promoted`.
    fn promote_waiters(&mut self, key: &LockKey, newly_promoted: &mut BTreeSet<TxnId>) {
        // Decide the promotion inside a scoped borrow so the readiness sweep
        // below can re-borrow the table.
        let decision = match self.table.get_mut(key) {
            Some(entry) => match entry.waiters.front().map(|(_, mode)| *mode) {
                None => Promotion::Freed,
                Some(LockMode::Exclusive) => match entry.waiters.pop_front() {
                    Some((next, _)) => {
                        entry.mode = LockMode::Exclusive;
                        entry.holders.clear();
                        entry.holders.push(next);
                        Promotion::Promoted(vec![next])
                    }
                    None => Promotion::Freed,
                },
                Some(LockMode::Shared) => {
                    entry.mode = LockMode::Shared;
                    entry.holders.clear();
                    let mut promoted = Vec::new();
                    while matches!(entry.waiters.front(), Some((_, LockMode::Shared))) {
                        if let Some((next, _)) = entry.waiters.pop_front() {
                            entry.holders.push(next);
                            promoted.push(next);
                        }
                    }
                    Promotion::Promoted(promoted)
                }
            },
            None => return,
        };

        let promoted = match decision {
            Promotion::Freed => {
                self.table.remove(key);
                return;
            }
            Promotion::Promoted(promoted) => promoted,
        };

        // For each promoted txn, check whether it is now holder on ALL of its
        // pending keys.  If so, it is fully ready — move to held_locks.  Remove
        // first (rather than `get` + a follow-up `remove`) so there is no
        // unwrap/expect on a "just confirmed Some" invariant: the owned
        // `pending` set is reinserted on the not-yet-ready path.
        for next in promoted {
            if let Some(pending) = self.pending_keys.remove(&next) {
                let all_held = pending
                    .iter()
                    .all(|k| self.table.get(k).is_none_or(|e| e.holders.contains(&next)));
                if all_held {
                    self.held_locks.insert(next, pending);
                    newly_promoted.insert(next);
                } else {
                    self.pending_keys.insert(next, pending);
                }
            }
        }
    }

    /// Check whether a previously-blocked transaction is now ready.
    ///
    /// A transaction is ready when for every key in its key set, the key is
    /// either:
    /// - Not present in the lock table (free), or
    /// - Present in the lock table with `txn` among the current holders
    ///   (shared or exclusive).
    ///
    /// This is called after `release` returns `txn_id` in the unblocked set.
    /// If `is_ready` returns `true`, the caller calls `acquire` again which
    /// will succeed on the all-available path (because the waiter was promoted).
    pub fn is_ready(&self, txn: TxnId, keys: &BTreeSet<LockKey>) -> bool {
        keys.iter().all(|key| {
            match self.table.get(key) {
                None => true,                                // key is free
                Some(entry) => entry.holders.contains(&txn), // txn is a current holder
            }
        })
    }

    /// Number of holders of `key` that hold it as a Calvin read reservation
    /// (a `TxnId` in the reservation position band), or 0 when the key is
    /// unlocked or held only by non-reservation transactions. Used to observe
    /// reservation install/release from outside the scheduler.
    pub fn reservation_holder_count(&self, key: &LockKey) -> usize {
        self.table
            .get(key)
            .map(|e| e.holders.iter().filter(|h| h.is_reservation()).count())
            .unwrap_or(0)
    }

    /// Number of currently-held locks (entries in the lock table).
    #[cfg(test)]
    pub fn lock_count(&self) -> usize {
        self.table.len()
    }

    /// Number of transactions currently holding at least one lock.
    #[cfg(test)]
    pub fn holder_count(&self) -> usize {
        self.held_locks.len()
    }
}

impl LockEntry {
    /// Whether this entry is held exclusively by exactly `txn` (the self
    /// re-acquire case on the exclusive path).
    fn held_exclusively_by(&self, txn: TxnId) -> bool {
        self.mode == LockMode::Exclusive && self.holders.len() == 1 && self.holders[0] == txn
    }

    /// Whether this entry is held **shared** by exactly `txn` and no one else —
    /// the self-upgrade case: `txn` may take the key exclusively because it is
    /// the sole current holder.
    fn held_shared_solely_by(&self, txn: TxnId) -> bool {
        self.mode == LockMode::Shared && self.holders.len() == 1 && self.holders[0] == txn
    }

    /// Whether `txn` is already enqueued as a waiter on this entry.
    fn has_waiter(&self, txn: TxnId) -> bool {
        self.waiters.iter().any(|(w, _)| *w == txn)
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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

    #[test]
    fn acquire_free_keys_returns_ready() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);
        let outcome = lm.acquire(t, keyset(&["a", "b"]));
        assert_eq!(outcome, AcquireOutcome::Ready);
        assert_eq!(lm.lock_count(), 2);
    }

    #[test]
    fn acquire_held_key_returns_blocked_and_enqueues_waiter() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);
        lm.acquire(t1, keyset(&["x"]));

        let outcome = lm.acquire(t2, keyset(&["x"]));
        assert_eq!(outcome, AcquireOutcome::Blocked);

        // t2 should be in the waiter queue for "x".
        assert!(lm.table.get(&key("x")).unwrap().has_waiter(t2));
    }

    #[test]
    fn release_returns_unblocked_waiter_ids() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);
        lm.acquire(t1, keyset(&["x"]));
        lm.acquire(t2, keyset(&["x"]));

        let unblocked = lm.release(t1);
        assert!(unblocked.contains(&t2));
    }

    #[test]
    fn autocommit_holder_release_promotes_and_returns_scheduler_waiter() {
        // Mirrors the write-admission fast path: an autocommit-band holder takes
        // an uncontended key, a normal-band scheduler txn then blocks behind it,
        // and the holder's release promotes that scheduler txn AND returns its id
        // — the value the fast-path guard forwards to the scheduler on drop
        // (previously discarded, stranding the promoted txn as a zombie holder).
        let mut lm = LockManager::new();
        let autocommit = txn(TxnId::AUTOCOMMIT_EPOCH, 0);
        let scheduler_txn = txn(9, 0);

        assert!(
            lm.try_acquire(autocommit, keyset(&["k"])),
            "the fast-path holder takes the uncontended key"
        );
        assert_eq!(
            lm.acquire(scheduler_txn, keyset(&["k"])),
            AcquireOutcome::Blocked,
            "the scheduler txn queues behind the fast-path holder"
        );

        let promoted = lm.release(autocommit);
        assert_eq!(
            promoted,
            vec![scheduler_txn],
            "release must return the promoted scheduler waiter"
        );
        assert!(
            lm.is_ready(scheduler_txn, &keyset(&["k"])),
            "the promoted scheduler txn is now holder of the freed key"
        );
    }

    #[test]
    fn release_preserves_fifo_waiter_order() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);
        let t3 = txn(1, 2);
        lm.acquire(t1, keyset(&["x"]));
        lm.acquire(t2, keyset(&["x"]));
        lm.acquire(t3, keyset(&["x"]));

        // Release t1 — t2 should become holder (FIFO).
        lm.release(t1);
        let holder = lm.table.get(&key("x")).unwrap().holders[0];
        assert_eq!(holder, t2);

        // Release t2 — t3 should become holder.
        lm.release(t2);
        let holder = lm.table.get(&key("x")).unwrap().holders[0];
        assert_eq!(holder, t3);
    }

    #[test]
    fn multi_key_txn_releases_all_atomically() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        lm.acquire(t1, keyset(&["a", "b", "c"]));
        assert_eq!(lm.lock_count(), 3);

        lm.release(t1);
        assert_eq!(lm.lock_count(), 0);
        assert_eq!(lm.holder_count(), 0);
    }

    #[test]
    fn is_ready_returns_true_when_all_keys_free_or_self_at_front() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);
        lm.acquire(t1, keyset(&["x", "y"]));
        lm.acquire(t2, keyset(&["x", "y"]));

        // t2 is not ready while t1 holds.
        assert!(!lm.is_ready(t2, &keyset(&["x", "y"])));

        // Release t1 — t2 becomes holder on both keys.
        lm.release(t1);
        // After release, t2 is promoted to holder on both keys.
        assert!(lm.is_ready(t2, &keyset(&["x", "y"])));
    }

    #[test]
    fn shared_shared_compatible() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(lm.acquire_shared(t1, key("s")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(t2, key("s")), AcquireOutcome::Ready);

        let entry = lm.table.get(&key("s")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert!(entry.holders.contains(&t1));
        assert!(entry.holders.contains(&t2));
    }

    #[test]
    fn shared_blocks_exclusive() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(lm.acquire_shared(t1, key("k")), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(t2, keyset(&["k"])),
            AcquireOutcome::Blocked,
            "an exclusive request must block behind a shared holder"
        );
        assert!(lm.table.get(&key("k")).unwrap().has_waiter(t2));
    }

    #[test]
    fn exclusive_blocks_shared() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(lm.acquire(t1, keyset(&["k"])), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire_shared(t2, key("k")),
            AcquireOutcome::Blocked,
            "a shared request must block behind an exclusive holder"
        );
        assert!(lm.table.get(&key("k")).unwrap().has_waiter(t2));
    }

    #[test]
    fn release_promotes_shared_run_together() {
        let mut lm = LockManager::new();
        let holder = txn(1, 0);
        let s1 = txn(2, 0);
        let s2 = txn(2, 1);

        // Exclusive holder, two shared waiters queued behind it.
        assert_eq!(lm.acquire(holder, keyset(&["k"])), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(s1, key("k")), AcquireOutcome::Blocked);
        assert_eq!(lm.acquire_shared(s2, key("k")), AcquireOutcome::Blocked);

        // Releasing the exclusive holder promotes the whole run of shared
        // waiters together.
        let promoted = lm.release(holder);
        assert!(promoted.contains(&s1));
        assert!(promoted.contains(&s2));

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert!(entry.holders.contains(&s1));
        assert!(entry.holders.contains(&s2));
    }

    #[test]
    fn release_promotes_single_exclusive_waiter() {
        let mut lm = LockManager::new();
        let holder = txn(1, 0);
        let x1 = txn(2, 0);
        let x2 = txn(2, 1);

        assert_eq!(lm.acquire(holder, keyset(&["k"])), AcquireOutcome::Ready);
        assert_eq!(lm.acquire(x1, keyset(&["k"])), AcquireOutcome::Blocked);
        assert_eq!(lm.acquire(x2, keyset(&["k"])), AcquireOutcome::Blocked);

        // Only the single leading exclusive waiter is promoted.
        let promoted = lm.release(holder);
        assert_eq!(promoted, vec![x1]);

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], x1);
        // x2 is still waiting behind x1.
        assert!(entry.has_waiter(x2));
    }

    #[test]
    fn multi_holder_release() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(lm.acquire_shared(t1, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(t2, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.lock_count(), 1);

        // Releasing one shared holder leaves the other holding the key.
        lm.release(t1);
        let entry = lm.table.get(&key("k")).unwrap();
        assert!(!entry.holders.contains(&t1));
        assert!(entry.holders.contains(&t2));
        assert_eq!(lm.lock_count(), 1);

        // Releasing the last shared holder frees the key.
        lm.release(t2);
        assert_eq!(lm.lock_count(), 0);
    }

    #[test]
    fn older_writer_wounds_shared() {
        let mut lm = LockManager::new();
        let t2 = txn(1, 2); // shared holder
        let t1 = txn(1, 1); // exclusive requester, older than t2

        assert_eq!(lm.acquire_shared(t2, key("k")), AcquireOutcome::Ready);
        // The older writer wounds the younger shared holder and proceeds.
        assert_eq!(lm.acquire(t1, keyset(&["k"])), AcquireOutcome::Ready);

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert!(entry.holders.contains(&t1), "R is now the exclusive holder");
        assert!(
            !entry.holders.contains(&t2),
            "the wounded shared holder is gone"
        );
    }

    #[test]
    fn younger_writer_waits() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 1); // shared holder
        let t2 = txn(1, 2); // exclusive requester, younger than t1

        assert_eq!(lm.acquire_shared(t1, key("k")), AcquireOutcome::Ready);
        // The younger writer must not wound; it waits behind the shared holder.
        assert_eq!(lm.acquire(t2, keyset(&["k"])), AcquireOutcome::Blocked);

        let entry = lm.table.get(&key("k")).unwrap();
        assert!(entry.holders.contains(&t1), "the shared holder still holds");
        assert!(!entry.holders.contains(&t2), "R holds nothing");
        assert!(entry.has_waiter(t2), "R is enqueued as an exclusive waiter");
    }

    #[test]
    fn exclusive_waits_on_exclusive() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(lm.acquire(t1, keyset(&["k"])), AcquireOutcome::Ready);
        assert_eq!(lm.acquire(t2, keyset(&["k"])), AcquireOutcome::Blocked);
        assert!(lm.table.get(&key("k")).unwrap().has_waiter(t2));
    }

    #[test]
    fn exclusive_waits_on_exclusive_regardless_of_age() {
        let mut lm = LockManager::new();
        let t2 = txn(1, 2); // exclusive holder (younger)
        let t1 = txn(1, 1); // exclusive requester (older)

        assert_eq!(lm.acquire(t2, keyset(&["k"])), AcquireOutcome::Ready);
        // An exclusive holder is NEVER wounded, even by an older writer.
        assert_eq!(lm.acquire(t1, keyset(&["k"])), AcquireOutcome::Blocked);

        let entry = lm.table.get(&key("k")).unwrap();
        assert!(
            entry.holders.contains(&t2),
            "the exclusive holder is intact"
        );
        assert!(!entry.holders.contains(&t1));
        assert!(entry.has_waiter(t1));
    }

    #[test]
    fn multi_key_atomic_wound_takes_both() {
        let mut lm = LockManager::new();
        let s1 = txn(1, 5); // shared holder on k1, younger than R
        let s2 = txn(1, 6); // shared holder on k2, younger than R
        let r = txn(1, 1); // exclusive requester, older than both

        assert_eq!(lm.acquire_shared(s1, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(s2, key("k2")), AcquireOutcome::Ready);

        assert_eq!(lm.acquire(r, keyset(&["k1", "k2"])), AcquireOutcome::Ready);

        for k in ["k1", "k2"] {
            let entry = lm.table.get(&key(k)).unwrap();
            assert_eq!(entry.mode, LockMode::Exclusive);
            assert!(entry.holders.contains(&r), "R holds {k}");
        }
        assert!(!lm.table.get(&key("k1")).unwrap().holders.contains(&s1));
        assert!(!lm.table.get(&key("k2")).unwrap().holders.contains(&s2));
    }

    #[test]
    fn multi_key_atomic_wait_holds_none() {
        let mut lm = LockManager::new();
        let s1 = txn(1, 5); // shared holder on k1, younger than R
        let s2 = txn(1, 0); // shared holder on k2, OLDER than R
        let r = txn(1, 1); // exclusive requester

        assert_eq!(lm.acquire_shared(s1, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(s2, key("k2")), AcquireOutcome::Ready);

        // R is younger than the holder on k2, so it must wait on BOTH keys and
        // hold neither (all-or-nothing).
        assert_eq!(
            lm.acquire(r, keyset(&["k1", "k2"])),
            AcquireOutcome::Blocked
        );

        assert!(
            !lm.table.get(&key("k1")).unwrap().holders.contains(&r),
            "R holds no key"
        );
        assert!(!lm.table.get(&key("k2")).unwrap().holders.contains(&r));
        // The older shared holder on k2 is untouched.
        assert!(lm.table.get(&key("k2")).unwrap().holders.contains(&s2));
    }

    #[test]
    fn crossed_reservations_are_acyclic() {
        // T1 holds shared K1 and wants exclusive K2; T2 holds shared K2 and
        // wants exclusive K1. The older writer's exclusive acquire wounds the
        // younger's shared holding, breaking the cycle — no deadlock.
        let mut lm = LockManager::new();
        let t1 = txn(1, 1); // older
        let t2 = txn(1, 2); // younger

        assert_eq!(lm.acquire_shared(t1, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(t2, key("k2")), AcquireOutcome::Ready);

        // T1 (older) acquires exclusive K2: wounds T2's shared holding and
        // proceeds.
        assert_eq!(lm.acquire(t1, keyset(&["k2"])), AcquireOutcome::Ready);

        let k2 = lm.table.get(&key("k2")).unwrap();
        assert_eq!(k2.mode, LockMode::Exclusive);
        assert!(k2.holders.contains(&t1), "the older writer proceeds");
        assert!(
            !k2.holders.contains(&t2),
            "the younger's reservation is wounded away"
        );
    }

    #[test]
    fn shared_reservation_self_upgrades_to_exclusive() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);

        assert_eq!(lm.acquire_shared(t, key("k")), AcquireOutcome::Ready);
        // The txn re-acquires its own shared reservation exclusively — this must
        // NOT self-deadlock by blocking on its own held key.
        assert_eq!(
            lm.acquire(t, keyset(&["k"])),
            AcquireOutcome::Ready,
            "self-upgrade from shared to exclusive must not block"
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], t);
    }

    #[test]
    fn self_upgrade_with_other_shared_holder_blocks_or_wounds() {
        // T_old is older than T_young: T_old's self-upgrade must wound T_young.
        let mut lm = LockManager::new();
        let t_old = txn(1, 0);
        let t_young = txn(1, 1);

        assert_eq!(lm.acquire_shared(t_old, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(t_young, key("k")), AcquireOutcome::Ready);

        assert_eq!(
            lm.acquire(t_old, keyset(&["k"])),
            AcquireOutcome::Ready,
            "the older self-upgrader wounds the younger shared holder"
        );
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], t_old);

        // Symmetric case: the YOUNGER of the two self-upgrades and must block.
        let mut lm = LockManager::new();
        let t_old = txn(1, 0);
        let t_young = txn(1, 1);

        assert_eq!(lm.acquire_shared(t_old, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(t_young, key("k")), AcquireOutcome::Ready);

        assert_eq!(
            lm.acquire(t_young, keyset(&["k"])),
            AcquireOutcome::Blocked,
            "the younger self-upgrader must wait behind the older shared holder"
        );
        // t_young drops its own shared hold (degrading to plain OCC) so the key
        // can drain to empty and its exclusive request can later be promoted;
        // t_old remains the sole shared holder, and t_young is enqueued as an
        // exclusive waiter rather than left stuck as a non-waiting holder.
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert!(entry.holders.contains(&t_old));
        assert!(!entry.holders.contains(&t_young));
        assert!(entry.has_waiter(t_young));

        // Once t_old releases, t_young is promoted to sole exclusive holder.
        let unblocked = lm.release(t_old);
        assert!(unblocked.contains(&t_young));
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], t_young);
    }

    #[test]
    fn self_upgrade_mixed_with_conflict_on_other_key() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);
        let u = txn(1, 1);

        // T reserves K1 shared; U holds K2 exclusively.
        assert_eq!(lm.acquire_shared(t, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire(u, keyset(&["k2"])), AcquireOutcome::Ready);

        // T tries to take both keys exclusively: K2 conflicts with U, so T must
        // block on the whole set — and critically must NOT self-deadlock on K1.
        assert_eq!(
            lm.acquire(t, keyset(&["k1", "k2"])),
            AcquireOutcome::Blocked,
            "conflict on k2 blocks the whole set"
        );

        // After U releases K2, T's re-acquire succeeds and upgrades K1 in place.
        lm.release(u);
        assert_eq!(
            lm.acquire(t, keyset(&["k1", "k2"])),
            AcquireOutcome::Ready,
            "once k2 frees up, t acquires both keys exclusively"
        );
        let k1 = lm.table.get(&key("k1")).unwrap();
        assert_eq!(k1.mode, LockMode::Exclusive);
        assert_eq!(k1.holders.len(), 1);
        assert_eq!(k1.holders[0], t);
        let k2 = lm.table.get(&key("k2")).unwrap();
        assert_eq!(k2.mode, LockMode::Exclusive);
        assert_eq!(k2.holders.len(), 1);
        assert_eq!(k2.holders[0], t);
    }
}
