// SPDX-License-Identifier: BUSL-1.1

//! Deterministic lock manager for the Calvin scheduler.
//!
//! # Design
//!
//! The lock manager provides a deterministic, totally-ordered lock table over
//! per-key entries keyed by [`LockKey`].  All locks are exclusive — every key
//! in a transaction's `read_set ∪ write_set` is acquired as an exclusive (write)
//! lock.
//!
//! # Why write-locks-only
//!
//! Deterministic single-threaded-per-vshard execution makes shared read locks
//! unnecessary.  Calvin guarantees that every replica applies transactions in the
//! same global order.  Because there is only one execution thread per vshard,
//! there is no concurrent reader that shared locks would need to permit.  Adding
//! shared read locks would complicate the waiter queue without providing any
//! throughput benefit.
//!
//! # Determinism
//!
//! `BTreeMap` is used throughout (not `HashMap`) so that iteration order is
//! deterministic and reproducible across replicas.  This is a correctness
//! requirement, not a style preference.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

// ── LockKey ───────────────────────────────────────────────────────────────────

/// A deterministic key identifying one lockable unit in the Calvin lock table.
///
/// Keys are totally ordered by their `Ord` impl so `BTreeMap` and `BTreeSet`
/// over `LockKey` produce a stable, portable ordering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockKey {
    /// Document / Vector engine: a single row identified by its surrogate.
    Surrogate {
        /// Collection name (interned string for cheap clone).
        collection: Arc<str>,
        /// Global surrogate identifier for this row.
        surrogate: u32,
    },
    /// Key-Value engine: a single row identified by raw bytes.
    Kv {
        collection: Arc<str>,
        /// Raw byte key (interned slice for cheap clone).
        key: Arc<[u8]>,
    },
    /// Graph edge: directed edge identified by (src, dst) surrogate pair.
    Edge {
        collection: Arc<str>,
        src: u32,
        dst: u32,
    },
}

// ── TxnId ─────────────────────────────────────────────────────────────────────

/// A globally unique, totally ordered transaction identifier.
///
/// `(epoch, position)` is the Calvin schedule position.  `BTreeMap<TxnId, _>`
/// and `BTreeSet<TxnId>` iterate in `(epoch, position)` order, which is the
/// deterministic dispatch order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId {
    /// Sequencer epoch.
    pub epoch: u64,
    /// Zero-based position within the epoch batch.
    pub position: u32,
}

impl TxnId {
    /// Reserved epoch band for autocommit fast-path lock holders. A holder in
    /// this band never collides with a real Calvin `(epoch, position)` schedule
    /// position, so the fast path and the deterministic scheduler can share one
    /// lock table without identity aliasing.
    pub const AUTOCOMMIT_EPOCH: u64 = u64::MAX;

    pub fn new(epoch: u64, position: u32) -> Self {
        Self { epoch, position }
    }

    /// Whether this id belongs to the reserved autocommit fast-path band. FIFO
    /// waiter ordering is insertion-order (`VecDeque`), and `BTreeMap` iteration
    /// merely sorts the reserved band last — neither is affected by the band.
    pub fn is_autocommit(&self) -> bool {
        self.epoch == Self::AUTOCOMMIT_EPOCH
    }
}

// ── AcquireOutcome ────────────────────────────────────────────────────────────

/// Result of a [`LockManager::acquire`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOutcome {
    /// All requested locks were free; the transaction is ready to dispatch.
    Ready,
    /// At least one key was already held by another transaction.  The
    /// transaction has been enqueued as a waiter on every unavailable key.
    Blocked,
}

// ── LockEntry ─────────────────────────────────────────────────────────────────

/// Per-key lock state.
struct LockEntry {
    /// The transaction currently holding this lock.
    holder: TxnId,
    /// Transactions waiting for this lock, in FIFO order.
    waiters: VecDeque<TxnId>,
}

// ── LockManager ───────────────────────────────────────────────────────────────

/// Deterministic Calvin lock manager for one vshard.
///
/// Manages an in-memory lock table keyed by [`LockKey`].  The table is held in
/// a `BTreeMap` so iteration is always deterministic.
///
/// # Key sets tracked per transaction
///
/// - `held_locks`: key sets for transactions that are the current holder on ALL
///   their keys and are actively executing (i.e. dispatched to the Data Plane).
/// - `pending_keys`: key sets for transactions that are blocked waiting for at
///   least one key.  When `release` promotes a blocked txn to holder on every
///   one of its keys, the entry moves from `pending_keys` to `held_locks`.
pub struct LockManager {
    /// Per-key lock entries.  Uses `BTreeMap` for deterministic iteration.
    table: BTreeMap<LockKey, LockEntry>,
    /// Per-transaction set of currently held keys for **dispatched** txns.
    /// Used by `release` to iterate the key set without a full table scan.
    held_locks: BTreeMap<TxnId, BTreeSet<LockKey>>,
    /// Key sets for **blocked** (not-yet-dispatched) txns.  Populated when
    /// `acquire` returns `Blocked`; cleared (moved to `held_locks`) when all
    /// keys have been acquired on the promotion path inside `release`.
    pending_keys: BTreeMap<TxnId, BTreeSet<LockKey>>,
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

    /// Attempt to acquire exclusive locks on all keys for `txn`.
    ///
    /// If every key is free or already held by `txn` (promoted from waiter),
    /// records `txn` as holder of each key and returns
    /// [`AcquireOutcome::Ready`].
    ///
    /// If any key is held by a different transaction, enqueues `txn` as a
    /// waiter on every unavailable key and returns [`AcquireOutcome::Blocked`].
    /// The txn's key set is stored in `pending_keys` so that `release` can
    /// promote it atomically when all keys become available.
    pub fn acquire(&mut self, txn: TxnId, keys: BTreeSet<LockKey>) -> AcquireOutcome {
        // First pass: determine whether any key is held by a *different* txn.
        // A key already held by `txn` itself (promoted via release) counts as
        // available — re-acquire on that key is a no-op.
        let all_available = keys.iter().all(|k| match self.table.get(k) {
            None => true,
            Some(entry) => entry.holder == txn,
        });

        if all_available {
            // Acquire all keys.  For keys not yet in the table (free), insert a
            // new entry.  For keys already held by this txn (promoted waiter),
            // leave the entry unchanged — the waiter queue is intact.
            for key in &keys {
                if !self.table.contains_key(key) {
                    self.table.insert(
                        key.clone(),
                        LockEntry {
                            holder: txn,
                            waiters: VecDeque::new(),
                        },
                    );
                }
            }
            // Move out of pending (if the txn was previously blocked on this
            // same key set) and into held_locks.
            self.pending_keys.remove(&txn);
            self.held_locks.insert(txn, keys);
            AcquireOutcome::Ready
        } else {
            // Enqueue as waiter on every key held by a different txn.
            for key in &keys {
                if let Some(entry) = self.table.get_mut(key)
                    && entry.holder != txn
                    && !entry.waiters.contains(&txn)
                {
                    entry.waiters.push_back(txn);
                }
                // Free keys: no entry exists; the txn will acquire them on the
                // re-acquire path after all held keys are released.
            }
            // Store the full key set so that release can promote this txn
            // atomically once all its keys become available.
            self.pending_keys.insert(txn, keys);
            AcquireOutcome::Blocked
        }
    }

    /// Non-blocking acquire: take all `keys` for `txn` iff every one is free (or
    /// already held by `txn`), returning `true`; otherwise return `false` WITHOUT
    /// enqueuing a waiter or recording any pending state.
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
    /// For each released key, promotes the front-of-queue waiter to holder.
    /// When a promoted waiter is now holder on ALL its pending keys, it is
    /// moved from `pending_keys` to `held_locks` immediately — the caller does
    /// not need to call `acquire` again for promotion to take effect.
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
            if let Some(entry) = self.table.get_mut(key)
                && entry.holder == txn
            {
                if let Some(next) = entry.waiters.pop_front() {
                    // Promote waiter to holder on this key.
                    entry.holder = next;

                    // Check whether `next` is now holder on ALL of its pending
                    // keys.  If so, it is fully ready — move to held_locks.
                    // Remove first (rather than `get` + a follow-up `remove`)
                    // so there is no unwrap/expect on a "just confirmed Some"
                    // invariant: the owned `pending` set is reinserted on the
                    // not-yet-ready path.
                    if let Some(pending) = self.pending_keys.remove(&next) {
                        let all_held = pending
                            .iter()
                            .all(|k| self.table.get(k).is_none_or(|e| e.holder == next));
                        if all_held {
                            self.held_locks.insert(next, pending);
                            newly_promoted.insert(next);
                        } else {
                            self.pending_keys.insert(next, pending);
                        }
                    }
                } else {
                    // No waiters: remove the entry entirely.
                    self.table.remove(key);
                }
            }
        }

        newly_promoted.into_iter().collect()
    }

    /// Check whether a previously-blocked transaction is now ready.
    ///
    /// A transaction is ready when for every key in its key set, the key is
    /// either:
    /// - Not present in the lock table (free), or
    /// - Present in the lock table with `txn` as the current holder.
    ///
    /// This is called after `release` returns `txn_id` in the unblocked set.
    /// If `is_ready` returns `true`, the caller calls `acquire` again which
    /// will succeed on the `all_free` path (because the waiter was promoted).
    pub fn is_ready(&self, txn: TxnId, keys: &BTreeSet<LockKey>) -> bool {
        keys.iter().all(|key| {
            match self.table.get(key) {
                None => true,                       // key is free
                Some(entry) => entry.holder == txn, // txn is the current holder
            }
        })
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

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(lm.table.get(&key("x")).unwrap().waiters.contains(&t2));
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
        let holder = lm.table.get(&key("x")).unwrap().holder;
        assert_eq!(holder, t2);

        // Release t2 — t3 should become holder.
        lm.release(t2);
        let holder = lm.table.get(&key("x")).unwrap().holder;
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
}
