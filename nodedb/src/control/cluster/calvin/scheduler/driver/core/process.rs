// SPDX-License-Identifier: BUSL-1.1

//! New-txn processing, dependent-read barrier setup, and txn-completion
//! bookkeeping for the Calvin scheduler.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::super::barrier::PendingDependentBarrier;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::{AcquireOutcome, LockKey, TxnId};

impl Scheduler {
    /// Process a newly arrived sequenced transaction.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn process_new_txn(
        &mut self,
        txn: SequencedTxn,
    ) {
        let txn_id = TxnId::new(txn.epoch, txn.position);

        // Record this delivery with the sequencer's per-`(epoch, vShard)` count.
        // A count >= 1 is the authoritative expected total (every position of the
        // epoch carries the same value, so this is idempotent); a count of 0
        // marks a batch encoded before the count field existed, and the position
        // is tracked so the epoch can fold via in-order delivery instead.
        self.applied
            .note_expected(txn.epoch, txn.position, txn.epoch_vshard_txn_count);

        // Exact per-position skip: never re-apply a position that already
        // committed (its CalvinApplied marker is durable), and never re-run a
        // whole epoch that has fully folded into the watermark. Re-running an
        // applied position would re-fire its side effects — this gate IS the
        // exactly-once mechanism. Skipping a whole epoch on its first completing
        // position (the previous per-epoch gate) dropped every other position of
        // that epoch across a restart: a torn transaction.
        if self.applied.is_applied(txn.epoch, txn.position) {
            // Learning the count for an already-applied position may complete a
            // historical epoch's applied set (during restart re-fan-out), folding
            // it into the watermark and pruning its tail — bounding memory.
            if let Some(watermark) = self.applied.advance() {
                self.publish_watermark(watermark);
            }
            return;
        }

        let keys = super::super::helpers::expand_rw_set(&txn);
        let keys_count = keys.len();
        let _acquire_span = tracing::info_span!(
            "scheduler_acquire_locks",
            epoch = txn.epoch,
            position = txn.position,
            vshard = self.vshard_id,
            keys_count,
        )
        .entered();
        let outcome = {
            let mut lm = self.lock_manager.lock().unwrap_or_else(|p| p.into_inner());
            lm.acquire(txn_id, keys.clone())
        };

        match outcome {
            AcquireOutcome::Ready => {
                // no-determinism: lock_acquired_time is scheduler observability, not Calvin WAL data
                self.dispatch_or_barrier(txn, txn_id, keys, Instant::now());
            }
            AcquireOutcome::Blocked => {
                self.metrics.record_blocked();
                self.blocked.insert(
                    txn_id,
                    super::super::types::BlockedTxn {
                        txn,
                        keys,
                        // no-determinism: blocked_at is scheduler observability, not Calvin WAL data
                        blocked_at: Instant::now(),
                    },
                );
            }
        }
    }

    /// Route a ready txn to either a static dispatch or a dependent barrier.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_or_barrier(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        keys: std::collections::BTreeSet<LockKey>,
        lock_acquired_time: Instant,
    ) {
        let is_dependent = txn.tx_class.dependent_reads.is_some();
        if is_dependent {
            self.insert_dependent_barrier(txn, txn_id, keys, lock_acquired_time);
        } else {
            self.dispatch_txn(txn, txn_id, keys, lock_acquired_time);
        }
    }

    /// Insert a dependent-read barrier for an active vshard.
    fn insert_dependent_barrier(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        keys: std::collections::BTreeSet<LockKey>,
        lock_acquired_time: Instant,
    ) {
        let spec = match &txn.tx_class.dependent_reads {
            Some(s) => s,
            None => {
                // Shouldn't happen; fall through to static dispatch.
                self.dispatch_txn(txn, txn_id, keys, lock_acquired_time);
                return;
            }
        };

        let waiting_for: std::collections::BTreeSet<u32> =
            spec.passive_reads.keys().copied().collect();
        // no-determinism: passive barrier timeout is scheduler observability, not Calvin WAL data
        let timeout_at = Instant::now() + self.config.passive_timeout();

        let barrier = PendingDependentBarrier {
            txn,
            keys,
            lock_acquired_time,
            waiting_for,
            received: BTreeMap::new(),
            timeout_at,
        };

        self.dependent_barrier.insert(txn_id, barrier);
    }

    /// Called when a transaction completes (success or infrastructure error).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn on_txn_complete(
        &mut self,
        txn_id: TxnId,
    ) {
        let epoch = txn_id.epoch;

        // Release this txn's locks, promote any newly-unblocked waiters, and
        // collect the ones ready to dispatch — all under ONE guard so the
        // release and the promoted re-acquire are atomic against a concurrent
        // gate probe. Dispatch happens AFTER the guard drops: `dispatch_or_barrier`
        // does not touch the lock table, but holding the guard across it would
        // deadlock if it ever did, so the collect-then-dispatch split keeps the
        // critical section minimal and re-entrancy-safe.
        let lm = Arc::clone(&self.lock_manager);
        let mut to_dispatch: Vec<(SequencedTxn, TxnId, std::collections::BTreeSet<LockKey>)> =
            Vec::new();
        {
            let mut guard = lm.lock().unwrap_or_else(|p| p.into_inner());
            let newly_unblocked = guard.release(txn_id);

            for waiter_id in newly_unblocked {
                if let Some(blocked) = self.blocked.get(&waiter_id)
                    && guard.is_ready(waiter_id, &blocked.keys)
                {
                    let keys = blocked.keys.clone();
                    let outcome = guard.acquire(waiter_id, keys.clone());
                    debug_assert_eq!(
                        outcome,
                        AcquireOutcome::Ready,
                        "is_ready returned true but acquire returned Blocked"
                    );

                    if let Some(blocked_txn) = self.blocked.remove(&waiter_id) {
                        let wait_ms = blocked_txn.blocked_at.elapsed().as_millis() as u64;
                        self.metrics.record_lock_wait_ms(wait_ms);
                        to_dispatch.push((blocked_txn.txn, waiter_id, keys));
                    }
                }
            }
        }

        for (txn, waiter_id, keys) in to_dispatch {
            // no-determinism: lock_acquired_time for unblocked txn is scheduler observability, not Calvin WAL data
            self.dispatch_or_barrier(txn, waiter_id, keys, Instant::now());
        }

        // Mark this EXACT position applied. The watermark folds an epoch only
        // once ALL of its positions for this vShard have terminally completed,
        // so any advertised watermark reflects a FULLY-applied epoch — the value
        // `BEGIN` needs for a torn-free cross-shard snapshot anchor.
        if let Some(watermark) = self.applied.mark_applied(epoch, txn_id.position) {
            self.publish_watermark(watermark);
        }

        self.pending.remove(&txn_id);
    }
}
