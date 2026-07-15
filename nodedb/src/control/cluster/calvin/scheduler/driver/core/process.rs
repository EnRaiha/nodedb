// SPDX-License-Identifier: BUSL-1.1

//! New-txn processing, dependent-read barrier setup, and txn-completion
//! bookkeeping for the Calvin scheduler.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use nodedb_cluster::calvin::types::{
    LockKeyWire, ReleaseReason, SchedulerInput, SequencedTxn, TxnIdWire,
};

use super::super::barrier::PendingDependentBarrier;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::{AcquireOutcome, TxnId};

impl Scheduler {
    /// Route a fanned-out scheduler input to its handler.
    ///
    /// Every replica applies the sequencer-ordered inputs in identical order, so
    /// the resulting `process`/`acquire_shared`/`release` calls are identical
    /// across replicas — the determinism contract. No wall clock or local state
    /// enters the routing decision.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn process_scheduler_input(
        &mut self,
        input: SchedulerInput,
    ) {
        match input {
            SchedulerInput::Txn(txn) => self.process_new_txn(txn),
            SchedulerInput::Reserve { owner, key } => self.install_reservation(owner, key),
            SchedulerInput::Release { owner, reason } => self.release_reservation(owner, reason),
        }
    }

    /// Install a SHARED reservation on `key` for interactive txn `owner`.
    ///
    /// The `AcquireOutcome` is intentionally discarded: promotion of a
    /// reservation that blocks behind an exclusive holder is handled by the
    /// existing FIFO waiter mechanics, and lock-promotion wiring for reservations
    /// lands in a later change.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn install_reservation(
        &mut self,
        owner: TxnIdWire,
        key: LockKeyWire,
    ) {
        let txn_id: TxnId = owner.into();
        let lock_key = super::super::helpers::decode_lock_key(&key);
        let mut lm = self.lock_manager.lock().unwrap_or_else(|p| p.into_inner());
        let _outcome = lm.acquire_shared(txn_id, lock_key);
    }

    /// Release ALL shared reservations held by `owner`, promoting any waiters that
    /// become ready — the same promotion -> dispatch path `on_txn_complete` uses.
    ///
    /// `reason` is carried for observability only; the release is identical for
    /// every reason.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn release_reservation(
        &mut self,
        owner: TxnIdWire,
        reason: ReleaseReason,
    ) {
        let txn_id: TxnId = owner.into();
        tracing::debug!(
            vshard = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            ?reason,
            "calvin: releasing shared reservation"
        );
        let newly_unblocked = {
            let lm = Arc::clone(&self.lock_manager);
            let mut guard = lm.lock().unwrap_or_else(|p| p.into_inner());
            guard.release(txn_id)
        };
        self.dispatch_promoted(newly_unblocked);
    }

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
                self.dispatch_or_barrier(txn, txn_id);
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
    ) {
        let is_dependent = txn.tx_class.dependent_reads.is_some();
        if is_dependent {
            self.insert_dependent_barrier(txn, txn_id);
        } else {
            self.dispatch_txn(txn, txn_id);
        }
    }

    /// Insert a dependent-read barrier for an active vshard.
    fn insert_dependent_barrier(&mut self, txn: SequencedTxn, txn_id: TxnId) {
        let spec = match &txn.tx_class.dependent_reads {
            Some(s) => s,
            None => {
                // Shouldn't happen; fall through to static dispatch.
                self.dispatch_txn(txn, txn_id);
                return;
            }
        };

        let waiting_for: std::collections::BTreeSet<u32> =
            spec.passive_reads.keys().copied().collect();
        // no-determinism: passive barrier timeout is scheduler observability, not Calvin WAL data
        let timeout_at = Instant::now() + self.config.passive_timeout();

        let barrier = PendingDependentBarrier {
            txn,
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

        // Release this txn's locks. `release` promotes any waiter queued behind
        // each freed key to holder (moving it pending -> held) and returns the
        // fully-promoted ids. Those ids are already holders in the table the
        // moment `release` returns, so a concurrent gate probe on the same key
        // sees the promoted holder and cannot steal it — the subsequent dispatch
        // is safe outside this critical section.
        let newly_unblocked = {
            let lm = Arc::clone(&self.lock_manager);
            let mut guard = lm.lock().unwrap_or_else(|p| p.into_inner());
            guard.release(txn_id)
        };
        self.dispatch_promoted(newly_unblocked);

        // Mark this EXACT position applied. The watermark folds an epoch only
        // once ALL of its positions for this vShard have terminally completed,
        // so any advertised watermark reflects a FULLY-applied epoch — the value
        // `BEGIN` needs for a torn-free cross-shard snapshot anchor.
        if let Some(watermark) = self.applied.mark_applied(epoch, txn_id.position) {
            self.publish_watermark(watermark);
        }

        self.pending.remove(&txn_id);
    }

    /// Dispatch transactions that a `LockManager::release` promoted to holder.
    ///
    /// Shared by both promotion entry points:
    /// - `on_txn_complete`, where THIS scheduler released a completed txn's locks;
    /// - the `promotion_rx` `select!` arm, where a Control-Plane fast-path
    ///   [`WriteAdmissionGuard`] drop released an uncontended key that one of this
    ///   scheduler's blocked txns had queued behind.
    ///
    /// In both cases `release` has already installed each promoted txn as holder
    /// on all its keys and moved it into `held_locks`. This method confirms
    /// readiness (an idempotent re-acquire — a no-op that also guards against a
    /// promoted id that is somehow not yet ready), clears the `blocked` entry,
    /// records the wait, and routes the txn to static dispatch or a dependent
    /// barrier. Collect-under-guard then dispatch-after-drop keeps the lock-table
    /// critical section minimal and re-entrancy-safe (`dispatch_or_barrier` does
    /// not touch the lock table).
    ///
    /// [`WriteAdmissionGuard`]: crate::control::server::shared::write_admission::WriteAdmissionGuard
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_promoted(
        &mut self,
        promoted: Vec<TxnId>,
    ) {
        if promoted.is_empty() {
            return;
        }

        let lm = Arc::clone(&self.lock_manager);
        let mut to_dispatch: Vec<(SequencedTxn, TxnId)> = Vec::new();
        {
            let mut guard = lm.lock().unwrap_or_else(|p| p.into_inner());
            for waiter_id in promoted {
                let Some(blocked) = self.blocked.get(&waiter_id) else {
                    // A promotion can only name a waiter this scheduler enqueued
                    // (its key set lives in `blocked`), so a miss should not
                    // happen. Skip defensively rather than panic — the txn holds
                    // no dispatch state here to act on.
                    tracing::debug!(
                        vshard = self.vshard_id,
                        epoch = waiter_id.epoch,
                        position = waiter_id.position,
                        "calvin: promoted txn absent from blocked map; skipping dispatch"
                    );
                    continue;
                };
                if !guard.is_ready(waiter_id, &blocked.keys) {
                    continue;
                }
                let keys = blocked.keys.clone();
                let outcome = guard.acquire(waiter_id, keys);
                debug_assert_eq!(
                    outcome,
                    AcquireOutcome::Ready,
                    "is_ready returned true but acquire returned Blocked"
                );

                if let Some(blocked_txn) = self.blocked.remove(&waiter_id) {
                    let wait_ms = blocked_txn.blocked_at.elapsed().as_millis() as u64;
                    self.metrics.record_lock_wait_ms(wait_ms);
                    to_dispatch.push((blocked_txn.txn, waiter_id));
                }
            }
        }

        for (txn, waiter_id) in to_dispatch {
            self.dispatch_or_barrier(txn, waiter_id);
        }
    }
}
