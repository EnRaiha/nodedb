// SPDX-License-Identifier: BUSL-1.1

//! Calvin sequencer Raft state machine.
//!
//! [`SequencerStateMachine`] is called on every replica (including the leader)
//! when a `SequencerEntry` is committed to the Raft log. It:
//!
//! 1. Decodes the `SequencerEntry` from the raw log bytes.
//! 2. Checks epoch monotonicity to detect log gaps (a gap means the apply path
//!    is broken and the node should not fan out — it logs an error and skips).
//! 3. Fans the `EpochBatch` out to per-vshard output channels. Uses `try_send`
//!    so the apply loop is never blocked. A full channel logs and drops — the
//!    scheduler's log-replay path will catch up.
//! 4. Advances `last_applied_epoch`.
//!
//! The `last_applied_epoch` counter is kept in memory only. On node restart the
//! sequencer group's Raft log is replayed from the beginning (or from the
//! latest snapshot), and the counter is rebuilt monotonically. This is safe
//! because the state machine is deterministic.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::calvin::CalvinCompletionRegistry;
use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::types::SchedulerInput;

/// Atomic counters for the sequencer state machine apply path.
pub struct StateMachineMetrics {
    /// Total epoch batches successfully applied.
    pub epochs_applied: AtomicU64,
    /// Total transactions fanned out to vshard channels.
    pub txns_fanned_out: AtomicU64,
    /// Transactions dropped because the vshard channel was full.
    pub txns_dropped_backpressure: AtomicU64,
    /// Epochs skipped because of a gap in the epoch sequence.
    pub epochs_skipped_gap: AtomicU64,
}

impl StateMachineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            epochs_applied: AtomicU64::new(0),
            txns_fanned_out: AtomicU64::new(0),
            txns_dropped_backpressure: AtomicU64::new(0),
            epochs_skipped_gap: AtomicU64::new(0),
        })
    }
}

impl Default for StateMachineMetrics {
    fn default() -> Self {
        Self {
            epochs_applied: AtomicU64::new(0),
            txns_fanned_out: AtomicU64::new(0),
            txns_dropped_backpressure: AtomicU64::new(0),
            epochs_skipped_gap: AtomicU64::new(0),
        }
    }
}

/// The Calvin sequencer Raft state machine.
///
/// One instance per replica (including leader). Applied on every `CommitApplier`
/// callback for the sequencer Raft group.
pub struct SequencerStateMachine {
    /// Last successfully applied epoch. Used for gap detection.
    /// The first valid epoch is 0; `last_applied_epoch = u64::MAX` means nothing
    /// has been applied yet (using `u64::MAX` avoids a separate `Option` and
    /// makes the "nothing applied" state explicit).
    last_applied_epoch: u64,
    /// Raft log index of the last committed entry applied on this replica.
    /// `NOT_YET_APPLIED` means nothing has been applied yet. Advanced for EVERY
    /// applied entry (not just `EpochBatch`), so it is a safe upper bound for the
    /// scheduler's catch-up `read_committed_entries(lo, hi)` range.
    last_committed_index: u64,
    /// Per-vshard output channels. The scheduler subscribes on the other end.
    vshard_senders: HashMap<u32, mpsc::Sender<SchedulerInput>>,
    /// Per-vShard "catch up from this Raft index" bookkeeping.
    ///
    /// When a fan-out `try_send` to a vShard's scheduler channel fails (Full or
    /// Closed), the input for that vShard was dropped. The current entry's Raft
    /// index is recorded here with MIN-COLLAPSE (the smallest dropped index per
    /// vShard wins), so the scheduler-side drain replays the sequencer Raft log
    /// from the earliest miss forward. Bounded by the number of hosted vShards —
    /// a vShard contributes at most one entry until its catch-up is drained.
    catch_up_from: Mutex<HashMap<u32, u64>>,
    pub metrics: Arc<StateMachineMetrics>,
    completion_registry: Arc<CalvinCompletionRegistry>,
}

const NOT_YET_APPLIED: u64 = u64::MAX;

impl SequencerStateMachine {
    /// Construct a fresh state machine with no applied epochs.
    pub fn new(
        vshard_senders: HashMap<u32, mpsc::Sender<SchedulerInput>>,
        completion_registry: Arc<CalvinCompletionRegistry>,
    ) -> Self {
        Self {
            last_applied_epoch: NOT_YET_APPLIED,
            last_committed_index: NOT_YET_APPLIED,
            vshard_senders,
            catch_up_from: Mutex::new(HashMap::new()),
            metrics: StateMachineMetrics::new(),
            completion_registry,
        }
    }

    /// The last epoch number that was successfully applied, or `None` if no
    /// epoch has been applied yet.
    pub fn last_applied_epoch(&self) -> Option<u64> {
        if self.last_applied_epoch == NOT_YET_APPLIED {
            None
        } else {
            Some(self.last_applied_epoch)
        }
    }

    /// The epoch number that the next proposal should use.
    pub fn next_epoch(&self) -> u64 {
        if self.last_applied_epoch == NOT_YET_APPLIED {
            0
        } else {
            self.last_applied_epoch + 1
        }
    }

    /// Register (or replace) the output sender for a vshard.
    ///
    /// Call this when a scheduler subscribes for a vshard hosted on this node.
    pub fn set_vshard_sender(&mut self, vshard: u32, sender: mpsc::Sender<SchedulerInput>) {
        self.vshard_senders.insert(vshard, sender);
    }

    /// Remove the output sender for a vshard (e.g. when a vshard is migrated
    /// away from this node).
    pub fn remove_vshard_sender(&mut self, vshard: u32) {
        self.vshard_senders.remove(&vshard);
    }

    /// The highest epoch number that has been committed and applied on this
    /// replica, or `None` if no epoch has been applied yet.
    ///
    /// Used by the Calvin scheduler's rebuild path: the scheduler captures
    /// this value before processing the Raft log to determine the upper bound
    /// of the rebuild range (`E+1 ..= current_committed_epoch`).
    pub fn current_committed_epoch(&self) -> Option<u64> {
        self.last_applied_epoch()
    }

    /// The Raft log index of the highest committed entry applied on this replica,
    /// or `None` if nothing has been applied yet.
    ///
    /// Advanced for EVERY applied entry (not just `EpochBatch`), so the scheduler
    /// can use it as a safe upper bound (`hi`) for the catch-up replay range
    /// `read_committed_entries(SEQUENCER_GROUP, lo ..= hi)`.
    pub fn current_committed_index(&self) -> Option<u64> {
        if self.last_committed_index == NOT_YET_APPLIED {
            None
        } else {
            Some(self.last_committed_index)
        }
    }

    /// Record that a fan-out to `vshard` was dropped at Raft index `index`.
    ///
    /// Min-collapse: the smallest dropped index per vShard is retained, so the
    /// scheduler-side drain replays from the earliest miss forward. O(1), no I/O.
    fn record_catch_up(&self, vshard: u32, index: u64) {
        let mut map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        map.entry(vshard)
            .and_modify(|i| *i = (*i).min(index))
            .or_insert(index);
    }

    /// Take (remove and return) the catch-up-from Raft index for `vshard`.
    ///
    /// Contract: TAKE semantics — the entry is cleared, so the scheduler-side
    /// drain consumes each recorded miss exactly once. Returns `None` when no
    /// drop is pending for the vShard. The next drop re-records a fresh index.
    pub fn take_catch_up_from(&self, vshard: u32) -> Option<u64> {
        let mut map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(&vshard)
    }

    /// Arm a catch-up for `vshard` from `index` (min-collapse), so the
    /// scheduler-side drain replays committed sequencer entries from there.
    ///
    /// Called when a scheduler subscribes for a vShard: the sequencer may have
    /// already committed (and fanned out to a then-absent sender — silently
    /// skipped) epochs for this vShard before the scheduler existed. A fresh
    /// node has nothing durably applied to rebuild from, so it would otherwise
    /// consider itself caught up and never replay those txns. Arming from the
    /// first available committed index makes the drain replay every committed
    /// entry for this vShard applied before subscription (idempotent: the
    /// scheduler's in-flight guard and Reserve/Release no-ops absorb re-apply).
    pub fn arm_catch_up_from(&self, vshard: u32, index: u64) {
        self.record_catch_up(vshard, index);
    }

    /// Read (WITHOUT removing) the catch-up-from Raft index for `vshard`.
    ///
    /// The scheduler drain peeks rather than takes so a replay that cannot
    /// complete this tick (committed index not yet known, transient log-read
    /// fault) leaves the entry armed for the next tick instead of silently
    /// dropping it — the loss the old take-then-early-return had.
    pub fn peek_catch_up_from(&self, vshard: u32) -> Option<u64> {
        let map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        map.get(&vshard).copied()
    }

    /// Clear `vshard`'s catch-up entry only if its recorded index is `<= up_to`.
    ///
    /// Called after a successful replay of `lo ..= up_to`: the recorded miss is
    /// now covered, so clear it — unless a concurrent drop has already lowered
    /// the entry to an index the just-finished replay did not cover (only
    /// possible for an index `<= up_to` given min-collapse, hence the guard is a
    /// belt-and-braces no-op in that case). A newer drop recorded at an index
    /// `> up_to` is preserved for the next drain.
    pub fn clear_catch_up_up_to(&self, vshard: u32, up_to: u64) {
        let mut map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(&idx) = map.get(&vshard)
            && idx <= up_to
        {
            map.remove(&vshard);
        }
    }

    /// The smallest armed catch-up index across ALL vShards, or `None` when no
    /// catch-up is pending.
    ///
    /// The sequencer-group log compactor floors its compaction index at this
    /// value so a dropped/undelivered fan-out is always replayable from the
    /// retained log — the hold-down the scheduler-side drain's `LogCompacted`
    /// arm depends on. Only hosted vShards ever arm a catch-up, so this never
    /// pins compaction on a vShard this node does not serve.
    pub fn min_catch_up_from(&self) -> Option<u64> {
        let map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        map.values().copied().min()
    }

    /// Apply a committed Raft log entry.
    ///
    /// Decodes the `SequencerEntry`, checks epoch monotonicity, fans out to
    /// per-vshard channels, and advances `last_applied_epoch`.
    ///
    /// `index` is the Raft log index of the committed entry, threaded so drop
    /// bookkeeping can record where the scheduler must catch up from and the
    /// committed-index watermark can advance.
    ///
    /// This method is synchronous (no `.await`). It MUST NOT block or do I/O.
    pub fn apply(&mut self, index: u64, data: &[u8]) {
        // Advance the committed-index watermark for EVERY committed entry, even
        // ones that fail to decode or are skipped as gaps — the entry is durably
        // committed at `index` regardless, so it is a safe replay upper bound.
        self.last_committed_index = index;

        let entry: SequencerEntry = match zerompk::from_msgpack(data) {
            Ok(e) => e,
            Err(err) => {
                error!(error = %err, "sequencer state machine: failed to decode entry; skipping");
                return;
            }
        };

        match entry {
            SequencerEntry::EpochBatch { mut batch } => {
                // Re-derive the participating_vshards field which is skipped
                // during serialization (it is computed from write_set collection names).
                for txn in &mut batch.txns {
                    txn.tx_class.restore_derived();
                }

                let expected = self.next_epoch();
                if batch.epoch != expected {
                    error!(
                        epoch = batch.epoch,
                        expected,
                        "sequencer state machine: epoch gap detected; \
                         this node may have missed entries. Skipping batch."
                    );
                    self.metrics
                        .epochs_skipped_gap
                        .fetch_add(1, Ordering::Relaxed);
                    crate::diag::sequencer_epoch_gap(
                        expected,
                        batch.epoch,
                        batch.txns.len(),
                        index,
                    );
                    // Advance anyway to the received epoch so we don't
                    // permanently stall on a gap. The scheduler will need to
                    // replay from the Raft log to recover.
                    self.last_applied_epoch = batch.epoch;
                    return;
                }

                let mut fanned_out = 0u64;
                let mut dropped = 0u64;
                // Collected for a single end-of-call diagnostics report,
                // never emitted per-txn — a sustained backpressure storm can
                // drop many positions in one apply() call and per-txn
                // emission would report-storm.
                let mut drop_pairs: Vec<(u32, &'static str)> = Vec::new();

                // Per-vShard count of how many of this epoch's positions target
                // each vShard. Delivered to each scheduler so it knows how many
                // positions of the epoch it must apply before the epoch is fully
                // applied on its vShard — the input to its per-`(epoch, position)`
                // applied gate and fully-applied watermark. Every position of an
                // epoch targeting a given vShard is stamped with the same count.
                // Shared with the replay path via `compute_vshard_txn_counts` so
                // the two paths can never drift.
                let vshard_txn_counts =
                    crate::calvin::sequencer::replay::compute_vshard_txn_counts(&batch);
                for txn in &batch.txns {
                    // Seed the expected vote-participant count deterministically on
                    // EVERY replica (not just the epoch's originating leader), so a
                    // post-failover sequencer leader can still detect vote
                    // completeness and aggregate the verdict.
                    self.completion_registry.seed_expected(
                        crate::calvin::TxnId::new(batch.epoch, txn.position),
                        txn.tx_class.participating_vshards().len(),
                    );
                }

                for txn in &batch.txns {
                    // Build a per-shard copy with epoch_system_ms stamped from
                    // the batch. This is the deterministic time anchor that engine
                    // handlers use instead of reading the wall clock themselves.
                    let mut txn_with_ts = txn.clone();
                    txn_with_ts.epoch_system_ms = batch.epoch_system_ms;

                    // Fan out only to vshards that participate in this txn.
                    let vshards = txn.tx_class.participating_vshards();
                    for vshard_id in vshards {
                        let vshard = vshard_id.as_u32();
                        if let Some(sender) = self.vshard_senders.get(&vshard) {
                            // Stamp the per-vShard position count for the vShard
                            // this copy is delivered to.
                            let mut per_vshard = txn_with_ts.clone();
                            per_vshard.epoch_vshard_txn_count =
                                vshard_txn_counts.get(&vshard).copied().unwrap_or(0);
                            match sender.try_send(SchedulerInput::Txn(per_vshard)) {
                                Ok(()) => {
                                    fanned_out += 1;
                                }
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!(
                                        epoch = batch.epoch,
                                        position = txn.position,
                                        vshard,
                                        "sequencer apply: vshard channel full (backpressure); \
                                         dropping txn. Scheduler will catch up via log replay."
                                    );
                                    self.record_catch_up(vshard, index);
                                    dropped += 1;
                                    drop_pairs.push((vshard, "full"));
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    warn!(
                                        vshard,
                                        epoch = batch.epoch,
                                        "sequencer apply: vshard sender gone; \
                                         scheduler may have exited"
                                    );
                                    self.record_catch_up(vshard, index);
                                    dropped += 1;
                                    drop_pairs.push((vshard, "closed"));
                                }
                            }
                        }
                        // If no sender registered for this vshard, silently skip —
                        // this node may not host that vshard.
                    }
                }

                if dropped > 0 {
                    crate::diag::sequencer_backpressure_drop(batch.epoch, dropped, &drop_pairs);
                }

                self.metrics
                    .txns_fanned_out
                    .fetch_add(fanned_out, Ordering::Relaxed);
                self.metrics
                    .txns_dropped_backpressure
                    .fetch_add(dropped, Ordering::Relaxed);
                self.metrics.epochs_applied.fetch_add(1, Ordering::Relaxed);
                self.last_applied_epoch = batch.epoch;
            }
            SequencerEntry::CompletionAck {
                epoch,
                position,
                vshard_id,
            } => {
                self.completion_registry
                    .note_completion_ack(crate::calvin::TxnId::new(epoch, position), vshard_id);
            }
            // Broadcast the OLLP predicate-mismatch signal to ALL replicas so the
            // coordinator's registry fires wherever it lives (including remote nodes).
            SequencerEntry::OllpMismatch { epoch, position } => {
                self.completion_registry
                    .note_ollp_mismatch(crate::calvin::TxnId::new(epoch, position));
            }
            // Broadcast the terminal routing-failure signal to ALL replicas so
            // the coordinator's registry fires wherever it lives (including
            // remote nodes), mirroring `OllpMismatch`.
            SequencerEntry::TxnRoutingFailed {
                epoch,
                position,
                detail,
            } => {
                self.completion_registry
                    .note_routing_failed(crate::calvin::TxnId::new(epoch, position), detail);
            }
            // Durable per-participant commit vote for a staged cross-shard txn.
            // The registry tallies votes per vshard; once every participant has
            // voted the leader aggregates them into the global verdict that gates
            // the cross-shard commit barrier (flush on commit, drop on abort).
            SequencerEntry::Vote {
                epoch,
                position,
                vshard,
                commit,
            } => {
                self.completion_registry.note_vote(
                    crate::calvin::TxnId::new(epoch, position),
                    vshard,
                    commit,
                );
            }
            // Authoritative commit/abort verdict for a staged cross-shard txn,
            // proposed by the leader once every participant voted. Applied on
            // ALL replicas to store the durable decision, which releases every
            // participant parked at the cross-shard commit barrier into its
            // flush (commit) or drop (abort).
            SequencerEntry::Verdict {
                epoch,
                position,
                commit,
            } => {
                self.completion_registry
                    .note_verdict(crate::calvin::TxnId::new(epoch, position), commit);
            }
            // Fan a hot-key read reservation out to its owning vShard's scheduler,
            // which installs the SHARED lock. Same `try_send` backpressure
            // discipline as the epoch-batch fan-out: a full/closed channel logs
            // and drops (this node may not host the vShard, in which case there is
            // simply no sender registered).
            SequencerEntry::ReserveRead { owner, vshard, key } => {
                if let Some(sender) = self.vshard_senders.get(&vshard) {
                    match sender.try_send(SchedulerInput::Reserve { owner, key }) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                vshard,
                                owner_epoch = owner.epoch,
                                owner_position = owner.position,
                                "sequencer apply: vshard channel full (backpressure); \
                                 dropping read reservation"
                            );
                            self.record_catch_up(vshard, index);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            warn!(
                                vshard,
                                "sequencer apply: vshard sender gone; \
                                 scheduler may have exited (reservation)"
                            );
                            self.record_catch_up(vshard, index);
                        }
                    }
                }
            }
            // Fan a reservation release out to its owning vShard's scheduler.
            // Same `try_send` discipline as `ReserveRead`.
            SequencerEntry::ReleaseReservation {
                owner,
                vshard,
                reason,
            } => {
                if let Some(sender) = self.vshard_senders.get(&vshard) {
                    match sender.try_send(SchedulerInput::Release { owner, reason }) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                vshard,
                                owner_epoch = owner.epoch,
                                owner_position = owner.position,
                                "sequencer apply: vshard channel full (backpressure); \
                                 dropping reservation release"
                            );
                            self.record_catch_up(vshard, index);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            warn!(
                                vshard,
                                "sequencer apply: vshard sender gone; \
                                 scheduler may have exited (reservation release)"
                            );
                            self.record_catch_up(vshard, index);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::types::{
        EngineKeySet, EpochBatch, ReadWriteSet, SequencedTxn, SortedVec, TxClass,
    };
    use nodedb_types::{
        TenantId,
        id::{DatabaseId, VShardId},
    };

    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != vshard {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, vshard));
            }
        }
        panic!("could not find two distinct-vshard collections in 512 tries");
    }

    fn make_tx_class_for_vshards(vshard_a: u32, vshard_b: u32) -> (TxClass, u32, u32) {
        // Find collections that map to the given vshards.
        // Since we can't control the hash, we use the known pattern from the type:
        // participating_vshards() is derived from collection names.
        // We'll use find_two_distinct_collections and use whatever vshards they hash to.
        let (col_a, col_b) = find_two_distinct_collections();
        let _ = (vshard_a, vshard_b); // actual vshard ids come from the collection hash
        let real_va = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_a).as_u32();
        let real_vb = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_b).as_u32();
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a,
                surrogates: SortedVec::new(vec![1]),
            },
            EngineKeySet::Document {
                collection: col_b,
                surrogates: SortedVec::new(vec![2]),
            },
        ]);
        let tx_class = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(1),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        (tx_class, real_va, real_vb)
    }

    fn make_batch_with_two_vshards() -> (EpochBatch, u32, u32) {
        let (tx_class, va, vb) = make_tx_class_for_vshards(0, 1);
        let batch = EpochBatch {
            epoch: 0,
            txns: vec![SequencedTxn {
                epoch: 0,
                position: 0,
                tx_class,
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 1,
                lock_owner: None,
            }],
            epoch_system_ms: 1_700_000_000_000,
        };
        (batch, va, vb)
    }

    fn encode_entry(entry: &SequencerEntry) -> Vec<u8> {
        zerompk::to_msgpack_vec(entry).expect("encode")
    }

    #[test]
    fn apply_on_fresh_state_increments_last_applied_epoch() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, _) = mpsc::channel(64);
        let (tx_b, _) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.last_applied_epoch(), None);

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(1, &data);

        assert_eq!(sm.last_applied_epoch(), Some(0));
        assert_eq!(sm.metrics.epochs_applied.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn gap_detection_rejects_out_of_order_epochs() {
        let (mut batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, _) = mpsc::channel(64);
        let (tx_b, _) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        // Apply epoch 0.
        let data0 = encode_entry(&SequencerEntry::EpochBatch {
            batch: batch.clone(),
        });
        sm.apply(1, &data0);
        assert_eq!(sm.last_applied_epoch(), Some(0));

        // Apply epoch 2 (skip epoch 1 → gap).
        batch.epoch = 2;
        for txn in &mut batch.txns {
            txn.epoch = 2;
        }
        let data2 = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(2, &data2);

        assert_eq!(sm.metrics.epochs_skipped_gap.load(Ordering::Relaxed), 1);
        // Epoch advances to 2 to avoid permanent stall.
        assert_eq!(sm.last_applied_epoch(), Some(2));
    }

    #[test]
    fn per_vshard_fanout_sends_only_to_participating_vshards() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, mut rx_a) = mpsc::channel(64);
        let (tx_b, mut rx_b) = mpsc::channel(64);
        // A third vshard with no txns.
        let (tx_c, mut rx_c) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        senders.insert(999, tx_c);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(1, &data);

        // Both participating vshards should have received the txn.
        assert!(rx_a.try_recv().is_ok(), "vshard A should have received txn");
        assert!(rx_b.try_recv().is_ok(), "vshard B should have received txn");
        // The unrelated vshard should be empty.
        assert!(
            rx_c.try_recv().is_err(),
            "vshard C should not have received txn"
        );
    }

    #[test]
    fn try_send_on_full_channel_logs_and_drops_without_blocking() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        // Capacity 0 is not allowed; use capacity 1 and fill it first.
        let (tx_a, _rx_a) = mpsc::channel(1);
        let (tx_b, _rx_b) = mpsc::channel(1);
        // Pre-fill channel A so it is full.
        let pre_fill: SequencedTxn = batch.txns[0].clone();
        let _ = tx_a.try_send(SchedulerInput::Txn(pre_fill));
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        // Must not panic or block.
        sm.apply(1, &data);

        // At least one drop was recorded (vshard A was full).
        assert!(sm.metrics.txns_dropped_backpressure.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn next_epoch_is_zero_on_fresh_state_machine() {
        let sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.next_epoch(), 0);
    }

    #[test]
    fn next_epoch_increments_after_apply() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, _) = mpsc::channel(64);
        let (tx_b, _) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(1, &data);

        assert_eq!(sm.next_epoch(), 1);
    }

    #[tokio::test]
    async fn apply_txn_routing_failed_dispatches_to_completion_registry() {
        let registry = CalvinCompletionRegistry::new_detached();
        let mut sm = SequencerStateMachine::new(HashMap::new(), Arc::clone(&registry));

        let data = encode_entry(&SequencerEntry::TxnRoutingFailed {
            epoch: 5,
            position: 2,
            detail: "unroutable plan".to_owned(),
        });
        sm.apply(1, &data);

        // The registry's waiter (registered AFTER apply) must still observe
        // the failure — `note_routing_failed` persists it on the entry.
        let rx = registry.register_completion(crate::calvin::TxnId::new(5, 2), 1);
        let outcome = rx.await.expect("routing failure fires");
        assert_eq!(
            outcome,
            crate::calvin::AttemptOutcome::Failed {
                detail: "unroutable plan".to_owned()
            }
        );
        // TxnRoutingFailed is not an EpochBatch, so it must not perturb the
        // epoch counter (mirrors OllpMismatch's non-effect on last_applied_epoch).
        assert_eq!(sm.last_applied_epoch(), None);
    }

    #[tokio::test]
    async fn apply_verdict_stores_decision_without_perturbing_epoch() {
        let registry = CalvinCompletionRegistry::new_detached();
        let mut sm = SequencerStateMachine::new(HashMap::new(), Arc::clone(&registry));
        let txn = crate::calvin::TxnId::new(9, 4);

        let data = encode_entry(&SequencerEntry::Verdict {
            epoch: 9,
            position: 4,
            commit: true,
        });
        sm.apply(1, &data);

        // The verdict is stored authoritatively on every replica.
        assert_eq!(registry.verdict(txn), Some(true));
        // Verdict is not an EpochBatch, so it must not perturb the epoch counter
        // (mirrors OllpMismatch/TxnRoutingFailed's non-effect).
        assert_eq!(sm.last_applied_epoch(), None);
    }

    #[test]
    fn catch_up_from_records_dropped_index_and_min_collapses() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        // Capacity 1, pre-filled → vshard A is full and every fan-out drops.
        let (tx_a, _rx_a) = mpsc::channel(1);
        // vshard B has room and a live receiver → never drops.
        let (tx_b, _rx_b) = mpsc::channel(64);
        let _ = tx_a.try_send(SchedulerInput::Txn(batch.txns[0].clone()));
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        // First drop for vshard A at a HIGH Raft index.
        let data0 = encode_entry(&SequencerEntry::EpochBatch {
            batch: batch.clone(),
        });
        sm.apply(10, &data0);

        // Second drop for the SAME vshard at a LOWER Raft index → min-collapse.
        let mut batch1 = batch.clone();
        batch1.epoch = 1;
        for txn in &mut batch1.txns {
            txn.epoch = 1;
        }
        let data1 = encode_entry(&SequencerEntry::EpochBatch { batch: batch1 });
        sm.apply(4, &data1);

        // The recorded catch-up index is the SMALLEST dropped index (4), and the
        // repeated drops for one vShard did not grow the map (a single entry that
        // min-collapsed). vshard B never dropped, so it has no entry.
        assert_eq!(sm.take_catch_up_from(vb), None);
        assert_eq!(sm.take_catch_up_from(va), Some(4));
        // TAKE semantics: the entry is cleared, so a second take returns None.
        assert_eq!(sm.take_catch_up_from(va), None);
    }

    /// PEEK must not consume: the scheduler drain reads the armed index, and
    /// only clears it after a confirmed replay. A take-then-early-return (the
    /// old shape) silently lost the miss when the replay could not complete.
    #[test]
    fn peek_catch_up_from_does_not_consume() {
        let (batch, va, _vb) = make_batch_with_two_vshards();
        let (tx_a, _rx_a) = mpsc::channel(1);
        let _ = tx_a.try_send(SchedulerInput::Txn(batch.txns[0].clone()));
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        sm.apply(9, &encode_entry(&SequencerEntry::EpochBatch { batch }));

        // Repeated peeks keep returning the same armed index.
        assert_eq!(sm.peek_catch_up_from(va), Some(9));
        assert_eq!(sm.peek_catch_up_from(va), Some(9));
    }

    /// Clearing is bounded by the replayed upper bound: a miss covered by the
    /// replay is cleared, one recorded ABOVE it survives for the next drain.
    #[test]
    fn clear_catch_up_up_to_respects_replayed_upper_bound() {
        let senders = HashMap::new();
        let sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        let v = 42u32;

        // Armed at 5, replay covered through 10 → cleared.
        sm.arm_catch_up_from(v, 5);
        sm.clear_catch_up_up_to(v, 10);
        assert_eq!(sm.peek_catch_up_from(v), None);

        // Armed at 20, replay only covered through 10 → still armed.
        sm.arm_catch_up_from(v, 20);
        sm.clear_catch_up_up_to(v, 10);
        assert_eq!(sm.peek_catch_up_from(v), Some(20));
    }

    /// The sequencer-log compaction hold-down floors on the LOWEST armed index
    /// across all vShards, so no replica's replay range is compacted away.
    #[test]
    fn min_catch_up_from_is_lowest_armed_index_across_vshards() {
        let senders = HashMap::new();
        let sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.min_catch_up_from(), None);

        sm.arm_catch_up_from(1, 30);
        sm.arm_catch_up_from(2, 12);
        sm.arm_catch_up_from(3, 25);
        assert_eq!(sm.min_catch_up_from(), Some(12));

        // Draining the lowest lifts the floor to the next outstanding miss.
        sm.clear_catch_up_up_to(2, 12);
        assert_eq!(sm.min_catch_up_from(), Some(25));

        sm.clear_catch_up_up_to(1, 30);
        sm.clear_catch_up_up_to(3, 25);
        assert_eq!(sm.min_catch_up_from(), None);
    }

    #[test]
    fn catch_up_from_records_dropped_index_on_closed_channel() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, rx_a) = mpsc::channel(64);
        let (tx_b, _rx_b) = mpsc::channel(64);
        // Close vshard A's receiver → the sender reports Closed on try_send.
        drop(rx_a);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(7, &data);

        // The Closed drop is recorded at the entry's index for the closed vShard.
        assert_eq!(sm.take_catch_up_from(va), Some(7));
        assert_eq!(sm.take_catch_up_from(vb), None);
    }

    #[test]
    fn current_committed_index_advances_for_every_applied_entry() {
        let mut sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.current_committed_index(), None);

        // A non-EpochBatch entry still advances the committed-index watermark.
        let data = encode_entry(&SequencerEntry::Verdict {
            epoch: 1,
            position: 0,
            commit: true,
        });
        sm.apply(42, &data);
        assert_eq!(sm.current_committed_index(), Some(42));
    }
}
