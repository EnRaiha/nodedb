// SPDX-License-Identifier: BUSL-1.1

//! The sequencer state machine's state and bookkeeping accessors.
//!
//! The apply path itself lives in [`super::apply`]; this file owns the struct,
//! its construction, and the read/arm/clear helpers the scheduler-side catch-up
//! drain and the sequencer-group log compactor call into.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::calvin::CalvinCompletionRegistry;
use crate::calvin::sequencer::epoch_guard::UnrecoverableEpochHook;
use crate::calvin::types::SchedulerInput;

use super::counters::StateMachineMetrics;

/// The Calvin sequencer Raft state machine.
///
/// One instance per replica (including leader). Applied on every `CommitApplier`
/// callback for the sequencer Raft group.
pub struct SequencerStateMachine {
    /// Last successfully applied epoch. Used for gap detection.
    /// The first valid epoch is 0; `last_applied_epoch = u64::MAX` means nothing
    /// has been applied yet (using `u64::MAX` avoids a separate `Option` and
    /// makes the "nothing applied" state explicit).
    pub(super) last_applied_epoch: u64,
    /// Raft log index of the last committed entry applied on this replica.
    /// `NOT_YET_APPLIED` means nothing has been applied yet. Advanced for EVERY
    /// applied entry (not just `EpochBatch`), so it is a safe upper bound for the
    /// scheduler's catch-up `read_committed_entries(lo, hi)` range.
    pub(super) last_committed_index: u64,
    /// Per-vshard output channels. The scheduler subscribes on the other end.
    pub(super) vshard_senders: HashMap<u32, mpsc::Sender<SchedulerInput>>,
    /// Per-vShard "catch up from this Raft index" bookkeeping.
    ///
    /// When a fan-out `try_send` to a vShard's scheduler channel fails (Full or
    /// Closed), the input for that vShard was dropped. The current entry's Raft
    /// index is recorded here with MIN-COLLAPSE (the smallest dropped index per
    /// vShard wins), so the scheduler-side drain replays the sequencer Raft log
    /// from the earliest miss forward. Bounded by the number of hosted vShards —
    /// a vShard contributes at most one entry until its catch-up is drained.
    pub(super) catch_up_from: Mutex<HashMap<u32, u64>>,
    /// Set once an already-consumed epoch was proposed again. While set, no
    /// further `EpochBatch` is applied: this replica's epoch sequence and the
    /// proposing leader's have diverged, and fanning out under a colliding
    /// identity would corrupt lock-table and completion state rather than
    /// merely lose the offending batch.
    pub(super) halted: bool,
    /// Host escalation for the halt above. `None` in tests and in embedded
    /// callers with no fail-stop path; production wires it to node shutdown.
    pub(super) unrecoverable_hook: Option<UnrecoverableEpochHook>,
    pub metrics: Arc<StateMachineMetrics>,
    pub(super) completion_registry: Arc<CalvinCompletionRegistry>,
}

pub(super) const NOT_YET_APPLIED: u64 = u64::MAX;

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
            halted: false,
            unrecoverable_hook: None,
            metrics: StateMachineMetrics::new(),
            completion_registry,
        }
    }

    /// Install the host's fail-stop escalation for an unrecoverable epoch
    /// regression.
    ///
    /// Without it the halt is still enforced locally and reported, but the node
    /// keeps serving with a sequencer that no longer accepts epochs — so a host
    /// that has a shutdown path SHOULD install one, and turn the halt into a
    /// visible stop rather than a silent stall.
    #[must_use]
    pub fn with_unrecoverable_hook(mut self, hook: UnrecoverableEpochHook) -> Self {
        self.unrecoverable_hook = Some(hook);
        self
    }

    /// Whether this state machine has halted on an unrecoverable epoch
    /// regression and is refusing to apply further epoch batches.
    pub fn is_halted(&self) -> bool {
        self.halted
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
    ///
    /// INVARIANT: the first epoch a restarted leader proposes must be strictly
    /// greater than any epoch already committed to the sequencer log. This
    /// counter satisfies that ONLY once every committed entry has been applied
    /// here — it is in-memory and rebuilt purely by replaying the group's log,
    /// so reading it before that replay finishes answers 0 no matter how much
    /// history the log holds. Callers seeding a proposer must gate on the
    /// group's applied watermark reaching its log tip first (see
    /// `SequencerService::ensure_epoch_seeded`); an epoch minted early collides
    /// with committed history and every replica refuses the batch.
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
    pub(super) fn record_catch_up(&self, vshard: u32, index: u64) {
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
}
