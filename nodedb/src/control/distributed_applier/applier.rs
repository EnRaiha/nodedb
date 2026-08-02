// SPDX-License-Identifier: BUSL-1.1

//! `DistributedApplier` — `CommitApplier` impl that queues committed Raft
//! entries onto a bounded mpsc channel for the background apply loop.
//!
//! Raft re-delivers committed entries: each tick collects from
//! `last_applied + 1`, and `last_applied` only moves after the applier returns,
//! so any entry proposed while a batch is still in flight re-collects the whole
//! in-flight prefix. The applier is therefore the point that must be idempotent
//! per Raft log index — the same rule the metadata cache applies to group 0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tracing::{debug, warn};

use nodedb_cluster::raft_loop::CommitApplier;
use nodedb_raft::message::LogEntry;

use super::propose_tracker::ProposeTracker;

/// Queued entry for the background apply loop.
pub struct ApplyBatch {
    pub(crate) group_id: u64,
    pub(crate) entries: Vec<LogEntry>,
}

/// CommitApplier that queues committed entries for async Data Plane execution.
///
/// Uses a bounded tokio mpsc channel: `apply_committed()` is called from the
/// sync Raft tick loop and pushes non-blockingly. A background async task
/// reads from the channel, dispatches each write to the Data Plane, and
/// notifies any waiting proposers.
pub struct DistributedApplier {
    apply_tx: mpsc::Sender<ApplyBatch>,
    tracker: Arc<ProposeTracker>,
    /// Per-group highest Raft log index this applier has already HANDED OFF —
    /// either completed in place (a leader-change no-op) or accepted onto the
    /// apply channel. Entries at or below it are re-deliveries and are dropped.
    ///
    /// The watermark advances at hand-off, not at hand-back, because the apply
    /// channel is single-consumer: once an entry is on it, `run_apply_loop`
    /// owns it and will apply it exactly once. It deliberately does NOT advance
    /// for entries a full channel rejected — those are genuinely undelivered
    /// and must be re-collected on the next tick.
    delivered: Mutex<HashMap<u64, u64>>,
}

impl DistributedApplier {
    pub fn new(apply_tx: mpsc::Sender<ApplyBatch>, tracker: Arc<ProposeTracker>) -> Self {
        Self {
            apply_tx,
            tracker,
            delivered: Mutex::new(HashMap::new()),
        }
    }

    /// Access the tracker (for registering propose waiters).
    pub fn tracker(&self) -> &Arc<ProposeTracker> {
        &self.tracker
    }

    /// Highest already-handed-off index for `group_id`, or 0 when this applier
    /// has seen nothing for the group yet (Raft indices are 1-based, so 0 never
    /// suppresses a real entry).
    fn delivered_index(&self, group_id: u64) -> u64 {
        self.delivered
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&group_id)
            .copied()
            .unwrap_or(0)
    }

    /// Move `group_id`'s hand-off watermark to `index`. Monotonic — a batch
    /// that only partially made it through must never pull the watermark back
    /// over entries an earlier batch already delivered.
    fn advance_delivered(&self, group_id: u64, index: u64) {
        let mut delivered = self.delivered.lock().unwrap_or_else(|p| p.into_inner());
        let slot = delivered.entry(group_id).or_insert(0);
        if index > *slot {
            *slot = index;
        }
    }
}

impl CommitApplier for DistributedApplier {
    fn apply_committed(&self, group_id: u64, entries: &[LogEntry]) -> u64 {
        let last_index = entries.last().map(|e| e.index).unwrap_or(0);

        // Drop the re-delivered prefix. The gate is the Raft log INDEX and
        // nothing else: a committed index is final (committed entries are never
        // truncated or rewritten), so "index already handed off" is exactly
        // "already applied" and can never suppress a genuinely new entry.
        //
        // Without it every re-delivery re-runs the whole batch: an append-shaped
        // write (spatial/columnar/timeseries insert, predicated update) lands
        // twice in the engine AND mints a second WAL redo record, AFTER triggers
        // and DML audit fire twice, and `ProposeTracker::complete` runs a second
        // time on an index whose waiter was already resolved and removed —
        // parking an orphan `Completed` slot that nothing ever reaps.
        let watermark = self.delivered_index(group_id);
        let fresh: Vec<&LogEntry> = entries.iter().filter(|e| e.index > watermark).collect();
        if fresh.is_empty() {
            debug!(
                group_id,
                watermark, last_index, "skipping fully re-delivered committed batch"
            );
            return last_index;
        }
        let fresh_last = fresh.last().map(|e| e.index).unwrap_or(last_index);

        // Empty entries are Raft leader-transition no-ops, not user
        // proposals. A waiter registered at (group_id, idx) was
        // proposed by a previous leader at index `idx`; when that
        // leader stepped down before the entry committed, the new
        // leader's election no-op commits at the same index and
        // overwrites it. The proposer's data is GONE — silently
        // firing `tracker.complete(Ok([]))` here would tell the
        // proposer their INSERT succeeded when in fact it was
        // truncated, producing the classic "simple_query returned
        // Ok but the row never appears" silent data-loss bug.
        //
        // Surface the truncation as an explicit error so the gateway
        // / caller can retry. Idempotent re-propose is safe because
        // the encoded payload carries enough identity (collection,
        // PK, surrogate) for the apply path to be replayable.
        for entry in &fresh {
            if entry.data.is_empty() {
                tracing::error!(
                    group_id,
                    log_index = entry.index,
                    "leader-change no-op committed at index where a proposer was waiting; \
                     surfacing RetryableLeaderChange so the gateway re-proposes"
                );
                // applied_key = 0 (no entry payload to derive a key
                // from). The slot fires the explicit
                // `RetryableLeaderChange` carried in `result`.
                self.tracker.complete(
                    group_id,
                    entry.index,
                    0,
                    Err(crate::Error::RetryableLeaderChange {
                        group_id,
                        log_index: entry.index,
                    }),
                );
            }
        }

        let real_entries: Vec<LogEntry> = fresh
            .iter()
            .filter(|e| !e.data.is_empty())
            .map(|e| (*e).clone())
            .collect();

        let Some(first_real_index) = real_entries.first().map(|e| e.index) else {
            // Nothing but no-ops, and they are now fully handled.
            self.advance_delivered(group_id, fresh_last);
            return last_index;
        };

        // Push to background task. If the channel is full, log a warning
        // but don't block the tick loop.
        if let Err(e) = self.apply_tx.try_send(ApplyBatch {
            group_id,
            entries: real_entries,
        }) {
            warn!(group_id, error = %e, "apply queue full, entries will be retried on next tick");
            // The rejected entries stay undelivered, so the watermark may only
            // cover the no-ops strictly BELOW the first of them — those were
            // completed above and must not fire a second time. Everything from
            // `first_real_index` up is re-collected on the next tick.
            self.advance_delivered(group_id, first_real_index.saturating_sub(1));
            // Don't advance applied index — entries will be re-delivered.
            return 0;
        }

        self.advance_delivered(group_id, fresh_last);
        last_index
    }
}

/// Create a DistributedApplier and the channel for the background apply loop.
///
/// Returns (applier, receiver). Spawn `run_apply_loop` with the receiver.
pub fn create_distributed_applier(
    tracker: Arc<ProposeTracker>,
) -> (DistributedApplier, mpsc::Receiver<ApplyBatch>) {
    let (tx, rx) = mpsc::channel(1024);
    let applier = DistributedApplier::new(tx, tracker);
    (applier, rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, data: &[u8]) -> LogEntry {
        LogEntry {
            term: 1,
            index,
            data: data.to_vec(),
        }
    }

    #[test]
    fn redelivered_batch_is_not_queued_twice() {
        let (applier, mut rx) = create_distributed_applier(Arc::new(ProposeTracker::new()));
        let entries = vec![entry(1, b"a"), entry(2, b"b")];

        assert_eq!(applier.apply_committed(7, &entries), 2);
        let batch = rx.try_recv().expect("first delivery queued");
        assert_eq!(batch.entries.len(), 2);

        // Raft re-collects from `last_applied + 1` while the batch is still in
        // flight, so the identical prefix arrives again.
        assert_eq!(applier.apply_committed(7, &entries), 2);
        assert!(
            rx.try_recv().is_err(),
            "re-delivered entries must not reach the apply loop a second time"
        );
    }

    #[test]
    fn new_index_after_a_redelivered_prefix_still_applies() {
        let (applier, mut rx) = create_distributed_applier(Arc::new(ProposeTracker::new()));
        let entries = vec![entry(1, b"a"), entry(2, b"b"), entry(3, b"c")];

        applier.apply_committed(7, &entries[..2]);
        rx.try_recv().expect("first delivery queued");

        assert_eq!(applier.apply_committed(7, &entries), 3);
        let batch = rx.try_recv().expect("the new entry must be queued");
        let indexes: Vec<u64> = batch.entries.iter().map(|e| e.index).collect();
        assert_eq!(indexes, vec![3]);
    }

    #[test]
    fn watermark_is_per_group() {
        let (applier, mut rx) = create_distributed_applier(Arc::new(ProposeTracker::new()));
        let entries = vec![entry(1, b"a")];

        applier.apply_committed(7, &entries);
        rx.try_recv().expect("group 7 queued");

        // Index 1 of a different group is a different entry entirely.
        applier.apply_committed(8, &entries);
        let batch = rx.try_recv().expect("group 8 queued");
        assert_eq!(batch.group_id, 8);
    }

    #[test]
    fn redelivered_leader_change_noop_does_not_resolve_a_waiter_twice() {
        let tracker = Arc::new(ProposeTracker::new());
        let (applier, _rx) = create_distributed_applier(tracker.clone());
        let noop = vec![entry(1, b"")];

        let mut waiter = tracker.register(7, 1, 0);
        applier.apply_committed(7, &noop);
        assert!(matches!(
            waiter.try_recv(),
            Ok(Err(crate::Error::RetryableLeaderChange { .. }))
        ));

        applier.apply_committed(7, &noop);
        let mut probe = tracker.register(7, 1, 0);
        assert!(
            probe.try_recv().is_err(),
            "a second completion would park an orphan result on an index whose \
             waiter is already gone"
        );
    }

    #[test]
    fn queue_full_keeps_the_rejected_entries_replayable() {
        let (tx, mut rx) = mpsc::channel(1);
        let applier = DistributedApplier::new(tx, Arc::new(ProposeTracker::new()));

        applier.apply_committed(7, &[entry(1, b"a")]);
        assert_eq!(
            applier.apply_committed(7, &[entry(2, b"b")]),
            0,
            "a full queue must not advance raft's applied index"
        );

        rx.try_recv().expect("first batch queued");
        assert_eq!(applier.apply_committed(7, &[entry(2, b"b")]), 2);
        let batch = rx
            .try_recv()
            .expect("the rejected entry must be re-accepted");
        assert_eq!(batch.entries[0].index, 2);
    }

    #[test]
    fn noops_below_a_rejected_entry_are_not_completed_twice() {
        let tracker = Arc::new(ProposeTracker::new());
        let (tx, mut rx) = mpsc::channel(1);
        let applier = DistributedApplier::new(tx, tracker.clone());
        let entries = vec![entry(2, b""), entry(3, b"x")];

        applier.apply_committed(7, &[entry(1, b"a")]);
        let mut waiter = tracker.register(7, 2, 0);
        assert_eq!(applier.apply_committed(7, &entries), 0);
        assert!(matches!(
            waiter.try_recv(),
            Ok(Err(crate::Error::RetryableLeaderChange { .. }))
        ));

        rx.try_recv().expect("first batch queued");
        assert_eq!(applier.apply_committed(7, &entries), 3);

        let mut probe = tracker.register(7, 2, 0);
        assert!(
            probe.try_recv().is_err(),
            "the no-op below the rejected entry was already handled"
        );
        let batch = rx
            .try_recv()
            .expect("the rejected entry must be re-accepted");
        let indexes: Vec<u64> = batch.entries.iter().map(|e| e.index).collect();
        assert_eq!(indexes, vec![3]);
    }
}
