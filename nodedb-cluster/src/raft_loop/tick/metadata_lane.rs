// SPDX-License-Identifier: BUSL-1.1

//! Metadata apply lane: group 0's committed entries applied in log order,
//! off the tick.
//!
//! The metadata applier awaits host effects, a Data-Plane dispatch among
//! them. Awaiting it inside the tick would stall heartbeats and elections of
//! every group on this node for as long as one apply takes. The tick hands
//! each committed batch to this lane through a bounded channel instead, and
//! moves on. A batch the full channel refuses goes back to `Ready`.
//!
//! The lane runs in the loop's own task, beside the tick loop, and ends with
//! it. For each run it:
//! 1. takes group 0's apply gate, so a snapshot install never interleaves;
//! 2. drops the entries at or below `last_applied`, which a snapshot covers;
//! 3. awaits the applier, which adopts epochs in log order and saves the
//!    durable applied floor before it returns (see [`super::metadata_apply`]);
//! 4. advances Raft's `last_applied` and the group's apply watcher to the
//!    index the applier reached.
//!
//! An entry the applier stops at stays in the lane, with every entry after
//! it, and the lane retries it after [`RETRY_TICKS`] ticks. Nothing past it
//! applies first. Log compaction reads the durable floor, which never passes
//! an applied entry.

use std::collections::VecDeque;
use std::time::Duration;

use nodedb_raft::LogEntry;
use tokio::sync::{mpsc, watch};

use crate::forward::PlanExecutor;
use crate::metadata_group::METADATA_GROUP_ID;
use crate::raft_loop::apply_gate::ApplyPermit;

use super::super::loop_core::{CommitApplier, RaftLoop};

/// Batches the lane holds before the tick queues further entries back into
/// `Ready`.
pub(in crate::raft_loop) const METADATA_LANE_DEPTH: usize = 64;

/// Ticks the lane waits before it retries an entry the applier stopped at.
const RETRY_TICKS: u32 = 10;

/// Append the entries of `batch` above the last queued index.
fn enqueue(pending: &mut VecDeque<LogEntry>, batch: Vec<LogEntry>) {
    let through = pending.back().map_or(0, |entry| entry.index);
    pending.extend(batch.into_iter().filter(|entry| entry.index > through));
}

/// Wait `wait`, or less when shutdown begins. Returns whether it began.
async fn wait_or_shutdown(shutdown: &mut watch::Receiver<bool>, wait: Duration) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(wait) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Apply group 0's batches from `rx` in log order until the tick loop
    /// closes the lane or shutdown begins.
    pub(in crate::raft_loop) async fn run_metadata_lane(
        &self,
        mut rx: mpsc::Receiver<Vec<LogEntry>>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut pending: VecDeque<LogEntry> = VecDeque::new();
        loop {
            if pending.is_empty() {
                match rx.recv().await {
                    Some(batch) => enqueue(&mut pending, batch),
                    None => return,
                }
            }
            while let Ok(batch) = rx.try_recv() {
                enqueue(&mut pending, batch);
            }
            // A node shutting down applies nothing more. Raft delivers the
            // entries again above the durable floor on the next boot.
            if *shutdown.borrow() {
                return;
            }
            let Some(permit) = self.metadata_apply_permit(&mut shutdown).await else {
                return;
            };
            let applied = self
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .last_applied(METADATA_GROUP_ID)
                .unwrap_or(0);
            pending.retain(|entry| entry.index > applied);
            let Some(through) = pending.back().map(|entry| entry.index) else {
                continue;
            };
            let batch: Vec<LogEntry> = pending.iter().cloned().collect();
            let delivered = self.apply_metadata_commits(&batch).await;
            drop(permit);
            self.finish_metadata_apply(delivered);
            pending.retain(|entry| entry.index > delivered);
            if delivered < through
                && wait_or_shutdown(&mut shutdown, self.tick_interval * RETRY_TICKS).await
            {
                return;
            }
        }
    }

    /// Group 0's apply gate, once no snapshot install holds it. `None` when
    /// shutdown begins first.
    async fn metadata_apply_permit(
        &self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Option<ApplyPermit> {
        let gates = self
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .apply_gates();
        loop {
            if let Some(permit) = gates.try_apply(METADATA_GROUP_ID) {
                return Some(permit);
            }
            if wait_or_shutdown(shutdown, self.tick_interval).await {
                return None;
            }
        }
    }

    /// Report an apply that reached `delivered` back to Raft and to the
    /// group's watcher, and open the boot-time readiness watch.
    ///
    /// The first apply of group 0 on this node, the election no-op or a
    /// replayed entry, flips the ready watch. The host awaits it before it
    /// binds client-facing listeners.
    fn finish_metadata_apply(&self, delivered: u64) {
        if delivered > 0 {
            let advanced = self
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .advance_applied(METADATA_GROUP_ID, delivered);
            match advanced {
                Ok(()) => self.group_watchers.bump(METADATA_GROUP_ID, delivered),
                Err(e) => tracing::warn!(
                    error = %e,
                    delivered,
                    "metadata lane: failed to advance the applied index"
                ),
            }
        }
        if !*self.ready_watch.borrow() {
            let _ = self.ready_watch.send(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64) -> LogEntry {
        LogEntry {
            term: 1,
            index,
            data: Vec::new(),
        }
    }

    #[test]
    fn a_repeated_range_is_queued_once() {
        let mut pending = VecDeque::new();
        enqueue(&mut pending, vec![entry(1), entry(2)]);
        enqueue(&mut pending, vec![entry(2), entry(3)]);
        let indices: Vec<u64> = pending.iter().map(|e| e.index).collect();
        assert_eq!(indices, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn a_closed_shutdown_channel_ends_the_wait() {
        let (tx, mut rx) = watch::channel(false);
        drop(tx);
        assert!(wait_or_shutdown(&mut rx, Duration::from_secs(60)).await);
    }
}
