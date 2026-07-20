// SPDX-License-Identifier: BUSL-1.1

//! Sequencer-fan-out catch-up drain for the Calvin scheduler.
//!
//! The sequencer state machine fans each committed `SchedulerInput` out to the
//! per-vShard scheduler channels with a bounded `try_send`. On a Full/Closed
//! channel the input is DROPPED (only bookkept, never blocking — `apply` shares
//! its call stack with every Raft group and must not stall node-wide
//! heartbeats). A dropped input would otherwise permanently diverge this
//! replica's lock table from its peers, since the lock table is a local
//! projection every replica rebuilds from the byte-identical sequencer Raft log.
//!
//! [`Scheduler::drain_catch_up`] closes that gap: it takes the earliest dropped
//! Raft index recorded for this vShard, replays the committed sequencer log
//! range through the SAME `process_scheduler_input` path the live fan-out feeds,
//! and thereby reconstructs the missed input deterministically. Replay is
//! idempotent — `process_new_txn`'s in-flight guard turns an already-in-flight
//! Txn into a no-op, and Reserve/Release re-application is a lock-manager no-op.

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;

use super::scheduler::Scheduler;

impl Scheduler {
    /// Replay any sequencer-fan-out inputs dropped on this replica.
    ///
    /// Run on the periodic stall tick. O(1) in the common case (no pending
    /// catch-up → one map probe and return).
    ///
    /// # Lock discipline (deadlock-safety)
    ///
    /// The two shared mutexes — the sequencer state machine and MultiRaft — are
    /// each acquired in an ISOLATED scope and NEVER nested. The Raft apply loop
    /// holds the SM lock while fanning out but never takes MultiRaft underneath
    /// it; this drain takes them strictly one-at-a-time (SM → release → MultiRaft
    /// → release → SM → release), so the two paths can never form a lock cycle.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn drain_catch_up(&mut self) {
        // 1. SM-lock scope: PEEK the earliest armed index for this vShard.
        //    `None` (the common case) means no catch-up is pending — return O(1).
        //    Otherwise pair it with the committed-index watermark as the replay
        //    upper bound. We PEEK rather than TAKE: the entry is cleared only
        //    after a confirmed replay (step 4), so a tick that cannot complete
        //    the replay (committed index not yet known, transient log-read
        //    fault) leaves the catch-up armed for the next tick instead of
        //    silently dropping it. Release the SM lock before the MultiRaft read.
        let (lo, hi) = {
            let sm = self
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let Some(lo) = sm.peek_catch_up_from(self.vshard_id) else {
                return;
            };
            let Some(hi) = sm.current_committed_index() else {
                // Armed but nothing applied yet — leave it armed and retry once
                // an entry is applied and `hi` is known.
                return;
            };
            if lo > hi {
                // Armed ahead of the committed watermark (e.g. spawn-armed from
                // the first available index before any entry applied on this
                // replica). Nothing to replay yet; stay armed.
                return;
            }
            (lo, hi)
        };

        // 2. MultiRaft-lock scope: read the committed sequencer log range. No SM
        //    lock is held here (see the lock-discipline note above).
        let entries = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            match mr.read_committed_entries(SEQUENCER_GROUP_ID, lo, hi) {
                Ok(entries) => entries,
                Err(nodedb_cluster::error::ClusterError::Raft(
                    nodedb_raft::RaftError::LogCompacted { .. },
                )) => {
                    // The armed index has been compacted below the retained log.
                    // The sequencer-group compaction hold-down (floored at
                    // `min_catch_up_from`) is meant to make this unreachable for
                    // an armed catch-up; if it is nonetheless hit (e.g. a
                    // snapshot-install resync that already subsumes this index),
                    // no replay is owed. Escalate non-silently, CLEAR the entry
                    // to avoid an infinite retry against a permanently-compacted
                    // index, and return.
                    self.metrics.record_catch_up_log_compacted();
                    tracing::error!(
                        vshard = self.vshard_id,
                        lo,
                        "calvin catch-up: sequencer log compacted below armed index; \
                         state is snapshot-covered"
                    );
                    self.sequencer_state_machine
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .clear_catch_up_up_to(self.vshard_id, hi);
                    return;
                }
                Err(e) => {
                    // Transient infra fault (e.g. group transiently absent).
                    // Leave the catch-up ARMED (we peeked, did not take) so a
                    // later drain retries it. Surface it rather than swallow.
                    tracing::warn!(
                        vshard = self.vshard_id,
                        lo,
                        hi,
                        error = %e,
                        "calvin catch-up: failed to read committed sequencer entries"
                    );
                    return;
                }
            }
        };

        // 3. SM-lock scope: decode the raw log entries into this vShard's
        //    `SchedulerInput` stream (a pure `&self` read — no side effects).
        let inputs = {
            let sm = self
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            sm.replay_epochs_for_vshard(&entries, self.vshard_id, 0, u64::MAX)
        };

        // 4. Feed each replayed input through the SAME live processing path — no
        //    lock held. Determinism: identical inputs through identical code.
        //    The in-flight guard makes an overlapping already-in-flight Txn a
        //    no-op; Reserve/Release re-application is idempotent.
        let replayed = inputs.len() as u64;
        for input in inputs {
            self.process_scheduler_input(input);
        }

        // Replay of `lo ..= hi` is complete: clear the armed catch-up, but only
        // up to `hi` — a concurrent drop recorded at an index `> hi` while this
        // replay ran is preserved for the next drain. This is the CONFIRM step
        // the peek-not-take at the top defers to; a transient failure above
        // returned early and left the entry armed.
        self.sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear_catch_up_up_to(self.vshard_id, hi);

        if replayed > 0 {
            self.metrics.record_catch_up_replayed(replayed);
            tracing::info!(
                vshard = self.vshard_id,
                lo,
                hi,
                replayed,
                "calvin catch-up: replayed dropped sequencer inputs from committed log"
            );
        }
    }
}
