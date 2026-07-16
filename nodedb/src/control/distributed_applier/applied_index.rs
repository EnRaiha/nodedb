// SPDX-License-Identifier: BUSL-1.1

//! Durable per-group Raft applied index — the boundary that keeps WAL replay
//! and Raft log replay from applying the same committed entry twice on boot.
//!
//! Two independent mechanisms rebuild engine state on startup: WAL replay
//! (`data::executor::wal_replay_all`) and Raft log replay (raft re-delivers
//! every retained entry above its applied floor). For surrogate-keyed absolute
//! overwrites a double apply is invisible, but for append-shaped writes — a
//! spatial row insert, a columnar/timeseries memtable append, a predicated
//! `Update` — it duplicates data.
//!
//! The invariant that separates them:
//!
//! > durable `applied_index >= N`  ⟹  the redo record for entry N is
//! > WAL-fsync-durable.
//!
//! So on boot WAL replay covers everything `<= applied_index`, raft resumes
//! delivery at `applied_index + 1`, and the two never overlap. Maintaining it
//! is entirely about WHERE the index advances: only after the write funnel's
//! durable-at-ack barrier has fsynced that entry's redo record. Advancing at
//! engine-apply instead would leave a crash window between the engine's commit
//! and the index write in which the entry is applied but not covered — the
//! double-apply this exists to close.

use std::sync::Arc;

use crate::control::state::SharedState;

/// Durably record `applied_index` as `group_id`'s applied floor.
///
/// The caller MUST only invoke this for an entry that applied SUCCESSFULLY and
/// whose redo record is already fsynced — i.e. after `submit_write` has
/// returned `Ok` with an ok status, whose durable-at-ack barrier has performed
/// that fsync. It deliberately does NOT fsync again.
///
/// A failed apply must NOT advance the floor, and neither may any entry BEHIND
/// one: leaving the floor below the first failure is what keeps that entry
/// replayable on the next boot instead of silently skipped. Use
/// [`AppliedPrefix`] to compute the index rather than passing a bare success.
///
/// The sink is monotonic per group at the raft node, so an out-of-order call
/// can never move a floor backwards. A no-op when no sink is installed
/// (single-node mode without raft wiring).
pub fn save_applied_index(state: &Arc<SharedState>, group_id: u64, applied_index: u64) {
    let Some(sink) = state.raft_applied_index_sink.get() else {
        return;
    };
    if let Err(e) = sink(group_id, applied_index) {
        // A failed save costs correctness only in the safe direction: the floor
        // stays behind, so the next boot re-delivers entries WAL replay also
        // covers — the pre-existing double-apply — rather than skipping any.
        // The next successful apply in this group re-saves a higher index and
        // closes the gap.
        tracing::warn!(
            group_id,
            applied_index,
            error = %e,
            "failed to persist durable raft applied index"
        );
    }
}

/// The highest CONTIGUOUS successfully-applied prefix of one apply batch — the
/// only index in that batch it is safe to save as the group's durable floor.
///
/// Contiguity is the whole point. The floor is the boot-time resume point: raft
/// re-delivers from `floor + 1`, so an index saved past a failed entry means
/// nothing ever delivers that entry again — silent, permanent loss, and
/// unrecoverable because the sink is monotonic and cannot be walked back. If
/// entry 3 fails and entry 4 succeeds, the batch's floor is 3's predecessor, no
/// matter how many later entries applied: their effects are already durable and
/// re-applying them on the next boot only costs the pre-existing double-apply,
/// which is the safe direction.
///
/// This holds `Option<u64>` rather than a running maximum because entries arrive
/// in ascending index order (raft collects committed entries as a contiguous
/// `last_applied + 1 ..= commit_index` range), so the last index the unbroken
/// prefix reaches is also the highest one.
#[derive(Debug, Default)]
pub struct AppliedPrefix {
    floor: Option<u64>,
    broken: bool,
}

impl AppliedPrefix {
    /// Start a fresh prefix for one batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record entry `index`'s apply outcome.
    ///
    /// A failure breaks the prefix permanently for this batch: every later
    /// entry, successful or not, is past the gap and can no longer be covered
    /// by the floor. Call this ONLY for entries whose success means "this
    /// entry's redo record is WAL-fsync-durable" — branches that apply no
    /// durable state must not call it at all (see [`Self::skip`]).
    pub fn record(&mut self, index: u64, applied_ok: bool) {
        if !applied_ok {
            self.broken = true;
            return;
        }
        if !self.broken {
            self.floor = Some(index);
        }
    }

    /// Note an entry that carries no durable state — it neither advances the
    /// prefix nor breaks it.
    ///
    /// Advancing on it would assert a redo record that was never written;
    /// breaking on it would stall the floor and force the batch's later,
    /// genuinely durable writes to be re-delivered and applied twice. A
    /// no-op is the only correct answer, and it is spelled out rather than
    /// left implicit so every branch of the apply loop is deliberate.
    pub fn skip(&self) {}

    /// The batch's durable floor, or `None` when nothing in it applied durably.
    pub fn floor(&self) -> Option<u64> {
        self.floor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_success_batch_floors_at_last_index() {
        let mut prefix = AppliedPrefix::new();
        for index in 7..=10 {
            prefix.record(index, true);
        }
        assert_eq!(prefix.floor(), Some(10));
    }

    #[test]
    fn middle_failure_floors_before_the_gap_and_never_past_it() {
        let mut prefix = AppliedPrefix::new();
        prefix.record(1, true);
        prefix.record(2, true);
        prefix.record(3, false);
        // Entry 3 never applied; 4 and 5 did. Saving 5 would make the next boot
        // resume at 6 and drop 3 forever.
        prefix.record(4, true);
        prefix.record(5, true);
        assert_eq!(prefix.floor(), Some(2));
    }

    #[test]
    fn all_failure_batch_saves_nothing() {
        let mut prefix = AppliedPrefix::new();
        prefix.record(1, false);
        prefix.record(2, false);
        assert_eq!(prefix.floor(), None);
    }

    #[test]
    fn leading_failure_floors_at_nothing_despite_later_success() {
        let mut prefix = AppliedPrefix::new();
        prefix.record(1, false);
        prefix.record(2, true);
        assert_eq!(prefix.floor(), None);
    }

    #[test]
    fn empty_batch_saves_nothing() {
        assert_eq!(AppliedPrefix::new().floor(), None);
    }

    #[test]
    fn skipped_entries_neither_advance_nor_break_the_prefix() {
        let mut prefix = AppliedPrefix::new();
        prefix.record(1, true);
        prefix.skip();
        prefix.record(3, true);
        assert_eq!(prefix.floor(), Some(3));

        // A skip after a break stays broken.
        let mut broken = AppliedPrefix::new();
        broken.record(1, true);
        broken.record(2, false);
        broken.skip();
        broken.record(4, true);
        assert_eq!(broken.floor(), Some(1));
    }
}
