// SPDX-License-Identifier: BUSL-1.1

//! Derivation of the sequencer leader's starting epoch.
//!
//! Split out of the service so the reasoning that guards it stays next to the
//! one function that implements it.

use std::sync::Mutex;

use tracing::{debug, info, warn};

use crate::calvin::sequencer::config::SEQUENCER_GROUP_ID;
use crate::calvin::sequencer::state_machine::SequencerStateMachine;
use crate::multi_raft::MultiRaft;

/// Derive — once — the first epoch this node may propose, returning `None`
/// while it is not yet safe to derive one.
///
/// INVARIANT: **the first epoch a restarted leader proposes must be
/// strictly greater than any epoch already committed to the sequencer
/// log.** An epoch number is half of every transaction's `(epoch,
/// position)` identity and is also the state machine's ordering check, so
/// re-minting a committed epoch is not a numbering blemish: on replay each
/// replica meets the historical epoch first, then the duplicate, and
/// refuses the duplicate's batch — every transaction in it is lost and its
/// waiters hang to their deadlines.
///
/// The seed can only come from the state machine's `next_epoch()`, and that
/// counter is in-memory: it is rebuilt solely by replaying the sequencer
/// group's committed log. Reading it while the service is being constructed
/// therefore always answers 0, however much history the log holds — the
/// Raft loop that drives the replay is not spawned until later in startup.
/// So the read happens here, lazily, on the first leader tick, gated on the
/// group having applied everything its local log holds.
///
/// The gate compares against the LOG TIP, not `commit_index`: a node that
/// has just won an election can still observe `commit_index` behind its own
/// log (its term's no-op has not committed yet) while `is_leader()` already
/// reports true, and every entry in a leader's log commits moments later
/// under that no-op. Gating on `commit_index` would leave exactly that
/// window open, which is the window a restart lands in.
///
/// The gate is "applied has caught up with the tip", NOT "an entry exists".
/// A brand-new node — and any node nobody has proposed to yet — has
/// `last_applied == log_tip == 0` and passes immediately, seeding epoch 0
/// from an empty state machine. Requiring an entry first would be a deadlock:
/// the only thing that puts the first entry in the sequencer log is this
/// node proposing under the very seed it is waiting for.
///
/// Returns `None` only while a replay is genuinely in flight; the caller then
/// defers minting for that tick (and only minting — every leader duty that
/// stamps no new identity still runs), so submissions stay queued rather than
/// being sequenced under a colliding epoch.
pub(super) fn derive_epoch_seed(
    node_id: u64,
    multi_raft: &Mutex<MultiRaft>,
    state_machine: &Mutex<SequencerStateMachine>,
) -> Option<u64> {
    // Read the Raft-side watermarks and release the lock before taking the
    // state machine's: the two are never held together anywhere, and this
    // is the only site that needs both.
    let (last_applied, log_tip, first_available) = {
        let mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        (
            mr.last_applied(SEQUENCER_GROUP_ID),
            mr.last_log_index(SEQUENCER_GROUP_ID),
            mr.first_available_index(SEQUENCER_GROUP_ID),
        )
    };
    let (Some(last_applied), Some(log_tip)) = (last_applied, log_tip) else {
        warn!(
            node_id,
            "sequencer group is not mounted on this node; cannot derive an epoch seed"
        );
        return None;
    };
    if last_applied < log_tip {
        debug!(
            node_id,
            last_applied, log_tip, "sequencer group still replaying; deferring epoch seed"
        );
        return None;
    }

    let state_machine = state_machine.lock().unwrap_or_else(|p| p.into_inner());
    // The retained log is the only record of committed epochs. If it starts
    // above the first index, earlier entries were discarded (compaction or
    // a snapshot install); when none of the retained ones carried an epoch
    // there is nothing left to derive a seed from, and minting 0 would
    // collide with the discarded history. Refusing to propose is a visible
    // stall; minting anyway is silent loss of every batch that follows.
    if first_available.unwrap_or(1) > 1 && state_machine.last_applied_epoch().is_none() {
        warn!(
            node_id,
            first_available,
            "sequencer log was truncated below every retained epoch; refusing to \
             propose rather than mint an epoch that may collide with discarded history"
        );
        return None;
    }
    let epoch = state_machine.next_epoch();
    drop(state_machine);

    info!(
        node_id,
        epoch, log_tip, "sequencer epoch seed derived from the replayed sequencer log"
    );
    Some(epoch)
}
