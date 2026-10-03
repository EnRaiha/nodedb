// SPDX-License-Identifier: BUSL-1.1

//! Derivation of the sequencer leader's starting epoch, and the service's
//! response to a halted state machine.
//!
//! The reasoning that guards the seed stays next to the one function that
//! implements it.

use std::sync::Mutex;
use std::sync::atomic::Ordering;

use tracing::{debug, info, warn};

use crate::calvin::sequencer::config::SEQUENCER_GROUP_ID;
use crate::calvin::sequencer::state_machine::SequencerStateMachine;
use crate::multi_raft::MultiRaft;

use super::core::SequencerService;

impl SequencerService {
    /// Derive the epoch seed once, then reuse it for the life of this service.
    ///
    /// Delegates the safety gate to [`derive_epoch_seed`]. `None` means it is
    /// not yet safe to mint an epoch on this node and the caller must skip
    /// minting for this tick.
    ///
    /// Publishes the outcome to `metrics.epoch_seeded` so the readiness probe
    /// can tell whether a Calvin submit landing here can be sequenced.
    pub(super) fn ensure_epoch_seeded(&mut self) -> Option<u64> {
        let seed = self.derive_or_cached_epoch();
        self.metrics
            .epoch_seeded
            .store(seed.is_some(), Ordering::Relaxed);
        seed
    }

    /// The seed itself, without the readiness publication.
    fn derive_or_cached_epoch(&mut self) -> Option<u64> {
        // Checked ahead of the cached seed, not just before deriving one: a halt
        // can land long after the seed was taken. A halted state machine refuses
        // every epoch batch, so a minted epoch would only manufacture identities
        // that nothing on this node will ever apply.
        if self.state_machine_halted() {
            return None;
        }
        if let Some(epoch) = self.current_epoch {
            return Some(epoch);
        }
        let epoch = derive_epoch_seed(self.node_id, &self.multi_raft, &self.state_machine)?;
        self.current_epoch = Some(epoch);
        Some(epoch)
    }

    /// Whether this node's sequencer state machine has stopped applying epoch
    /// batches after an unrecoverable epoch regression.
    pub(super) fn state_machine_halted(&self) -> bool {
        self.state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_halted()
    }

    /// Fail every queued submission fast while the state machine is halted.
    ///
    /// A halt scopes the fault to sequencing: reads, non-Calvin writes, metadata
    /// and every other engine on this node are unaffected, so the node keeps
    /// serving. What it must not do is keep accepting Calvin work — nothing will
    /// ever sequence it. Dropping each submission's assignment makes the
    /// awaiting Control-Plane caller observe a closed channel immediately and
    /// surface an error, instead of every writer hanging to its deadline behind
    /// a queue that will never drain. Reservation requests degrade to plain OCC
    /// the same way they do on a follower.
    pub(super) fn shed_submissions_after_halt(&mut self) {
        let discarded = self.discard_inbox();
        let reservations_discarded = self.reservation_receiver.drain_all_discard();
        if !self.halt_reported {
            self.halt_reported = true;
            tracing::error!(
                node_id = self.node_id,
                "sequencer state machine halted on an epoch regression; this node has stopped \
                 sequencing and is failing Calvin submissions fast. Every other query path \
                 keeps serving — operator intervention is required to resume sequencing."
            );
        }
        if discarded > 0 || reservations_discarded > 0 {
            debug!(
                node_id = self.node_id,
                discarded, reservations_discarded, "sequencer halted; shed queued submissions"
            );
        }
    }
}

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
