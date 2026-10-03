// SPDX-License-Identifier: BUSL-1.1

//! One transaction's completion entry in the
//! [`super::completion::CalvinCompletionRegistry`], and the outcome it fires.

use std::collections::{BTreeMap, BTreeSet};

use super::completion::{AttemptOutcome, ParticipantVote, VerdictOutcome};
use super::completion_waiter::CompletionWaiter;

pub(crate) struct PendingCompletion {
    pub(crate) expected_participants: usize,
    pub(crate) acked_vshards: BTreeSet<u32>,
    /// The apply result each acked participant's `CompletionAck` carried,
    /// by vShard. A participant whose ack carried none has no entry.
    pub(crate) ack_results: BTreeMap<u32, Vec<u8>>,
    pub(crate) completion_tx: Option<CompletionWaiter>,
    /// Set when an OLLP mismatch is observed before the coordinator registers
    /// its waiter, so the outcome is not lost across registration order (mirrors
    /// how `acked_vshards` persists ack state regardless of registration order).
    pub(crate) mismatched: bool,
    /// Set when a terminal routing failure is observed before the coordinator
    /// registers its waiter, mirroring `mismatched`. Takes precedence over both
    /// `mismatched` and completion: a routing failure is never retried and never
    /// falsely reported as success.
    pub(crate) routing_failed: Option<String>,
    /// Set when the multi-part transaction lost its parts
    /// (`SequencerEntry::TxnPartsAbandoned`). Its outcome is
    /// `Aborted { PartsLost }` without waiting for acks: no participant
    /// staged the whole transaction.
    pub(crate) abandoned: bool,
    /// Durable per-participant commit votes tallied from `SequencerEntry::Vote`
    /// and `SequencerEntry::AbortVote`, keyed by vshard so a re-proposed vote
    /// (retry) overwrites deterministically. Once complete, the leader
    /// aggregates the tally into the global `verdict` that gates each
    /// participant's flush/drop at the cross-shard commit barrier.
    pub(crate) votes: BTreeMap<u32, ParticipantVote>,
    /// The authoritative commit/abort verdict, applied from a replicated
    /// `SequencerEntry::Verdict` or `AbortVerdict`; `None` until applied. This
    /// is the durable barrier gate: a participant parked in `AwaitingVerdict`
    /// resumes into its flush (commit) or drop (abort) once it is set. It is
    /// also the ONLY authority for "already decided" — a post-failover leader
    /// re-proposes from `votes` until it is stored (see
    /// `drain_unproposed_verdicts`).
    pub(crate) verdict: Option<VerdictOutcome>,
    /// Dedup guard for the LOCAL emit path: set the first time the tally becomes
    /// complete so the verdict signal is emitted exactly once across vote
    /// re-proposals. Per-node, non-durable, never reset on failover — so it is
    /// NOT consulted by `drain_unproposed_verdicts` (which trusts only the
    /// durable `verdict`, letting a promoted leader re-propose a still-unstored one).
    pub(crate) verdict_proposed: bool,
    /// Queued for eviction as a terminal entry with no waiter
    /// (`completion_gc`).
    pub(crate) parked: bool,
}

/// Choose the terminal outcome for a COMPLETED entry (all expected vshards
/// acked) by consulting the durable global verdict.
///
/// The verdict is applied from a replicated `SequencerEntry::Verdict` that is
/// strictly ordered BEFORE the abort's `CompletionAck` in the sequencer Raft
/// log, so an ABORT verdict is always stored by the time this entry's
/// completion fires. A stored abort becomes `Aborted`, carrying the reason the
/// verdict recorded; `None` (single-shard / no-verdict paths) and a commit
/// verdict are `Completed`. We deliberately do NOT gate on `verdict.is_some()`
/// — that would stall the no-verdict completion paths.
pub(crate) fn outcome_for(entry: &PendingCompletion) -> AttemptOutcome {
    match entry.verdict {
        Some(VerdictOutcome::Abort(reason)) => AttemptOutcome::Aborted { reason },
        _ => AttemptOutcome::Completed,
    }
}

impl PendingCompletion {
    pub(crate) fn new(expected_participants: usize) -> Self {
        Self {
            expected_participants,
            acked_vshards: BTreeSet::new(),
            ack_results: BTreeMap::new(),
            completion_tx: None,
            mismatched: false,
            routing_failed: None,
            abandoned: false,
            votes: BTreeMap::new(),
            verdict: None,
            verdict_proposed: false,
            parked: false,
        }
    }

    pub(crate) fn has_waiter(&self) -> bool {
        self.completion_tx.is_some()
    }

    /// Whether the entry's outcome is decided: every expected participant
    /// acked, or an OLLP mismatch or a routing failure is recorded.
    pub(crate) fn is_terminal(&self) -> bool {
        self.is_complete() || self.mismatched || self.routing_failed.is_some() || self.abandoned
    }

    /// Every acked participant's apply result, in vShard order.
    pub(crate) fn take_ack_results(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.ack_results)
            .into_values()
            .collect()
    }

    pub(crate) fn is_complete(&self) -> bool {
        // Require a KNOWN participant count (>0). The `expected_participants == 0`
        // default means "not yet seeded" — completion must not fire until the
        // count is known (via `note_assigned` on the leader, or `register_completion`
        // from the routed assignment on a remote coordinator). Without this guard a
        // replicated ack that races ahead of seeding, or a bare `register_completion`,
        // would spuriously report `Completed` with zero acks.
        self.expected_participants > 0 && self.acked_vshards.len() >= self.expected_participants
    }
}
