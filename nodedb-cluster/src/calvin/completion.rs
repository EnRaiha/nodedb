// SPDX-License-Identifier: BUSL-1.1

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::calvin::completion_entry::{PendingCompletion, outcome_for};
use crate::calvin::completion_waiter::{CompletionReport, CompletionWaiter};
use crate::calvin::sequencer::AbortReason;

/// The sequencer assignment of one submission: `(epoch, position,
/// participants)`.
pub type Assignment = (u64, u32, usize);

/// Receives one submission's [`Assignment`]. It reads a closed channel when
/// the sequencer rejected or discarded the submission without sequencing it.
pub type AssignmentReceiver = oneshot::Receiver<Assignment>;

/// Calvin transaction identity in the sequencer-assigned coordinate space.
///
/// `(epoch, position)` is the unique key the sequencer Raft state machine
/// stamps onto every admitted transaction; it is the join key between the
/// completion-awaiter side and the per-vshard ack side of the registry.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct TxnId {
    pub epoch: u64,
    pub position: u32,
}

impl TxnId {
    pub fn new(epoch: u64, position: u32) -> Self {
        Self { epoch, position }
    }
}

/// Terminal outcome of a single Calvin transaction attempt.
///
/// Exactly one of these fires per attempt on the unified completion channel:
/// all expected vshards acked AND the global verdict was commit (`Completed`),
/// all expected vshards acked but the global cross-shard verdict was ABORT
/// (`Aborted`), the executor reported an OLLP prediction mismatch that forces a
/// retry (`Mismatch`), or the scheduler rejected the transaction's routing as
/// terminally broken (`Failed`). `Failed` is never retried. `Aborted` retries
/// only for `AbortReason::PredictionDrift`, which is the same drift as
/// `Mismatch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    Completed,
    /// The global cross-shard verdict was ABORT. `reason` names the cause.
    Aborted {
        reason: AbortReason,
    },
    Mismatch,
    Failed {
        detail: String,
    },
}

/// One participant vShard's durable commit vote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantVote {
    Commit,
    Abort(AbortReason),
}

/// A commit/abort decision: the tally aggregated from votes, and the
/// authoritative verdict stored on a `PendingCompletion`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerdictOutcome {
    Commit,
    Abort(AbortReason),
}

impl VerdictOutcome {
    /// `true` for a commit decision. The scheduler's flush/drop gate needs only
    /// this bit; the reason travels to the coordinator instead.
    pub fn is_commit(self) -> bool {
        matches!(self, Self::Commit)
    }
}

/// What this node's registry has applied for one participant vShard of a txn.
///
/// A scheduler reads it to learn whether a sequencer entry it proposed has
/// been applied here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantProgress {
    /// A `Vote` or `AbortVote` from this vShard is in the tally.
    pub voted: bool,
    /// A `CompletionAck` from this vShard is recorded.
    pub acked: bool,
    /// The txn's global verdict is stored.
    pub has_verdict: bool,
    /// An `OllpMismatch` for the txn is recorded.
    pub mismatched: bool,
    /// A `TxnRoutingFailed` for the txn is recorded and not yet delivered.
    pub routing_failed: bool,
}

/// `pub(crate)`: also read by the vote/verdict-tally methods in
/// `completion_verdict.rs` (a sibling module in the same crate).
#[derive(Default)]
pub(crate) struct Inner {
    assignments: BTreeMap<u64, oneshot::Sender<Assignment>>,
    pub(crate) completions: BTreeMap<TxnId, PendingCompletion>,
    /// Per-vShard senders for the verdict push, keyed by vShard id. Each local
    /// Calvin scheduler registers its receiver's sender here at construction;
    /// `note_verdict` broadcasts a [`super::completion_verdict::VerdictSignal`]
    /// to all of them under this same mutex, so a stored verdict and its push
    /// notification never disagree.
    pub(crate) verdict_signal_senders:
        BTreeMap<u32, mpsc::Sender<super::completion_verdict::VerdictSignal>>,
    /// Terminal entries with no waiter, oldest first, with the instant each
    /// became one (`completion_gc`).
    pub(crate) waiterless: VecDeque<(Instant, TxnId)>,
    /// How long a terminal entry waits for a waiter before eviction. `None`
    /// takes [`super::completion_gc::DEFAULT_WAITERLESS_TTL`].
    pub(crate) waiterless_ttl: Option<Duration>,
}

impl Inner {
    /// Remove `txn`'s entry and deliver `outcome` to `waiter`. `signal` names
    /// the event in the warning logged when the receiver is gone.
    pub(crate) fn fire(
        &mut self,
        txn: TxnId,
        waiter: CompletionWaiter,
        outcome: AttemptOutcome,
        ack_results: Vec<Vec<u8>>,
        signal: &'static str,
    ) {
        self.completions.remove(&txn);
        if !waiter.send(outcome, ack_results) {
            tracing::warn!(
                epoch = txn.epoch,
                position = txn.position,
                signal,
                "calvin completion receiver dropped before its outcome fired; \
                 client likely timed out on completion wait"
            );
        }
    }
}

pub struct CalvinCompletionRegistry {
    /// `pub(crate)`: also locked by the vote/verdict-tally methods in
    /// `completion_verdict.rs` (a sibling module in the same crate); never
    /// exposed beyond the crate.
    pub(crate) inner: Mutex<Inner>,
    /// Emits `(txn, verdict)` exactly once when a staged cross-shard txn's vote
    /// tally becomes complete (all expected participants voted). The paired
    /// receiver lives in the `SequencerService`, whose leader-guarded arm turns
    /// the signal into a `SequencerEntry::Verdict` proposal. `pub(crate)`: also
    /// used by `note_vote` in `completion_verdict.rs`.
    pub(crate) verdict_tx: mpsc::Sender<(TxnId, VerdictOutcome)>,
    /// Completion acks this node applied, with their Raft index.
    pub applied_acks: super::applied_acks::AppliedAckLog,
}

impl CalvinCompletionRegistry {
    /// Construct a registry wired to a verdict signal channel. The paired
    /// receiver must be handed to the `SequencerService` on this node so the
    /// leader can propose the aggregated verdict.
    pub fn new(verdict_tx: mpsc::Sender<(TxnId, VerdictOutcome)>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            verdict_tx,
            applied_acks: super::applied_acks::AppliedAckLog::default(),
        })
    }

    /// Construct a registry with no verdict consumer: the signal channel is
    /// created internally and its receiver dropped, so vote-complete transitions
    /// are still computed and stored but never delivered to a sequencer service.
    /// For callers (and tests) that do not drive verdict proposal.
    pub fn new_detached() -> Arc<Self> {
        let (verdict_tx, _verdict_rx) = mpsc::channel(1);
        Self::new(verdict_tx)
    }

    /// Register interest in the assignment of submission `inbox_seq`.
    ///
    /// Call it before the submission reaches the inbox channel, so the tick
    /// that drains it always finds the sender. `Inbox::submit_with` does so.
    pub fn register_submission(&self, inbox_seq: u64) -> AssignmentReceiver {
        let (tx, rx) = oneshot::channel();
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .assignments
            .insert(inbox_seq, tx);
        rx
    }

    /// Drop the assignment sender of submission `inbox_seq`, if one is held.
    ///
    /// The sequencer calls it for every submission it rejects or discards
    /// without sequencing. The waiting caller then reads a closed channel at
    /// once. A caller that gives up waiting calls it too, so no sender stays
    /// behind.
    pub fn drop_assignment(&self, inbox_seq: u64) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .assignments
            .remove(&inbox_seq);
    }

    pub fn note_assigned(&self, inbox_seq: u64, txn: TxnId, expected_participants: usize) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(tx) = inner.assignments.remove(&inbox_seq)
            && tx
                .send((txn.epoch, txn.position, expected_participants))
                .is_err()
        {
            tracing::warn!(
                inbox_seq,
                epoch = txn.epoch,
                position = txn.position,
                "calvin assignment receiver dropped before sequencer position arrived; \
                 client likely timed out on submit_with_retry"
            );
        }
        inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(expected_participants));
    }

    /// Register interest in `txn`'s terminal outcome, seeding the authoritative
    /// `expected_participants` from the (routed) assignment.
    ///
    /// Cross-node, the coordinator's registry never receives `note_assigned` —
    /// that fires only on the sequencer leader — so the participant count arrives
    /// here, via `RoutedAssignment.participants`. `max` upgrades the unknown (0)
    /// default and is idempotent when `note_assigned` already seeded it single-node.
    pub fn register_completion(
        &self,
        txn: TxnId,
        expected_participants: usize,
    ) -> oneshot::Receiver<AttemptOutcome> {
        let (tx, rx) = oneshot::channel();
        self.register_waiter(txn, expected_participants, CompletionWaiter::Outcome(tx));
        rx
    }

    /// [`Self::register_completion`] for a coordinator that also reads each
    /// participant's apply result, as its `CompletionAck` carried it. The
    /// acks reach every sequencer replica, so the results arrive whether or
    /// not this node hosts a replica of each participant.
    pub fn register_completion_report(
        &self,
        txn: TxnId,
        expected_participants: usize,
    ) -> oneshot::Receiver<CompletionReport> {
        let (tx, rx) = oneshot::channel();
        self.register_waiter(txn, expected_participants, CompletionWaiter::Report(tx));
        rx
    }

    fn register_waiter(&self, txn: TxnId, expected_participants: usize, tx: CompletionWaiter) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(expected_participants));
        entry.expected_participants = entry.expected_participants.max(expected_participants);
        // Routing failure takes precedence over everything else: it is terminal
        // and must never be masked by a later ack or mismatch signal.
        if let Some(detail) = entry.routing_failed.take() {
            inner.fire(
                txn,
                tx,
                AttemptOutcome::Failed { detail },
                Vec::new(),
                "routing failure",
            );
        } else if entry.abandoned {
            // The entry keeps its stored verdict for the participants that
            // still probe it, and parks as a waiterless one.
            super::completion_parts::send_parts_lost(txn, tx);
            inner.settle_waiterless(txn);
        } else if entry.mismatched {
            inner.fire(
                txn,
                tx,
                AttemptOutcome::Mismatch,
                Vec::new(),
                "OLLP mismatch",
            );
        } else if entry.is_complete() {
            // Acks raced ahead of waiter registration: consult the stored
            // verdict so an already-complete ABORT surfaces as `Aborted`, not a
            // false `Completed`.
            let outcome = outcome_for(entry);
            let results = entry.take_ack_results();
            inner.fire(txn, tx, outcome, results, "all acked");
        } else {
            entry.completion_tx = Some(tx);
        }
    }

    pub fn note_completion_ack(&self, txn: TxnId, vshard_id: u32) {
        self.note_completion_ack_with(txn, vshard_id, Vec::new());
    }

    /// Record `vshard_id`'s `CompletionAck` for `txn`, with the apply result
    /// it carried. An empty `result` records none.
    pub fn note_completion_ack_with(&self, txn: TxnId, vshard_id: u32, result: Vec<u8>) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.acked_vshards.insert(vshard_id);
        if !result.is_empty() {
            entry.ack_results.insert(vshard_id, result);
        }
        if entry.is_complete() {
            // Consult the stored global verdict: `Verdict` is applied strictly
            // before this abort's `CompletionAck` in the sequencer Raft log, so
            // an ABORT is `Some(false)` here and must surface as `Aborted`
            // rather than a silent `Completed` (which would drop the writes and
            // report COMMIT SUCCESS to the client).
            //
            // Only fire + evict when the coordinator's waiter is registered. If
            // the final ack races AHEAD of registration, LEAVE the entry — its
            // `acked_vshards` (and the stored verdict) persist, so
            // `register_completion`'s `is_complete()` branch fires the outcome.
            // Evicting here would strand that branch (a fresh entry created by
            // the later `register_completion` never re-completes). Mirrors how
            // `mismatched`/`routing_failed` persist across the same race.
            if let Some(tx) = entry.completion_tx.take() {
                let outcome = outcome_for(entry);
                let results = entry.take_ack_results();
                inner.fire(txn, tx, outcome, results, "final ack");
            }
        }
        inner.settle_waiterless(txn);
    }

    /// Record an OLLP prediction mismatch for `txn`, the second terminal outcome
    /// of an attempt. Mismatch takes precedence over completion: a mismatched
    /// attempt must retry, never falsely report success.
    ///
    /// If the coordinator's waiter is already registered, fire `Mismatch` and
    /// evict the entry. Otherwise leave the `mismatched` flag set so a later
    /// `register_completion` fires it (mirrors `acked_vshards` persistence).
    pub fn note_ollp_mismatch(&self, txn: TxnId) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.mismatched = true;
        if let Some(tx) = entry.completion_tx.take() {
            inner.fire(
                txn,
                tx,
                AttemptOutcome::Mismatch,
                Vec::new(),
                "OLLP mismatch",
            );
        }
        inner.settle_waiterless(txn);
    }

    /// Record a terminal, NON-retryable routing failure for `txn` — the
    /// scheduler rejected the transaction's local plan routing as
    /// `Unroutable`, `ControlPlaneOnly`, or `NotAWrite`. Takes precedence over
    /// completion AND over an OLLP mismatch: a routing failure can never
    /// converge via retry, so it must never be masked by a later ack or
    /// mismatch signal.
    ///
    /// If the coordinator's waiter is already registered, fire `Failed` and
    /// evict the entry. Otherwise leave the `routing_failed` detail set so a
    /// later `register_completion` fires it (mirrors `mismatched` persistence).
    pub fn note_routing_failed(&self, txn: TxnId, detail: String) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.routing_failed = Some(detail.clone());
        if let Some(tx) = entry.completion_tx.take() {
            inner.fire(
                txn,
                tx,
                AttemptOutcome::Failed { detail },
                Vec::new(),
                "routing failure",
            );
        }
        inner.settle_waiterless(txn);
    }

    /// Set how long a terminal entry with no waiter stays for one to
    /// register. The host sets it longer than its statement deadline.
    pub fn set_waiterless_ttl(&self, ttl: Duration) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .waiterless_ttl = Some(ttl);
    }

    /// What this registry holds for participant `vshard` of `txn`.
    ///
    /// `None` means no entry exists for `txn`. That is either a txn this node
    /// never seeded, or one whose outcome already fired and evicted its entry.
    pub fn participant_progress(&self, txn: TxnId, vshard: u32) -> Option<ParticipantProgress> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .get(&txn)
            .map(|entry| ParticipantProgress {
                voted: entry.votes.contains_key(&vshard),
                acked: entry.acked_vshards.contains(&vshard),
                has_verdict: entry.verdict.is_some(),
                mismatched: entry.mismatched,
                routing_failed: entry.routing_failed.is_some(),
            })
    }

    /// Test-only: returns the number of pending completion entries.
    /// Used to verify entries are removed once all acks arrive (no leak).
    #[cfg(test)]
    pub fn pending_completions_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .len()
    }

    /// Test-only: the number of assignment senders held.
    #[cfg(test)]
    pub fn pending_assignments_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .assignments
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A replica on which no coordinator waits keeps no completion entry,
    /// and no ack result, past the eviction window. An entry that has a
    /// waiter stays until its outcome fires.
    #[tokio::test]
    async fn waiterless_terminal_entries_are_evicted_with_their_results() {
        let reg = CalvinCompletionRegistry::new_detached();
        reg.set_waiterless_ttl(Duration::ZERO);

        let acked = TxnId::new(3, 0);
        reg.note_assigned(1, acked, 1);
        reg.note_completion_ack_with(acked, 5, vec![0xAB; 4096]);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "a complete entry nobody waits for is evicted with its result"
        );

        let mismatched = TxnId::new(3, 1);
        reg.note_ollp_mismatch(mismatched);
        assert_eq!(reg.pending_completions_len(), 0);

        let awaited = TxnId::new(3, 2);
        let rx = reg.register_completion_report(awaited, 2);
        reg.note_completion_ack_with(awaited, 5, b"five".to_vec());
        reg.note_completion_ack_with(TxnId::new(3, 3), 6, b"other".to_vec());
        assert!(
            reg.participant_progress(awaited, 5).is_some(),
            "an entry with a waiter is never evicted"
        );
        reg.note_completion_ack_with(awaited, 6, b"six".to_vec());
        let report = rx.await.expect("completion fires");
        assert_eq!(report.ack_results, vec![b"five".to_vec(), b"six".to_vec()]);
    }

    /// The coordinator hosts no replica of either participant: it learns
    /// their apply results only from the `CompletionAck`s its sequencer
    /// replica applies. The report carries each participant's result, in
    /// vShard order, whether the acks land before or after it registers.
    #[tokio::test]
    async fn a_report_carries_every_participants_ack_result() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(11, 4);
        reg.note_completion_ack_with(txn, 20, b"twenty".to_vec());
        let rx = reg.register_completion_report(txn, 3);
        reg.note_completion_ack_with(txn, 10, b"ten".to_vec());
        reg.note_completion_ack_with(txn, 30, Vec::new());
        let report = rx.await.expect("completion fires");
        assert_eq!(report.outcome, AttemptOutcome::Completed);
        assert_eq!(
            report.ack_results,
            vec![b"ten".to_vec(), b"twenty".to_vec()]
        );
        assert_eq!(reg.pending_completions_len(), 0);

        let late = TxnId::new(11, 5);
        reg.note_completion_ack_with(late, 7, b"seven".to_vec());
        let report = reg
            .register_completion_report(late, 1)
            .await
            .expect("an already complete txn fires on registration");
        assert_eq!(report.ack_results, vec![b"seven".to_vec()]);
    }

    #[tokio::test]
    async fn completion_entry_removed_after_all_acks() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(7, 0);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        assert_eq!(reg.pending_completions_len(), 1);
        reg.note_completion_ack(txn, 10);
        assert_eq!(reg.pending_completions_len(), 1);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once all expected vshards have acked"
        );
    }

    #[tokio::test]
    async fn completion_entry_removed_when_register_arrives_after_acks() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(9, 3);
        reg.note_assigned(1, txn, 2);
        reg.note_completion_ack(txn, 10);
        // Entry remains: expected=2, only 1 ack received.
        assert_eq!(reg.pending_completions_len(), 1);
        let rx = reg.register_completion(txn, 2);
        assert_eq!(reg.pending_completions_len(), 1);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires once both acks arrived");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once awaiter is signalled"
        );
    }

    #[tokio::test]
    async fn mismatch_arriving_before_register_fires_mismatch() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(11, 1);
        reg.note_assigned(1, txn, 2);
        // Mismatch observed before the coordinator registers its waiter: the
        // flag must persist so a later register_completion fires it.
        reg.note_ollp_mismatch(txn);
        assert_eq!(reg.pending_completions_len(), 1);
        let rx = reg.register_completion(txn, 2);
        let outcome = rx.await.expect("mismatch fires");
        assert_eq!(outcome, AttemptOutcome::Mismatch);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once mismatch is signalled"
        );
    }

    #[tokio::test]
    async fn register_before_mismatch_fires_mismatch() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(12, 5);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        assert_eq!(reg.pending_completions_len(), 1);
        // Waiter already stored; the mismatch must wake it directly.
        reg.note_ollp_mismatch(txn);
        let outcome = rx.await.expect("mismatch fires");
        assert_eq!(outcome, AttemptOutcome::Mismatch);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once mismatch is signalled"
        );
    }

    #[tokio::test]
    async fn register_completion_seeds_participants_without_note_assigned() {
        // Cross-node coordinator: no note_assigned ever fires on its registry, so
        // register_completion must seed expected_participants from the assignment.
        // Without the seed (or with the is_complete>0 guard absent) this would
        // spuriously fire Completed with zero acks.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(21, 0);
        let rx = reg.register_completion(txn, 1);
        assert_eq!(
            reg.pending_completions_len(),
            1,
            "expected=1, 0 acks → must NOT complete prematurely"
        );
        reg.note_completion_ack(txn, 7);
        let outcome = rx.await.expect("completion fires after the single ack");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn ack_racing_ahead_of_register_does_not_prematurely_complete() {
        // The replicated ack can reach a remote coordinator's registry BEFORE the
        // coordinator calls register_completion. With expected_participants still
        // unknown (0), the ack must persist without firing/evicting; the later
        // register_completion seeds the count and then completes.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(22, 0);
        reg.note_completion_ack(txn, 7);
        assert_eq!(
            reg.pending_completions_len(),
            1,
            "ack before seeding must persist, not self-complete on expected=0"
        );
        let rx = reg.register_completion(txn, 1);
        let outcome = rx.await.expect("completion fires once participants seeded");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn routing_failed_arriving_before_register_fires_failed() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(14, 1);
        reg.note_assigned(1, txn, 2);
        // Routing failure observed before the coordinator registers its
        // waiter: the detail must persist so a later register_completion
        // fires it.
        reg.note_routing_failed(txn, "unroutable plan".to_owned());
        assert_eq!(reg.pending_completions_len(), 1);
        let rx = reg.register_completion(txn, 2);
        let outcome = rx.await.expect("routing failure fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Failed {
                detail: "unroutable plan".to_owned()
            }
        );
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once routing failure is signalled"
        );
    }

    #[tokio::test]
    async fn register_before_routing_failed_fires_failed() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(15, 4);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        assert_eq!(reg.pending_completions_len(), 1);
        // Waiter already stored; the routing failure must wake it directly.
        reg.note_routing_failed(txn, "control-plane-only plan".to_owned());
        let outcome = rx.await.expect("routing failure fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Failed {
                detail: "control-plane-only plan".to_owned()
            }
        );
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once routing failure is signalled"
        );
    }

    #[tokio::test]
    async fn routing_failed_takes_precedence_over_pending_acks_and_mismatch() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(16, 0);
        reg.note_assigned(1, txn, 2);
        // No waiter registered yet: an ack and a mismatch both persist onto
        // the entry without firing anything.
        reg.note_completion_ack(txn, 10);
        reg.note_ollp_mismatch(txn);
        assert_eq!(reg.pending_completions_len(), 1);
        // The routing failure also persists (still no waiter)...
        reg.note_routing_failed(txn, "non-write plan".to_owned());
        // ...and when the coordinator finally registers, it must observe the
        // routing failure, not the mismatch or the ack — routing failure is
        // terminal and must never be masked by either.
        let rx = reg.register_completion(txn, 2);
        let outcome = rx.await.expect("routing failure fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Failed {
                detail: "non-write plan".to_owned()
            }
        );
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once routing failure is signalled"
        );
    }

    #[tokio::test]
    async fn abort_verdict_makes_completion_report_aborted() {
        // An ABORT verdict is stored (Raft-ordered) before the acks that complete
        // the tally. Completion must consult it and report `Aborted`, never a
        // silent `Completed` that would drop the writes and report COMMIT success.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(31, 0);
        reg.note_assigned(1, txn, 2);
        reg.note_verdict(
            txn,
            VerdictOutcome::Abort(AbortReason::SerializationConflict),
        );
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Aborted {
                reason: AbortReason::SerializationConflict
            },
            "a stored ABORT verdict must surface as Aborted, not Completed"
        );
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn abort_verdict_reports_aborted_when_register_arrives_after_acks() {
        // Acks (and the verdict) race ahead of waiter registration: the
        // already-complete branch in `register_completion` must also consult the
        // stored verdict and report `Aborted`.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(32, 1);
        reg.note_assigned(1, txn, 2);
        reg.note_verdict(txn, VerdictOutcome::Abort(AbortReason::ParticipantError));
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let rx = reg.register_completion(txn, 2);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Aborted {
                reason: AbortReason::ParticipantError
            }
        );
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn commit_verdict_reports_completed() {
        // A COMMIT verdict must still report `Completed`.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(33, 2);
        reg.note_assigned(1, txn, 2);
        reg.note_verdict(txn, VerdictOutcome::Commit);
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    /// A dropped assignment closes the caller's channel at once and leaves no
    /// sender behind. A later `note_assigned` for the seq finds none.
    #[test]
    fn drop_assignment_closes_the_callers_channel_and_frees_the_sender() {
        let reg = CalvinCompletionRegistry::new_detached();
        let mut rejected = reg.register_submission(4);
        let mut kept = reg.register_submission(5);
        assert_eq!(reg.pending_assignments_len(), 2);

        reg.drop_assignment(4);
        assert_eq!(
            rejected.try_recv(),
            Err(oneshot::error::TryRecvError::Closed),
            "the caller must see a closed channel, not wait out its timeout"
        );
        assert_eq!(reg.pending_assignments_len(), 1);

        reg.note_assigned(5, TxnId::new(2, 0), 3);
        assert_eq!(kept.try_recv(), Ok((2, 0, 3)));
        assert_eq!(reg.pending_assignments_len(), 0);

        // Dropping a seq with no sender is a no-op.
        reg.drop_assignment(4);
        assert_eq!(reg.pending_assignments_len(), 0);
    }

    #[tokio::test]
    async fn no_verdict_reports_completed_unchanged() {
        // Single-shard / no-verdict paths (`verdict == None`) must report
        // `Completed` unchanged — the fix must not stall them.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(34, 3);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn mismatch_takes_precedence_over_pending_acks() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(13, 2);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        // One ack arrives but the attempt is not yet complete (expected=2).
        reg.note_completion_ack(txn, 10);
        assert_eq!(reg.pending_completions_len(), 1);
        // A mismatch on the same attempt must win and force a retry.
        reg.note_ollp_mismatch(txn);
        let outcome = rx.await.expect("mismatch fires");
        assert_eq!(outcome, AttemptOutcome::Mismatch);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once mismatch is signalled"
        );
    }

    #[tokio::test]
    async fn participant_progress_is_none_before_any_entry_exists() {
        let reg = CalvinCompletionRegistry::new_detached();
        assert_eq!(reg.participant_progress(TxnId::new(40, 0), 1), None);
    }

    #[tokio::test]
    async fn participant_progress_reports_each_applied_signal_for_its_vshard() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(40, 1);
        reg.seed_expected(txn, 2);
        let empty = reg.participant_progress(txn, 1).expect("seeded entry");
        assert!(!empty.voted && !empty.acked && !empty.has_verdict);
        assert!(!empty.mismatched && !empty.routing_failed);

        reg.note_vote(txn, 1, ParticipantVote::Commit);
        reg.note_completion_ack(txn, 1);
        let own = reg.participant_progress(txn, 1).expect("entry");
        assert!(own.voted && own.acked);
        let peer = reg.participant_progress(txn, 2).expect("entry");
        assert!(
            !peer.voted && !peer.acked,
            "another vShard's signals do not count"
        );

        reg.note_verdict(txn, VerdictOutcome::Commit);
        reg.note_ollp_mismatch(txn);
        reg.note_routing_failed(txn, "unroutable".to_string());
        let txn_wide = reg.participant_progress(txn, 2).expect("entry");
        assert!(txn_wide.has_verdict && txn_wide.mismatched && txn_wide.routing_failed);
    }

    #[tokio::test]
    async fn participant_progress_is_none_once_the_outcome_fired() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(40, 2);
        let _rx = reg.register_completion(txn, 1);
        reg.note_completion_ack(txn, 1);
        assert_eq!(reg.participant_progress(txn, 1), None);
    }
}
