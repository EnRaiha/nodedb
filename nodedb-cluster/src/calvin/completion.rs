// SPDX-License-Identifier: BUSL-1.1

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

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
/// all expected vshards acked (`Completed`), the executor reported an OLLP
/// prediction mismatch that forces a retry (`Mismatch`), or the scheduler
/// rejected the transaction's routing as terminally broken (`Failed`) — this
/// last case is NEVER retried, unlike `Mismatch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    Completed,
    Mismatch,
    Failed { detail: String },
}

struct PendingCompletion {
    expected_participants: usize,
    acked_vshards: BTreeSet<u32>,
    completion_tx: Option<oneshot::Sender<AttemptOutcome>>,
    /// Set when an OLLP mismatch is observed before the coordinator registers
    /// its waiter, so the outcome is not lost across registration order (mirrors
    /// how `acked_vshards` persists ack state regardless of registration order).
    mismatched: bool,
    /// Set when a terminal routing failure is observed before the coordinator
    /// registers its waiter, mirroring `mismatched`. Takes precedence over both
    /// `mismatched` and completion: a routing failure is never retried and never
    /// falsely reported as success.
    routing_failed: Option<String>,
    /// Durable per-participant commit votes tallied from `SequencerEntry::Vote`.
    /// Keyed by vshard so a re-proposed vote (retry) overwrites deterministically
    /// rather than double-counting. Currently observed-only: nothing reads this
    /// tally to change flush/drop behavior yet (that is a follow-up); the
    /// leader's local decision still drives.
    votes: BTreeMap<u32, bool>,
}

impl PendingCompletion {
    fn new(expected_participants: usize) -> Self {
        Self {
            expected_participants,
            acked_vshards: BTreeSet::new(),
            completion_tx: None,
            mismatched: false,
            routing_failed: None,
            votes: BTreeMap::new(),
        }
    }

    fn is_complete(&self) -> bool {
        // Require a KNOWN participant count (>0). The `expected_participants == 0`
        // default means "not yet seeded" — completion must not fire until the
        // count is known (via `note_assigned` on the leader, or `register_completion`
        // from the routed assignment on a remote coordinator). Without this guard a
        // replicated ack that races ahead of seeding, or a bare `register_completion`,
        // would spuriously report `Completed` with zero acks.
        self.expected_participants > 0 && self.acked_vshards.len() >= self.expected_participants
    }
}

#[derive(Default)]
struct Inner {
    assignments: BTreeMap<u64, oneshot::Sender<(u64, u32, usize)>>,
    completions: BTreeMap<TxnId, PendingCompletion>,
}

#[derive(Default)]
pub struct CalvinCompletionRegistry {
    inner: Mutex<Inner>,
}

impl CalvinCompletionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register_submission(&self, inbox_seq: u64) -> oneshot::Receiver<(u64, u32, usize)> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .assignments
            .insert(inbox_seq, tx);
        rx
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
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(expected_participants));
        entry.expected_participants = entry.expected_participants.max(expected_participants);
        // Routing failure takes precedence over everything else: it is terminal
        // and must never be masked by a later ack or mismatch signal.
        if let Some(detail) = entry.routing_failed.take() {
            inner.completions.remove(&txn);
            if tx.send(AttemptOutcome::Failed { detail }).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before routing-failure signal; \
                     client likely timed out on completion wait"
                );
            }
        } else if entry.mismatched {
            inner.completions.remove(&txn);
            if tx.send(AttemptOutcome::Mismatch).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before OLLP-mismatch signal; \
                     client likely timed out on completion wait"
                );
            }
        } else if entry.is_complete() {
            inner.completions.remove(&txn);
            if tx.send(AttemptOutcome::Completed).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before all-acked signal; \
                     client likely timed out on completion wait"
                );
            }
        } else {
            entry.completion_tx = Some(tx);
        }
        rx
    }

    pub fn note_completion_ack(&self, txn: TxnId, vshard_id: u32) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.acked_vshards.insert(vshard_id);
        if entry.is_complete() {
            let tx = entry.completion_tx.take();
            inner.completions.remove(&txn);
            if let Some(tx) = tx
                && tx.send(AttemptOutcome::Completed).is_err()
            {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    vshard_id,
                    "calvin completion receiver dropped before final ack; \
                     client likely timed out on completion wait"
                );
            }
        }
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
            inner.completions.remove(&txn);
            if tx.send(AttemptOutcome::Mismatch).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before OLLP-mismatch signal; \
                     client likely timed out on completion wait"
                );
            }
        }
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
            inner.completions.remove(&txn);
            if tx.send(AttemptOutcome::Failed { detail }).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before routing-failure signal; \
                     client likely timed out on completion wait"
                );
            }
        }
    }

    /// Record one participant vshard's durable commit vote for `txn`, tallied
    /// from a replicated `SequencerEntry::Vote`.
    ///
    /// This is observed-only: it accumulates the vote per `vshard` (last write
    /// wins, deterministic across re-proposals from retries) but does NOT
    /// compute a verdict or wake any waiter — the local flush/drop decision
    /// still drives. A follow-up aggregates the tally into the commit
    /// verdict.
    pub fn note_vote(&self, txn_id: TxnId, vshard: u32, commit: bool) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn_id)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.votes.insert(vshard, commit);
    }

    /// Test/inspection accessor: returns the current per-vshard vote tally for
    /// `txn_id`, or `None` if no entry (and therefore no votes) exist yet.
    pub fn vote_tally(&self, txn_id: TxnId) -> Option<BTreeMap<u32, bool>> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .get(&txn_id)
            .map(|entry| entry.votes.clone())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completion_entry_removed_after_all_acks() {
        let reg = CalvinCompletionRegistry::new();
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
        let reg = CalvinCompletionRegistry::new();
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
        let reg = CalvinCompletionRegistry::new();
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
        let reg = CalvinCompletionRegistry::new();
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
        let reg = CalvinCompletionRegistry::new();
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
        let reg = CalvinCompletionRegistry::new();
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
        let reg = CalvinCompletionRegistry::new();
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
        let reg = CalvinCompletionRegistry::new();
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
        let reg = CalvinCompletionRegistry::new();
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
    async fn note_vote_tallies_one_vote_per_vshard() {
        let reg = CalvinCompletionRegistry::new();
        let txn = TxnId::new(30, 0);
        reg.note_vote(txn, 1, true);
        reg.note_vote(txn, 2, false);
        let tally = reg.vote_tally(txn).expect("entry created by note_vote");
        assert_eq!(tally.len(), 2);
        assert_eq!(tally.get(&1), Some(&true));
        assert_eq!(tally.get(&2), Some(&false));
    }

    #[tokio::test]
    async fn mismatch_takes_precedence_over_pending_acks() {
        let reg = CalvinCompletionRegistry::new();
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
}
