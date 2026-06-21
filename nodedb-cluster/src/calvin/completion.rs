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
/// either all expected vshards acked (`Completed`), or the executor reported an
/// OLLP prediction mismatch that forces a retry (`Mismatch`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    Completed,
    Mismatch,
}

struct PendingCompletion {
    expected_participants: usize,
    acked_vshards: BTreeSet<u32>,
    completion_tx: Option<oneshot::Sender<AttemptOutcome>>,
    /// Set when an OLLP mismatch is observed before the coordinator registers
    /// its waiter, so the outcome is not lost across registration order (mirrors
    /// how `acked_vshards` persists ack state regardless of registration order).
    mismatched: bool,
}

impl PendingCompletion {
    fn new(expected_participants: usize) -> Self {
        Self {
            expected_participants,
            acked_vshards: BTreeSet::new(),
            completion_tx: None,
            mismatched: false,
        }
    }

    fn is_complete(&self) -> bool {
        self.acked_vshards.len() >= self.expected_participants
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

    pub fn register_completion(&self, txn: TxnId) -> oneshot::Receiver<AttemptOutcome> {
        let (tx, rx) = oneshot::channel();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        // Mismatch takes precedence over completion: a mismatched attempt must
        // retry, never falsely report success.
        if entry.mismatched {
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
        let rx = reg.register_completion(txn);
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
        let rx = reg.register_completion(txn);
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
        let rx = reg.register_completion(txn);
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
        let rx = reg.register_completion(txn);
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
    async fn mismatch_takes_precedence_over_pending_acks() {
        let reg = CalvinCompletionRegistry::new();
        let txn = TxnId::new(13, 2);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn);
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
