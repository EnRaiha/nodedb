// SPDX-License-Identifier: BUSL-1.1

//! Epoch-ordering guard for the Calvin sequencer state machine.
//!
//! The sequencer's epoch counter is not just a sequence number — it is half of
//! every transaction's `(epoch, position)` identity (locks, completion waiters,
//! votes, verdicts all key on it). So an epoch that arrives out of order is
//! never a cosmetic ordering blip: it means this replica's view of the
//! sequencer log and the proposing leader's view have diverged, and the two
//! directions of divergence need opposite handling.
//!
//! - [`EpochCheck::Ahead`] — entries are missing *locally*. The arriving batch
//!   is intact and self-describing, so it is still fanned out; only the entries
//!   between the two epochs were lost, and the scheduler recovers those by
//!   replaying the sequencer Raft log.
//! - [`EpochCheck::Behind`] — the arriving epoch was already consumed here. Its
//!   transactions carry identities that collide with committed history, so they
//!   can neither be applied (they would alias live lock-table / completion
//!   entries) nor be dropped (that is silent loss of committed writes). This is
//!   unrecoverable at the state-machine level and must fail loudly.

use std::sync::Arc;

/// Facts about an unrecoverable epoch regression, handed to the host so it can
/// fail the node loudly instead of continuing with a diverged state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequencerHalt {
    /// The epoch the state machine required next.
    pub expected_epoch: u64,
    /// The epoch the committed entry actually carried.
    pub found_epoch: u64,
    /// How many transactions the offending batch carried.
    pub txns_in_batch: usize,
    /// Raft log index of the offending entry.
    pub raft_index: u64,
}

/// Escalation hook invoked once when the sequencer state machine halts on an
/// unrecoverable epoch regression.
///
/// The host wires this to its fail-stop path. It runs on the Raft apply thread,
/// so it MUST NOT block or do I/O — signalling a shutdown watch is the intended
/// shape.
pub type UnrecoverableEpochHook = Arc<dyn Fn(SequencerHalt) + Send + Sync>;

/// How an arriving epoch relates to the one the state machine required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochCheck {
    /// Exactly the expected epoch — the normal path.
    InOrder,
    /// Past the expected epoch: entries are missing on this replica.
    Ahead,
    /// Below the expected epoch: an already-consumed epoch was re-proposed.
    Behind,
}

impl EpochCheck {
    /// Short, stable label used as a report grouping key so the two divergence
    /// directions never collapse into one group — they have different causes
    /// and different operator actions.
    pub fn direction(self) -> &'static str {
        match self {
            EpochCheck::InOrder => "in_order",
            EpochCheck::Ahead => "ahead",
            EpochCheck::Behind => "behind",
        }
    }
}

/// Classify `found` against the `expected` next epoch.
pub fn classify(expected: u64, found: u64) -> EpochCheck {
    match found.cmp(&expected) {
        std::cmp::Ordering::Equal => EpochCheck::InOrder,
        std::cmp::Ordering::Greater => EpochCheck::Ahead,
        std::cmp::Ordering::Less => EpochCheck::Behind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_names_each_divergence_direction() {
        assert_eq!(classify(5, 5), EpochCheck::InOrder);
        assert_eq!(classify(5, 7), EpochCheck::Ahead);
        assert_eq!(classify(5, 4), EpochCheck::Behind);
        // Epoch 0 against a fresh state machine is in-order, not a regression.
        assert_eq!(classify(0, 0), EpochCheck::InOrder);
    }

    #[test]
    fn directions_are_distinct_labels() {
        assert_ne!(
            EpochCheck::Ahead.direction(),
            EpochCheck::Behind.direction()
        );
    }
}
