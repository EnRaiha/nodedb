// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for the Calvin sequencer's silent data-loss reports.
//!
//! Grouping keys deliberately carry no per-occurrence value (an epoch, a
//! Raft index, a transaction position) — those identify the *occurrence*,
//! and reports group by the *bug*, so a sustained gap or a backpressure
//! storm files one report with a growing occurrence count rather than one
//! directory per hit.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// Round `n` down to a power of two, so a magnitude can enter a grouping key
/// without the exact value doing so.
fn magnitude_bucket(n: u64) -> u64 {
    if n == 0 {
        0
    } else {
        1u64 << (u64::BITS - 1 - n.leading_zeros())
    }
}

/// A break in the sequencer's epoch sequence: `apply` received an epoch that
/// was not the one `last_applied_epoch` required.
///
/// `direction` names which way the two views diverged, and the two directions
/// are different bugs. `"ahead"`: entries are missing on this replica, so the
/// epochs between the two were never fanned out here (the arriving batch itself
/// still is). `"behind"`: an already-consumed epoch was proposed a second time,
/// so its transactions carry identities that alias committed history — the
/// state machine halts rather than apply or drop them.
pub(super) struct SequencerEpochGap {
    pub epoch_expected: u64,
    pub epoch_found: u64,
    pub gap: u64,
    pub direction: &'static str,
    pub txns_in_batch: usize,
    pub raft_index: u64,
}

impl DomainContext for SequencerEpochGap {
    fn domain_kind(&self) -> &'static str {
        "nodedb_cluster.sequencer_epoch_gap"
    }

    fn grouping_key(&self) -> String {
        // Direction names the bug — a replica that fell behind and a leader
        // that re-minted a committed epoch have nothing in common. How many
        // epochs the break spans then separates one skipped entry from a
        // replica far adrift; the exact epoch numbers are the occurrence and
        // would split one bug into one group per hit.
        format!("{}~{}", self.direction, magnitude_bucket(self.gap))
    }

    fn to_json(&self) -> Value {
        json!({
            "epoch_expected": self.epoch_expected,
            "epoch_found": self.epoch_found,
            "gap": self.gap,
            "direction": self.direction,
            "txns_in_batch": self.txns_in_batch,
            "raft_index": self.raft_index,
            "why_fatal": "'ahead': the epochs between expected and found were never fanned \
                          out on this replica, so any completion waiter registered for one \
                          of their positions never resolves until its own deadline elapses. \
                          'behind': the arriving batch re-uses an epoch already consumed \
                          here, so its (epoch, position) identities alias committed ones — \
                          the state machine halts instead of aliasing live lock-table and \
                          completion entries or silently discarding committed writes",
            "operator_action": "'ahead': the scheduler must replay the sequencer Raft log to \
                                 recover the skipped epochs' transactions; check why this \
                                 replica missed entries between epoch_expected and \
                                 epoch_found. 'behind': a leader minted an epoch at or below \
                                 committed history — check that its epoch seed was derived \
                                 from a fully replayed sequencer log before it proposed",
        })
    }
}

/// One transaction copy dropped while fanning an epoch batch out to a
/// vshard's scheduler channel.
pub(super) struct DroppedTxn {
    pub vshard: u32,
    pub cause: &'static str,
}

/// Transactions dropped during a single `apply` call's fan-out loop because
/// the destination vshard channel was full or its receiver was gone.
pub(super) struct SequencerBackpressureDrop<'a> {
    pub epoch: u64,
    pub dropped_count: u64,
    pub drops: &'a [DroppedTxn],
}

impl DomainContext for SequencerBackpressureDrop<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb_cluster.sequencer_backpressure_drop"
    }

    fn grouping_key(&self) -> String {
        // Which vshard and which failure mode (channel full vs. sender gone)
        // names the bug — a saturated scheduler and an exited one call for
        // different operator action. The epoch and per-txn positions are the
        // occurrence and are excluded so a sustained storm collapses into
        // one growing group instead of one per apply() call.
        let mut pairs: Vec<String> = self
            .drops
            .iter()
            .map(|d| format!("{}:{}", d.vshard, d.cause))
            .collect();
        pairs.sort_unstable();
        pairs.dedup();
        pairs.join(",")
    }

    fn to_json(&self) -> Value {
        let drops: Vec<Value> = self
            .drops
            .iter()
            .map(|d| json!({"vshard": d.vshard, "cause": d.cause}))
            .collect();
        json!({
            "epoch": self.epoch,
            "dropped_count": self.dropped_count,
            "drops": drops,
            "why_fatal": "seed_expected already registered a completion waiter for every \
                          position in this epoch before the fan-out loop ran, so a txn \
                          dropped here leaves that waiter with no vote that will ever \
                          arrive; the caller only sees a generic completion timeout with \
                          no indication that backpressure, not a lost vote, caused it",
            "operator_action": "a 'full' cause means the named vshard's scheduler is not \
                                 keeping up with committed epochs and needs more capacity \
                                 or a slower ingest rate; a 'closed' cause means the \
                                 scheduler task exited and the sequencer has no live \
                                 channel for that vshard. Either way the scheduler's \
                                 log-replay path must catch this vshard up from the Raft \
                                 log",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_gap_grouping_ignores_the_exact_epochs() {
        let near = SequencerEpochGap {
            epoch_expected: 10,
            epoch_found: 12,
            gap: 2,
            direction: "ahead",
            txns_in_batch: 5,
            raft_index: 100,
        };
        let far = SequencerEpochGap {
            epoch_expected: 9000,
            epoch_found: 9002,
            gap: 2,
            direction: "ahead",
            txns_in_batch: 40,
            raft_index: 90_000,
        };
        assert_eq!(near.grouping_key(), far.grouping_key());
    }

    /// A replica that fell behind and a leader that re-minted a committed epoch
    /// are different bugs with different fixes, so they must never share a group
    /// even when the magnitude of the break happens to match.
    #[test]
    fn epoch_gap_grouping_separates_the_two_divergence_directions() {
        let ahead = SequencerEpochGap {
            epoch_expected: 10,
            epoch_found: 12,
            gap: 2,
            direction: "ahead",
            txns_in_batch: 5,
            raft_index: 100,
        };
        let behind = SequencerEpochGap {
            epoch_expected: 12,
            epoch_found: 10,
            gap: 2,
            direction: "behind",
            txns_in_batch: 5,
            raft_index: 100,
        };
        assert_ne!(ahead.grouping_key(), behind.grouping_key());
    }

    #[test]
    fn backpressure_grouping_ignores_epoch_and_dedups_pairs() {
        let a = SequencerBackpressureDrop {
            epoch: 1,
            dropped_count: 2,
            drops: &[
                DroppedTxn {
                    vshard: 3,
                    cause: "full",
                },
                DroppedTxn {
                    vshard: 3,
                    cause: "full",
                },
            ],
        };
        let b = SequencerBackpressureDrop {
            epoch: 9000,
            dropped_count: 40,
            drops: &[DroppedTxn {
                vshard: 3,
                cause: "full",
            }],
        };
        assert_eq!(a.grouping_key(), b.grouping_key());
        assert_eq!(a.grouping_key(), "3:full");
    }
}
