// SPDX-License-Identifier: BUSL-1.1

//! Encoding of an aggregated commit/abort decision into the sequencer log entry
//! the leader proposes for it.

use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::{TxnId, VerdictOutcome};

/// Encode a decision as the entry the leader proposes. An abort takes the
/// reason-carrying `AbortVerdict`, so the coordinator reports the actual cause.
/// A reasonless abort — a pre-existing vote re-tallied after failover — keeps
/// the original `Verdict` shape.
pub(crate) fn verdict_entry(txn: TxnId, outcome: VerdictOutcome) -> SequencerEntry {
    match outcome {
        VerdictOutcome::Commit => SequencerEntry::Verdict {
            epoch: txn.epoch,
            position: txn.position,
            commit: true,
        },
        VerdictOutcome::Abort(Some(reason)) => SequencerEntry::AbortVerdict {
            epoch: txn.epoch,
            position: txn.position,
            reason,
        },
        VerdictOutcome::Abort(None) => SequencerEntry::Verdict {
            epoch: txn.epoch,
            position: txn.position,
            commit: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::AbortReason;

    #[test]
    fn abort_with_a_reason_proposes_abort_verdict() {
        let entry = verdict_entry(
            TxnId::new(4, 1),
            VerdictOutcome::Abort(Some(AbortReason::ParticipantError)),
        );
        assert_eq!(
            entry,
            SequencerEntry::AbortVerdict {
                epoch: 4,
                position: 1,
                reason: AbortReason::ParticipantError,
            }
        );
    }

    #[test]
    fn commit_and_reasonless_abort_keep_the_original_verdict_shape() {
        assert_eq!(
            verdict_entry(TxnId::new(4, 2), VerdictOutcome::Commit),
            SequencerEntry::Verdict {
                epoch: 4,
                position: 2,
                commit: true,
            }
        );
        assert_eq!(
            verdict_entry(TxnId::new(4, 3), VerdictOutcome::Abort(None)),
            SequencerEntry::Verdict {
                epoch: 4,
                position: 3,
                commit: false,
            }
        );
    }
}
