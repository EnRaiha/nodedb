// SPDX-License-Identifier: BUSL-1.1

//! What happened to a `CatalogEntry` handed to the metadata proposer.
//!
//! These three outcomes demand three different follow-ups from the caller and
//! must never be conflated: a bare log index cannot distinguish "nothing was
//! replicated, write the catalog yourself" from "held for COMMIT, touch
//! nothing", and doing the former for the latter durably leaks rolled-back DDL.

/// Result of proposing one `CatalogEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeOutcome {
    /// Replicated through the metadata raft group and applied locally at this
    /// log index. The applier has already written the catalog.
    Replicated { log_index: u64 },
    /// Captured by the connection's DDL transaction buffer. Nothing is durable
    /// yet and nothing may be applied: COMMIT proposes the whole batch,
    /// ROLLBACK discards it.
    Buffered,
    /// Nothing was replicated — no metadata raft group (single node) or
    /// rolling-upgrade compat mode. The caller owns the catalog write and any
    /// local side effects.
    LocalOnly,
}

impl ProposeOutcome {
    /// True when the caller must write the catalog and run its local side
    /// effects itself. False for both `Replicated` and `Buffered`.
    pub fn needs_local_apply(self) -> bool {
        matches!(self, Self::LocalOnly)
    }

    /// True when a raft applier has already landed the entry on this node.
    pub fn is_replicated(self) -> bool {
        matches!(self, Self::Replicated { .. })
    }

    /// True when the entry is held for COMMIT and no side effect may run yet.
    pub fn is_buffered(self) -> bool {
        matches!(self, Self::Buffered)
    }

    /// The replicated log index, or 0 when nothing was replicated. Use only
    /// for logging and for wire fields that already carry 0 as "not
    /// replicated"; never to decide whether to apply locally.
    pub fn log_index(self) -> u64 {
        match self {
            Self::Replicated { log_index } => log_index,
            Self::Buffered | Self::LocalOnly => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_local_only_applies_locally() {
        assert!(ProposeOutcome::LocalOnly.needs_local_apply());
        assert!(!ProposeOutcome::Buffered.needs_local_apply());
        assert!(!ProposeOutcome::Replicated { log_index: 7 }.needs_local_apply());
    }

    #[test]
    fn log_index_is_zero_for_non_replicated() {
        assert_eq!(ProposeOutcome::Replicated { log_index: 7 }.log_index(), 7);
        assert_eq!(ProposeOutcome::Buffered.log_index(), 0);
        assert_eq!(ProposeOutcome::LocalOnly.log_index(), 0);
    }

    #[test]
    fn predicates_are_mutually_exclusive() {
        for outcome in [
            ProposeOutcome::Replicated { log_index: 1 },
            ProposeOutcome::Buffered,
            ProposeOutcome::LocalOnly,
        ] {
            let set = [
                outcome.is_replicated(),
                outcome.is_buffered(),
                outcome.needs_local_apply(),
            ];
            assert_eq!(set.iter().filter(|flag| **flag).count(), 1);
        }
    }
}
