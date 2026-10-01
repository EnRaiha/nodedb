// SPDX-License-Identifier: BUSL-1.1

//! What happened to a `CatalogEntry` handed to the metadata proposer.
//!
//! A caller never writes the catalog itself. A replicated entry already ran
//! both post-apply lanes on this node. A buffered one must touch nothing:
//! applying it durably leaks rolled-back DDL.

/// Result of proposing one `CatalogEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeOutcome {
    /// Replicated through the metadata raft group and applied locally at this
    /// log index. The applier has already written the catalog.
    Replicated { log_index: u64 },
    /// Captured by the connection's DDL transaction buffer. Nothing is durable
    /// yet and nothing is applied: COMMIT proposes the whole batch,
    /// ROLLBACK discards it.
    Buffered,
}

impl ProposeOutcome {
    /// True when a raft applier has already landed the entry on this node.
    pub fn is_replicated(self) -> bool {
        matches!(self, Self::Replicated { .. })
    }

    /// True when the entry is held for COMMIT and no side effect runs yet.
    pub fn is_buffered(self) -> bool {
        matches!(self, Self::Buffered)
    }

    /// True when the entry landed on this node.
    pub fn is_durable(self) -> bool {
        !self.is_buffered()
    }

    /// The replicated log index, or 0 for a buffered entry. Use only for
    /// logging and for wire fields that already carry 0 as "not replicated".
    pub fn log_index(self) -> u64 {
        match self {
            Self::Replicated { log_index } => log_index,
            Self::Buffered => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_buffered_is_not_durable() {
        assert!(!ProposeOutcome::Buffered.is_durable());
        assert!(ProposeOutcome::Replicated { log_index: 7 }.is_durable());
    }

    #[test]
    fn log_index_is_zero_for_buffered() {
        assert_eq!(ProposeOutcome::Replicated { log_index: 7 }.log_index(), 7);
        assert_eq!(ProposeOutcome::Buffered.log_index(), 0);
    }
}
