// SPDX-License-Identifier: BUSL-1.1

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Read consistency level for queries.
///
/// Controls the trade-off between freshness and latency. Every query carries
/// a consistency level that determines how the read is routed and which
/// snapshot watermark is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ReadConsistency {
    /// Leader-read after commit index >= required LSN.
    /// Default for metadata, constraints, and transactional reads.
    #[default]
    Strong,

    /// Follower read allowed if replication lag <= the specified duration.
    /// Good for dashboards, analytics, and read-heavy workloads that
    /// tolerate bounded staleness.
    BoundedStaleness(Duration),

    /// Local read for CRDT edge sync where monotonic convergence is acceptable.
    /// Fastest option — reads from local replica without any freshness check.
    Eventual,
}

impl ReadConsistency {
    /// Returns true if this level requires reading from the Raft leader.
    pub fn requires_leader(&self) -> bool {
        matches!(self, ReadConsistency::Strong)
    }

    /// Maximum acceptable staleness. `None` for Strong (must be fresh)
    /// and Eventual (any staleness accepted).
    pub fn max_staleness(&self) -> Option<Duration> {
        match self {
            ReadConsistency::BoundedStaleness(d) => Some(*d),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strong_requires_leader() {
        assert!(ReadConsistency::Strong.requires_leader());
    }

    #[test]
    fn bounded_staleness_does_not_require_leader() {
        let bs = ReadConsistency::BoundedStaleness(Duration::from_secs(5));
        assert!(!bs.requires_leader());
        assert_eq!(bs.max_staleness(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn eventual_does_not_require_leader() {
        assert!(!ReadConsistency::Eventual.requires_leader());
        assert_eq!(ReadConsistency::Eventual.max_staleness(), None);
    }

    #[test]
    fn default_is_strong() {
        assert_eq!(ReadConsistency::default(), ReadConsistency::Strong);
    }
}
