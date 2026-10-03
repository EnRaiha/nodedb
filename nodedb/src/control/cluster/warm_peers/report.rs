// SPDX-License-Identifier: BUSL-1.1

//! Report returned by [`super::warm_known_peers`].
//!
//! Logged at INFO regardless of outcome so operators see
//! `peer_warm: 3 succeeded, 1 failed in 412ms` in the
//! startup log without grepping for individual lines.

use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PeerWarmReport {
    /// Number of peers attempted (everything in the topology
    /// except this node).
    pub attempted: usize,
    /// Node ids that successfully warmed.
    pub succeeded: Vec<u64>,
    /// Per-peer failure with the underlying transport error
    /// stringified — kept as String because the cluster
    /// transport returns its own typed error which we don't
    /// want to leak into this struct's API surface.
    pub failed: Vec<(u64, String)>,
    /// Total wall-clock for the whole warm phase. Includes
    /// the parallel-dial time, NOT the sum of individual
    /// per-peer dials.
    pub elapsed: Duration,
}

impl PeerWarmReport {
    /// Whether at least one peer was warmed. Useful for
    /// tests; the production path logs the full report
    /// regardless.
    pub fn is_success(&self) -> bool {
        !self.succeeded.is_empty()
    }

    /// Whether every attempted peer was reached.
    pub fn is_complete(&self) -> bool {
        self.failed.is_empty() && self.succeeded.len() == self.attempted
    }

    /// Empty report — used when the topology has no peers.
    pub fn empty() -> Self {
        Self {
            attempted: 0,
            succeeded: Vec::new(),
            failed: Vec::new(),
            elapsed: Duration::ZERO,
        }
    }
}

impl fmt::Display for PeerWarmReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "peer_warm: {}/{} succeeded ({} failed) in {:?}",
            self.succeeded.len(),
            self.attempted,
            self.failed.len(),
            self.elapsed
        )?;
        for (id, err) in &self.failed {
            write!(f, "\n  node {id}: {err}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_report_is_complete_and_unsuccessful() {
        let r = PeerWarmReport::empty();
        assert!(r.is_complete());
        assert!(!r.is_success());
    }

    #[test]
    fn complete_report() {
        let r = PeerWarmReport {
            attempted: 2,
            succeeded: vec![1, 2],
            failed: vec![],
            elapsed: Duration::from_millis(50),
        };
        assert!(r.is_complete());
        assert!(r.is_success());
    }

    #[test]
    fn partial_failure_not_complete() {
        let r = PeerWarmReport {
            attempted: 3,
            succeeded: vec![1],
            failed: vec![(2, "timeout".into()), (3, "refused".into())],
            elapsed: Duration::from_millis(2000),
        };
        assert!(!r.is_complete());
        assert!(r.is_success());
    }

    #[test]
    fn display_includes_failures() {
        let r = PeerWarmReport {
            attempted: 2,
            succeeded: vec![1],
            failed: vec![(2, "connection refused".into())],
            elapsed: Duration::from_millis(100),
        };
        let s = r.to_string();
        assert!(s.contains("1/2 succeeded"));
        assert!(s.contains("node 2"));
        assert!(s.contains("connection refused"));
    }
}
