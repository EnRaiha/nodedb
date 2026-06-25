// SPDX-License-Identifier: BUSL-1.1

//! Configuration for a single Raft group on this node.
//!
//! A Raft group has two kinds of peer membership:
//!
//! - **Voters** (`peers`): full members that participate in leader election
//!   and count toward the commit quorum.
//! - **Learners** (`learners`): non-voting members that receive replicated
//!   log entries but do not vote in elections and do not count toward the
//!   commit quorum. Learners exist so a joining node can catch up to the
//!   leader's log before being promoted to a voter — the standard Raft
//!   single-server conf-change safety pattern.
//!
//! `self.config.node_id` is never in either list; the node is implicitly
//! its own member. `starts_as_learner` controls whether this node boots as
//! a `Learner` role — set by the joining path when the group is created
//! from a `JoinResponse` that assigned this node as a learner.

use std::time::Duration;

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's ID (must be unique within the Raft group).
    pub node_id: u64,
    /// Raft group ID (for Multi-Raft routing).
    pub group_id: u64,
    /// IDs of voting peers in this group (excluding self).
    pub peers: Vec<u64>,
    /// IDs of non-voting learner peers in this group (excluding self).
    ///
    /// Learners receive log replication but do not vote in elections and
    /// are not counted in the commit quorum. They are promoted to voters
    /// once they catch up — see `RaftNode::promote_learner`.
    pub learners: Vec<u64>,
    /// IDs of cross-cluster observer peers tracked by this leader.
    ///
    /// Observers receive log entries and send advisory acks, but they never
    /// participate in leader election and are never counted in the commit
    /// quorum. A slow observer does not stall source commits.
    pub observers: Vec<u64>,
    /// Whether this node itself starts in the `Learner` role (boot-time).
    ///
    /// Set `true` when a new node joins an existing cluster and is
    /// created as a learner for a given group; cleared when the node is
    /// promoted to voter via `promote_self_to_voter`.
    pub starts_as_learner: bool,
    /// Whether this node itself starts in the `Observer` role (boot-time).
    ///
    /// Set `true` when this node is a cross-cluster mirror replica observing
    /// a source cluster's Raft group. An observer never participates in
    /// elections and never contributes to the commit quorum. Acks it sends
    /// to the source leader are advisory only.
    pub starts_as_observer: bool,
    /// Minimum election timeout.
    pub election_timeout_min: Duration,
    /// Maximum election timeout.
    pub election_timeout_max: Duration,
    /// Heartbeat interval (must be << election_timeout_min).
    pub heartbeat_interval: Duration,
    /// Number of log entries to retain past `snapshot_index` before the
    /// node auto-compacts its Raft log.
    ///
    /// When `Some(t)`, after the DATA-PLANE state machine has durably
    /// applied an entry at index `I`, the node compacts its log up to `I`
    /// once `I - snapshot_index >= t`. Compaction discards entries
    /// `<= I`; thereafter a lagging or freshly-joined follower whose
    /// `next_index` falls behind the new `snapshot_index` is brought up
    /// to date via an `InstallSnapshot` (rebuilt on demand by the
    /// `SnapshotBuilder` hook from live engine state) rather than log
    /// replay.
    ///
    /// `None` (the default) disables auto-compaction entirely — the log
    /// grows until an explicit snapshot/compaction is triggered
    /// elsewhere. This preserves the historical behavior exactly.
    ///
    /// The trigger MUST be gated on the data-plane applied watermark, not
    /// raft's commit index: compacting past an index the state machine
    /// has not yet applied would let the `SnapshotBuilder` serialize
    /// incomplete state and corrupt a follower's snapshot.
    pub log_compaction_threshold: Option<u64>,
}

impl RaftConfig {
    /// Total number of voters (self + voter peers).
    ///
    /// Learners are excluded. This value drives quorum math and so must
    /// never grow transiently while the learner is catching up — that is
    /// exactly the safety property the learner phase is designed to give.
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Quorum size: `floor(n/2) + 1` over the voter set.
    pub fn quorum(&self) -> usize {
        self.cluster_size() / 2 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(peers: Vec<u64>, learners: Vec<u64>) -> RaftConfig {
        RaftConfig {
            node_id: 1,
            group_id: 0,
            peers,
            learners,
            observers: vec![],
            starts_as_learner: false,
            starts_as_observer: false,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            log_compaction_threshold: None,
        }
    }

    #[test]
    fn quorum_excludes_learners() {
        // Single voter (self), two learners catching up → quorum is still 1.
        let c = cfg(vec![], vec![2, 3]);
        assert_eq!(c.cluster_size(), 1);
        assert_eq!(c.quorum(), 1);

        // Three voters + one learner → quorum is 2 (not 3).
        let c = cfg(vec![2, 3], vec![4]);
        assert_eq!(c.cluster_size(), 3);
        assert_eq!(c.quorum(), 2);

        // Five voters + two learners → quorum is 3.
        let c = cfg(vec![2, 3, 4, 5], vec![6, 7]);
        assert_eq!(c.cluster_size(), 5);
        assert_eq!(c.quorum(), 3);
    }
}
