// SPDX-License-Identifier: BUSL-1.1

//! How far behind the leader this replica is.
//!
//! A bounded-staleness read promises the caller that what it reads is at
//! most `max_staleness` old. Answering that needs two facts, not one: how
//! much of the leader's committed log this node has applied, and how long
//! ago the leader last said what that was.

use std::time::Duration;

use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

/// Why a replica cannot serve a bounded-staleness read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StalenessVerdict {
    /// Within the requested bound.
    Fresh,
    /// The leader has not been heard from within the bound, so how far
    /// behind this node is right now is unknown.
    NoRecentContact,
    /// Entries the leader had already committed are not applied here yet.
    Behind {
        /// Leader's commit index at last contact.
        leader_commit: u64,
        /// What this node has applied.
        applied: u64,
    },
}

impl<S: LogStorage> RaftNode<S> {
    /// Whether this replica is within `max_staleness` of the leader.
    ///
    /// The leader is always fresh — it is the source of the bound, not a
    /// consumer of it. A follower is fresh when it has applied everything
    /// the leader had committed as of a contact no older than the bound:
    /// anything committed since is at most `max_staleness` old, which is
    /// precisely what was promised.
    pub fn staleness_verdict(&self, max_staleness: Duration) -> StalenessVerdict {
        if self.role == NodeRole::Leader {
            return StalenessVerdict::Fresh;
        }
        let Some(contact) = self.leader_contact else {
            return StalenessVerdict::NoRecentContact;
        };
        if contact.at.elapsed() > max_staleness {
            return StalenessVerdict::NoRecentContact;
        }
        if self.volatile.last_applied < contact.leader_commit {
            return StalenessVerdict::Behind {
                leader_commit: contact.leader_commit,
                applied: self.volatile.last_applied,
            };
        }
        StalenessVerdict::Fresh
    }

    /// Whether a read with `max_staleness` can be served from this replica.
    pub fn within_staleness_bound(&self, max_staleness: Duration) -> bool {
        self.staleness_verdict(max_staleness) == StalenessVerdict::Fresh
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::StalenessVerdict;
    use crate::message::AppendEntriesRequest;
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;

    const BOUND: Duration = Duration::from_secs(5);

    fn config(node_id: u64, peers: Vec<u64>) -> RaftConfig {
        RaftConfig {
            node_id,
            group_id: 1,
            peers,
            learners: vec![],
            observers: vec![],
            starts_as_learner: false,
            starts_as_observer: false,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            log_compaction_threshold: None,
        }
    }

    fn follower() -> RaftNode<MemStorage> {
        RaftNode::new(config(1, vec![2, 3]), MemStorage::new())
    }

    /// Deliver a heartbeat from node 2 announcing `leader_commit`.
    fn heartbeat(node: &mut RaftNode<MemStorage>, leader_commit: u64) {
        node.handle_append_entries(&AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit,
            group_id: 1,
        });
    }

    #[test]
    fn a_replica_that_never_heard_from_a_leader_is_not_fresh() {
        let node = follower();
        assert_eq!(
            node.staleness_verdict(BOUND),
            StalenessVerdict::NoRecentContact
        );
    }

    #[test]
    fn a_caught_up_replica_in_recent_contact_is_fresh() {
        let mut node = follower();
        heartbeat(&mut node, 0);
        assert_eq!(node.staleness_verdict(BOUND), StalenessVerdict::Fresh);
    }

    /// The failure the old measure could not see: applying steadily while
    /// far behind the leader looked fresh, because only the time since the
    /// last local apply was checked.
    #[test]
    fn a_replica_behind_the_leader_is_not_fresh_however_recent_the_contact() {
        let mut node = follower();
        heartbeat(&mut node, 500);
        node.advance_applied(10);
        assert_eq!(
            node.staleness_verdict(BOUND),
            StalenessVerdict::Behind {
                leader_commit: 500,
                applied: 10,
            }
        );
    }

    /// The other direction: an idle cluster writes nothing, so a caught-up
    /// replica applies nothing. Heartbeats still prove it is current.
    #[test]
    fn an_idle_cluster_does_not_make_a_caught_up_replica_look_stale() {
        let mut node = follower();
        heartbeat(&mut node, 7);
        node.advance_applied(7);
        assert_eq!(node.staleness_verdict(BOUND), StalenessVerdict::Fresh);
    }

    #[test]
    fn contact_older_than_the_bound_is_not_fresh() {
        let mut node = follower();
        heartbeat(&mut node, 0);
        node.leader_contact_at_override(Instant::now() - Duration::from_secs(30));
        assert_eq!(
            node.staleness_verdict(BOUND),
            StalenessVerdict::NoRecentContact
        );
    }

    /// The leader is the source of the bound, so it needs no contact of its
    /// own to satisfy one.
    #[test]
    fn a_leader_is_always_within_the_bound() {
        let mut node = RaftNode::new(config(1, vec![]), MemStorage::new());
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);
        assert_eq!(node.staleness_verdict(BOUND), StalenessVerdict::Fresh);
    }
}
