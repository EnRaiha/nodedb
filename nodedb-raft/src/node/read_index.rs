// SPDX-License-Identifier: BUSL-1.1

//! Linearizable reads on the leader.
//!
//! A node that believes it is the leader may already have been deposed: a
//! partition does not notify the old leader. Serving a read on that belief
//! returns state the new leader has since moved past. The read index is
//! therefore confirmed against a quorum before it is served.

use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

/// An in-flight linearizable read, taken by [`RaftNode::start_read_index`].
///
/// Opaque on purpose: the ack counts inside are meaningful only to the node
/// that issued them, in the term that issued them.
#[derive(Debug, Clone)]
pub struct ReadIndexProbe {
    /// Index the read may be served at once confirmed.
    pub read_index: u64,
    term: u64,
    acks: Vec<(u64, u64)>,
}

impl<S: LogStorage> RaftNode<S> {
    /// Begin a linearizable read, or return `None` if this node is not the
    /// leader. Confirm with [`Self::read_index_confirmed`] before serving.
    pub fn start_read_index(&self) -> Option<ReadIndexProbe> {
        if self.role != NodeRole::Leader {
            return None;
        }
        let leader = self.leader_state.as_ref()?;
        Some(ReadIndexProbe {
            read_index: self.volatile.commit_index,
            term: self.hard_state.current_term,
            acks: self
                .config
                .peers
                .iter()
                .map(|&id| (id, leader.ack_count_for(id)))
                .collect(),
        })
    }

    /// Whether a quorum has responded since `probe` was taken.
    ///
    /// Every voter counted answered at least once after the probe, so a
    /// quorum still recognised this node as leader after the read index was
    /// chosen. False once the term moves or leadership is lost.
    pub fn read_index_confirmed(&self, probe: &ReadIndexProbe) -> bool {
        if self.role != NodeRole::Leader || self.hard_state.current_term != probe.term {
            return false;
        }
        let leader = match self.leader_state.as_ref() {
            Some(ls) => ls,
            None => return false,
        };
        let mut count = 1u64; // self counts.
        for &(peer, seen) in &probe.acks {
            if leader.ack_count_for(peer) > seen {
                count += 1;
            }
        }
        count as usize >= self.config.quorum()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::message::{AppendEntriesResponse, RequestVoteResponse};
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;

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

    /// Node 1 leading a 3-voter group (peers 2 and 3), so quorum is 2 and one
    /// peer response confirms.
    fn leader() -> RaftNode<MemStorage> {
        let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        let _ = node.take_ready();
        node.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();
        node
    }

    fn ack(node: &mut RaftNode<MemStorage>, peer: u64, success: bool) {
        node.handle_append_entries_response(
            peer,
            &AppendEntriesResponse {
                term: node.current_term(),
                success,
                last_log_index: node.last_log_index(),
            },
        );
    }

    #[test]
    fn a_follower_cannot_start_a_read() {
        let node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());
        assert!(node.start_read_index().is_none());
    }

    /// The whole point: holding a probe is not permission to serve it.
    #[test]
    fn a_fresh_probe_is_not_confirmed() {
        let node = leader();
        let probe = node.start_read_index().expect("leader starts a read");
        assert!(
            !node.read_index_confirmed(&probe),
            "a read must not be served before any peer has answered"
        );
    }

    #[test]
    fn one_peer_response_confirms_a_three_voter_quorum() {
        let mut node = leader();
        let probe = node.start_read_index().expect("leader starts a read");
        ack(&mut node, 2, true);
        assert!(node.read_index_confirmed(&probe));
    }

    /// A rejection still proves the peer recognises this term, which is what
    /// the leadership check needs — so it counts.
    #[test]
    fn a_rejected_append_still_confirms_leadership() {
        let mut node = leader();
        let probe = node.start_read_index().expect("leader starts a read");
        ack(&mut node, 2, false);
        assert!(node.read_index_confirmed(&probe));
    }

    /// Responses that arrived *before* the probe prove nothing about now.
    #[test]
    fn responses_predating_the_probe_do_not_confirm() {
        let mut node = leader();
        ack(&mut node, 2, true);
        ack(&mut node, 3, true);

        let probe = node.start_read_index().expect("leader starts a read");
        assert!(
            !node.read_index_confirmed(&probe),
            "acks banked before the probe must not satisfy it"
        );
    }

    /// The case the mechanism exists for: a deposed leader holds a probe it
    /// can never confirm, because it is no longer the leader.
    #[test]
    fn a_deposed_leader_cannot_confirm_its_read() {
        let mut node = leader();
        let probe = node.start_read_index().expect("leader starts a read");

        // A higher term arrives — this node is no longer the leader.
        ack(&mut node, 2, true);
        node.handle_append_entries_response(
            3,
            &AppendEntriesResponse {
                term: node.current_term() + 1,
                success: false,
                last_log_index: 0,
            },
        );

        assert_ne!(node.role(), NodeRole::Leader);
        assert!(
            !node.read_index_confirmed(&probe),
            "a node that lost leadership must not serve the read it started"
        );
    }

    /// A single-voter group has no peers to hear from, so the leader alone is
    /// the quorum and the read confirms immediately.
    #[test]
    fn a_single_voter_leader_confirms_alone() {
        let mut node = RaftNode::new(config(1, vec![]), MemStorage::new());
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);

        let probe = node.start_read_index().expect("leader starts a read");
        assert!(node.read_index_confirmed(&probe));
    }

    /// The read index is the commit index at probe time, not whatever the log
    /// has reached since.
    #[test]
    fn the_probe_pins_the_commit_index_it_was_taken_at() {
        let node = leader();
        let probe = node.start_read_index().expect("leader starts a read");
        assert_eq!(probe.read_index, node.commit_index());
    }
}
