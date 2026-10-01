// SPDX-License-Identifier: BUSL-1.1

//! Read-only views of contact with peers: the leader's per-peer response
//! counts, and whether the leader this node names is live.

use std::time::Instant;

use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// `AppendEntries` responses received from `peer` in the current term,
    /// rejections included. `None` when this node is not the leader.
    pub fn peer_ack_count(&self, peer: u64) -> Option<u64> {
        self.leader_state
            .as_ref()
            .map(|leader| leader.ack_count_for(peer))
    }

    /// The highest log index every voter's log is known to hold. A log
    /// compacted at or below it never sends a voter a snapshot, under this
    /// leader or a later one: every voter holds the compacted entries.
    ///
    /// The leader takes the lowest `match_index` over the other voters,
    /// capped at its commit index: a committed entry stays in every log that
    /// holds it. It sends the value with each `AppendEntries`. Every replica
    /// keeps the highest value it saw, so the floor never moves back. A voter
    /// that stops answering holds it where it is. Learners and observers do
    /// not count: a new replica takes a snapshot.
    pub fn replicated_floor(&self) -> u64 {
        let Some(leader) = self.leader_state.as_ref() else {
            return self.replicated_floor;
        };
        let lowest_voter = self
            .config
            .peers
            .iter()
            .map(|&peer| leader.match_index_for(peer))
            .min()
            .unwrap_or(u64::MAX);
        self.replicated_floor
            .max(lowest_voter.min(self.volatile.commit_index))
    }

    /// Whether `peer`'s latest response to this leader asked for a snapshot.
    /// Such a peer holds no state to lead from. `false` off the leader.
    pub fn peer_awaits_snapshot(&self, peer: u64) -> bool {
        self.leader_state
            .as_ref()
            .is_some_and(|leader| leader.awaiting_snapshot.contains(&peer))
    }

    /// Whether this node leads by an election against other voters that no
    /// leadership transfer started: a failover or a partition heal. A
    /// bootstrap self-election and a transfer target's win are not.
    pub fn leads_by_contested_election(&self) -> bool {
        self.role == NodeRole::Leader && self.contested_win
    }

    /// The leader this node names at `now`, when it has proof the leader is
    /// live: this node leads, or the leader reached it within
    /// `election_timeout_min`. `0` otherwise.
    ///
    /// A follower keeps naming a crashed leader until its election timeout
    /// fires. This view drops that leader as soon as its contact goes stale.
    pub fn live_leader(&self, now: Instant) -> u64 {
        if self.role == NodeRole::Leader {
            return self.config.node_id;
        }
        if self.leader_id == 0 {
            return 0;
        }
        let fresh = self.leader_contact.is_some_and(|contact| {
            now.saturating_duration_since(contact.at) < self.config.election_timeout_min
        });
        if fresh { self.leader_id } else { 0 }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::message::{AppendEntriesRequest, RequestVoteResponse};
    use crate::node::core::RaftNode;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;
    use crate::test_support::{force_election, test_config};

    fn heartbeat(term: u64, leader_id: u64) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
            round: 1,
            replicated_floor: 0,
        }
    }

    /// A leadership transfer moves the term before the new leader reaches
    /// this node. In between, this node names no leader: never the old
    /// leader at the new term, which a routing hint at that term would keep.
    #[test]
    fn a_transfer_campaign_leaves_no_leader_named_at_its_term_until_the_winner_reaches_this_node() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        let _ = node.handle_append_entries(&heartbeat(1, 1));
        let now = std::time::Instant::now();
        assert_eq!((node.live_leader(now), node.current_term()), (1, 1));

        // Node 3's transfer campaign at term 2.
        let vote = node.handle_request_vote(&crate::message::RequestVoteRequest {
            term: 2,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
            transfer: true,
        });
        assert!(vote.vote_granted);
        assert_eq!(node.current_term(), 2);
        assert_eq!(node.leader_id(), 0, "the old leader does not lead term 2");
        assert_eq!(node.live_leader(std::time::Instant::now()), 0);

        let _ = node.handle_append_entries(&heartbeat(2, 3));
        let now = std::time::Instant::now();
        assert_eq!((node.live_leader(now), node.current_term()), (3, 2));
    }

    /// A leader that steps down for lost quorum names no leader.
    #[test]
    fn a_leader_that_steps_down_names_no_leader() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        force_election(&mut node);
        let _ = node.take_ready();
        node.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(node.leader_id(), 1);
        node.become_follower(node.current_term());
        assert_eq!(node.leader_id(), 0);
        assert_eq!(node.live_leader(std::time::Instant::now()), 0);
    }

    #[test]
    fn an_election_against_peers_is_contested_and_a_lone_or_transfer_win_is_not() {
        let mut lone = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        force_election(&mut lone);
        assert_eq!(lone.role(), NodeRole::Leader);
        assert!(!lone.leads_by_contested_election());

        let mut contested = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        force_election(&mut contested);
        let _ = contested.take_ready();
        contested.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(contested.role(), NodeRole::Leader);
        assert!(contested.leads_by_contested_election());

        let mut target = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        target.transfer_campaign_term = 1;
        force_election(&mut target);
        let _ = target.take_ready();
        target.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(target.role(), NodeRole::Leader);
        assert!(!target.leads_by_contested_election());
    }

    /// The leader's floor is the lowest voter's committed match. A voter
    /// that has not answered holds it at 0.
    #[test]
    fn the_replicated_floor_waits_for_every_voter() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        force_election(&mut node);
        let _ = node.take_ready();
        node.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(node.role(), NodeRole::Leader);
        let ack = crate::message::AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: 1,
            round: 1,
            needs_snapshot: false,
        };
        node.handle_append_entries_response(2, &ack);
        assert_eq!(node.replicated_floor(), 0, "voter 3 holds nothing yet");
        node.handle_append_entries_response(3, &ack);
        assert_eq!(node.commit_index(), 1);
        assert_eq!(node.replicated_floor(), 1);
    }

    /// A follower keeps the highest floor a leader sent it.
    #[test]
    fn a_follower_keeps_the_highest_replicated_floor() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        let heartbeat = |replicated_floor| AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
            round: 1,
            replicated_floor,
        };
        let _ = node.handle_append_entries(&heartbeat(5));
        assert_eq!(node.replicated_floor(), 5);
        let _ = node.handle_append_entries(&heartbeat(3));
        assert_eq!(node.replicated_floor(), 5, "the floor never moves back");
    }

    #[test]
    fn a_follower_names_the_leader_only_while_its_contact_is_fresh() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        assert_eq!(node.live_leader(std::time::Instant::now()), 0);
        let _ = node.handle_append_entries(&AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
            round: 1,
            replicated_floor: 0,
        });
        let now = std::time::Instant::now();
        assert_eq!(node.live_leader(now), 1);
        let stale = now + node.config.election_timeout_min + Duration::from_millis(1);
        assert_eq!(node.live_leader(stale), 0);
        assert_eq!(
            node.leader_id(),
            1,
            "the Raft leader id itself is unchanged"
        );
    }
}
