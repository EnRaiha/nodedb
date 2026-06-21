// SPDX-License-Identifier: BUSL-1.1

//! `RequestVote` request and response handlers.

use tracing::debug;

use crate::message::{RequestVoteRequest, RequestVoteResponse};
use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Handle incoming RequestVote RPC.
    ///
    /// Learners and observers never grant votes: by definition they are not
    /// members of the voting set for this term, and granting a vote could
    /// let an incorrect quorum form.
    pub fn handle_request_vote(&mut self, req: &RequestVoteRequest) -> RequestVoteResponse {
        if req.term > self.hard_state.current_term {
            self.become_follower(req.term);
        }

        if req.term < self.hard_state.current_term {
            return RequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: false,
            };
        }

        match self.role {
            NodeRole::Learner | NodeRole::Observer => {
                // Non-voters still observe a higher term before rejecting the
                // request, so they cannot re-enter as stale voters later.
                return RequestVoteResponse {
                    term: self.hard_state.current_term,
                    vote_granted: false,
                };
            }
            NodeRole::Follower | NodeRole::Candidate | NodeRole::Leader => {}
        }

        let voted_for = self.hard_state.voted_for;
        let can_vote = voted_for == 0 || voted_for == req.candidate_id;

        let log_ok = req.last_log_term > self.log.last_term()
            || (req.last_log_term == self.log.last_term()
                && req.last_log_index >= self.log.last_index());

        if can_vote && log_ok {
            self.hard_state.voted_for = req.candidate_id;
            self.persist_hard_state();
            self.reset_election_timeout();

            debug!(
                node = self.config.node_id,
                group = self.config.group_id,
                candidate = req.candidate_id,
                term = req.term,
                "granted vote"
            );

            RequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: true,
            }
        } else {
            RequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: false,
            }
        }
    }

    /// Handle RequestVote response (candidate only).
    pub fn handle_request_vote_response(&mut self, peer: u64, resp: &RequestVoteResponse) {
        if resp.term > self.hard_state.current_term {
            self.become_follower(resp.term);
            return;
        }

        // A delayed grant belongs only to the election term in which it was
        // cast. Counting it in a later candidacy can manufacture a quorum and
        // create two leaders in the same term.
        if resp.term < self.hard_state.current_term {
            return;
        }

        if self.role != NodeRole::Candidate {
            return;
        }

        if resp.vote_granted {
            self.votes_received.insert(peer);
            let vote_count = self.votes_received.len() + 1; // +1 for self-vote

            if vote_count >= self.config.quorum() {
                self.become_leader();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::message::{RequestVoteRequest, RequestVoteResponse};
    use crate::node::core::RaftNode;
    use crate::node::rpc::test_helpers::{observer_self_config, test_config};
    use crate::state::NodeRole;
    use crate::storage::MemStorage;

    #[test]
    fn vote_grant_and_reject() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        let req = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
        };
        let resp = node.handle_request_vote(&req);
        assert!(resp.vote_granted);

        let req2 = RequestVoteRequest {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
        };
        let resp2 = node.handle_request_vote(&req2);
        assert!(!resp2.vote_granted);
    }

    #[test]
    fn learner_rejects_vote_request() {
        let mut config = test_config(2, vec![1]);
        config.starts_as_learner = true;
        let mut node = RaftNode::new(config, MemStorage::new());
        assert_eq!(node.role(), NodeRole::Learner);

        let req = RequestVoteRequest {
            term: 5,
            candidate_id: 1,
            last_log_index: 10,
            last_log_term: 4,
            group_id: 1,
        };
        let resp = node.handle_request_vote(&req);
        assert!(
            !resp.vote_granted,
            "learner must never grant a vote, got {resp:?}"
        );
        assert_eq!(resp.term, 5);
        assert_eq!(node.current_term(), 5);
        assert_eq!(node.role(), NodeRole::Learner);
    }

    #[test]
    fn stale_vote_grant_does_not_win_newer_election() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());

        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.role(), NodeRole::Candidate);

        // Let the first election time out and start term 2 before term 1's
        // delayed grant arrives.
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        assert_eq!(node.current_term(), 2);

        node.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(node.role(), NodeRole::Candidate);
    }

    #[test]
    fn three_node_election() {
        let config1 = test_config(1, vec![2, 3]);
        let config2 = test_config(2, vec![1, 3]);
        let config3 = test_config(3, vec![1, 2]);

        let mut node1 = RaftNode::new(config1, MemStorage::new());
        let mut node2 = RaftNode::new(config2, MemStorage::new());
        let mut node3 = RaftNode::new(config3, MemStorage::new());

        node1.election_deadline = Instant::now() - Duration::from_millis(1);
        node1.tick();
        assert_eq!(node1.role(), NodeRole::Candidate);

        let ready = node1.take_ready();
        assert_eq!(ready.vote_requests.len(), 2);

        let resp2 = node2.handle_request_vote(&ready.vote_requests[0].1);
        let resp3 = node3.handle_request_vote(&ready.vote_requests[1].1);
        assert!(resp2.vote_granted);
        assert!(resp3.vote_granted);

        node1.handle_request_vote_response(2, &resp2);
        assert_eq!(node1.role(), NodeRole::Leader);
    }

    /// Observer never votes even when it receives a RequestVote RPC.
    #[test]
    fn observer_self_never_grants_vote() {
        let mut obs = RaftNode::new(observer_self_config(5), MemStorage::new());
        assert_eq!(obs.role(), NodeRole::Observer);

        let req = RequestVoteRequest {
            term: 10,
            candidate_id: 1,
            last_log_index: 100,
            last_log_term: 9,
            group_id: 1,
        };
        let resp = obs.handle_request_vote(&req);
        assert!(!resp.vote_granted, "observer must never grant a vote");
        assert_eq!(obs.role(), NodeRole::Observer, "role must stay Observer");
    }

    /// Observer ticking past its election deadline must never start an election.
    #[test]
    fn observer_tick_does_not_start_election() {
        let mut obs = RaftNode::new(observer_self_config(5), MemStorage::new());
        obs.election_deadline = Instant::now() - Duration::from_millis(1);
        obs.tick();
        assert_eq!(obs.role(), NodeRole::Observer);
        assert_eq!(obs.current_term(), 0);
    }
}
