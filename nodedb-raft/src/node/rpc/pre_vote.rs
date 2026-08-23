// SPDX-License-Identifier: BUSL-1.1

//! `PreVote` request and response handlers.
//!
//! A pre-vote probe asks peers whether they WOULD vote for the candidate at
//! `current_term + 1`. It consumes nothing: no term is adopted, no vote is
//! recorded, no election deadline is reset. Only a granting quorum promotes
//! the prober to a real election.

use tracing::debug;

use crate::message::{PreVoteRequest, PreVoteResponse};
use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Answer an incoming `PreVote` probe.
    ///
    /// Mutates nothing — not `current_term`, `voted_for`, `election_deadline`,
    /// `leader_id`, or the role. Resetting the deadline for a probe would let
    /// any peer suppress a legitimate election just by probing.
    pub fn handle_pre_vote(&mut self, req: &PreVoteRequest) -> PreVoteResponse {
        let term = self.hard_state.current_term;
        let refuse = PreVoteResponse {
            term,
            vote_granted: false,
        };

        match self.role {
            // A leader is about to reassert itself by heartbeat.
            NodeRole::Leader => return refuse,
            // Non-voters: granting could let an incorrect quorum form.
            NodeRole::Learner | NodeRole::Observer => return refuse,
            NodeRole::Follower | NodeRole::Candidate => {}
        }

        // A prober whose hypothetical term is at or below ours is behind us. It
        // must catch up via AppendEntries; a real election at that term would
        // be rejected anyway, so granting only buys a lagging node a loop.
        if req.term <= term {
            return refuse;
        }

        // Leader stickiness, bounded by TIME. A leader that is still reaching
        // us needs no replacement. The bound is what makes the check safe: once
        // a real leader crashes, contact ages past `election_timeout_min` and
        // every follower starts granting again. A node that has never heard a
        // leader is never blocked here, so a fresh cluster still elects.
        let leader_is_live = self
            .leader_contact
            .is_some_and(|c| c.at.elapsed() < self.config.election_timeout_min);
        if leader_is_live {
            return refuse;
        }

        // Up-to-date rule, identical to the real vote path.
        let log_ok = req.last_log_term > self.log.last_term()
            || (req.last_log_term == self.log.last_term()
                && req.last_log_index >= self.log.last_index());
        if !log_ok {
            return refuse;
        }

        debug!(
            node = self.config.node_id,
            group = self.config.group_id,
            candidate = req.candidate_id,
            probed_term = req.term,
            "granted pre-vote"
        );

        PreVoteResponse {
            term,
            vote_granted: true,
        }
    }

    /// Handle a `PreVote` response for an in-flight round.
    pub fn handle_pre_vote_response(&mut self, peer: u64, resp: &PreVoteResponse) {
        // A live peer's REAL term is real information, even mid-probe.
        if resp.term > self.hard_state.current_term {
            self.become_follower(resp.term);
            self.pre_vote = None;
            return;
        }

        if self.role == NodeRole::Leader {
            return;
        }

        // The response carries the responder's real term, not the hypothetical
        // one, so a round cannot be identified from the response alone. A stale
        // response is instead discarded by the round itself: `pre_vote` is
        // `None` once the round ended (quorum reached, superseded, or stepped
        // down), and a round whose base term has since moved no longer probes
        // `current_term + 1` and is abandoned here. Miscounting inside a live
        // round is harmless regardless — a pre-vote grants nothing, and the
        // real election it triggers still needs real votes.
        let next_term = self.hard_state.current_term + 1;
        let quorum = self.config.quorum();
        let Some(round) = self.pre_vote.as_mut() else {
            return;
        };
        if round.term != next_term {
            self.pre_vote = None;
            return;
        }
        if !resp.vote_granted {
            return;
        }

        round.granted.insert(peer);
        let count = round.granted.len() + 1; // +1: this node backs itself.
        if count >= quorum {
            self.pre_vote = None;
            // The one and only place the term is bumped and real votes go out.
            self.start_election();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::message::{
        AppendEntriesRequest, PreVoteRequest, PreVoteResponse, RequestVoteRequest,
    };
    use crate::node::core::RaftNode;
    use crate::node::rpc::test_helpers::{observer_self_config, test_config};
    use crate::state::NodeRole;
    use crate::storage::MemStorage;

    fn probe(term: u64, last_log_index: u64, last_log_term: u64) -> PreVoteRequest {
        PreVoteRequest {
            term,
            candidate_id: 9,
            last_log_index,
            last_log_term,
            group_id: 1,
        }
    }

    /// Node 2 in a 3-voter group, currently following leader 1 at term 1.
    fn follower_of_live_leader() -> RaftNode<MemStorage> {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        node.handle_append_entries(&AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
        });
        assert_eq!(node.role(), NodeRole::Follower);
        node
    }

    fn granted(term: u64) -> PreVoteResponse {
        PreVoteResponse {
            term,
            vote_granted: true,
        }
    }

    #[test]
    fn probing_and_answering_leave_every_term_untouched_until_quorum() {
        let mut prober = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        prober.election_deadline_override(Instant::now() - Duration::from_millis(1));
        prober.tick();

        assert_eq!(prober.role(), NodeRole::Follower);
        assert_eq!(prober.current_term(), 0);
        let ready = prober.take_ready();
        assert_eq!(ready.pre_vote_requests.len(), 2);
        assert!(ready.vote_requests.is_empty());
        assert!(
            ready.hard_state.is_none(),
            "a pre-vote round must persist nothing"
        );
        assert_eq!(ready.pre_vote_requests[0].1.term, 1);

        let mut peer = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        let resp = peer.handle_pre_vote(&ready.pre_vote_requests[0].1);
        assert!(resp.vote_granted);
        assert_eq!(peer.current_term(), 0, "answering must not adopt a term");
        assert_eq!(peer.role(), NodeRole::Follower);

        // Quorum of 3 is 2, so one grant plus self promotes to a real election.
        prober.handle_pre_vote_response(2, &resp);
        assert_eq!(prober.role(), NodeRole::Candidate);
        assert_eq!(prober.current_term(), 1);
        assert!(prober.pre_vote.is_none());
        assert_eq!(prober.take_ready().vote_requests.len(), 2);

        // A late grant from the finished round changes nothing.
        prober.handle_pre_vote_response(3, &granted(1));
        assert_eq!(prober.current_term(), 1);
    }

    #[test]
    fn a_follower_in_recent_leader_contact_refuses() {
        let mut follower = follower_of_live_leader();

        let mut prober = RaftNode::new(test_config(3, vec![1, 2]), MemStorage::new());
        prober.handle_append_entries(&AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
        });
        prober.election_deadline_override(Instant::now() - Duration::from_millis(1));
        prober.tick();
        let req = prober.take_ready().pre_vote_requests[0].1.clone();

        let resp = follower.handle_pre_vote(&req);
        assert!(!resp.vote_granted);
        assert_eq!(follower.current_term(), 1);
        assert_eq!(follower.leader_id(), 1);

        prober.handle_pre_vote_response(2, &resp);
        assert_eq!(
            prober.current_term(),
            1,
            "a refused probe must not inflate the prober's term"
        );
        assert_eq!(prober.role(), NodeRole::Follower);
    }

    #[test]
    fn stickiness_releases_once_contact_is_older_than_the_election_timeout() {
        let mut follower = follower_of_live_leader();
        assert!(!follower.handle_pre_vote(&probe(2, 0, 0)).vote_granted);

        // election_timeout_min is 150ms in the test config.
        follower.leader_contact_at_override(Instant::now() - Duration::from_secs(1));

        let resp = follower.handle_pre_vote(&probe(2, 0, 0));
        assert!(
            resp.vote_granted,
            "a crashed leader must stop blocking pre-votes, or no leader is ever elected again"
        );
        assert_eq!(resp.term, 1);
        assert_eq!(follower.current_term(), 1);
    }

    #[test]
    fn a_node_that_never_heard_a_leader_grants() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        let deadline_before = node.election_deadline;

        assert!(node.handle_pre_vote(&probe(1, 0, 0)).vote_granted);
        assert_eq!(node.current_term(), 0);
        assert_eq!(
            node.election_deadline, deadline_before,
            "a probe must not push back the election deadline"
        );
    }

    #[test]
    fn a_leader_refuses() {
        let mut leader = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        leader.election_deadline_override(Instant::now() - Duration::from_millis(1));
        leader.tick();
        assert_eq!(leader.role(), NodeRole::Leader);

        assert!(!leader.handle_pre_vote(&probe(5, 0, 0)).vote_granted);
        assert_eq!(leader.role(), NodeRole::Leader);
    }

    #[test]
    fn a_learner_refuses() {
        let mut config = test_config(2, vec![1]);
        config.starts_as_learner = true;
        let mut learner = RaftNode::new(config, MemStorage::new());

        assert!(!learner.handle_pre_vote(&probe(5, 10, 4)).vote_granted);
        assert_eq!(learner.current_term(), 0);
        assert_eq!(learner.role(), NodeRole::Learner);
    }

    #[test]
    fn an_observer_refuses() {
        let mut observer = RaftNode::new(observer_self_config(5), MemStorage::new());

        assert!(!observer.handle_pre_vote(&probe(5, 10, 4)).vote_granted);
        assert_eq!(observer.current_term(), 0);
        assert_eq!(observer.role(), NodeRole::Observer);
    }

    #[test]
    fn a_prober_with_a_shorter_log_is_refused() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        node.handle_append_entries(&AppendEntriesRequest {
            term: 2,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![crate::message::LogEntry {
                term: 2,
                index: 1,
                data: b"x".to_vec(),
            }],
            leader_commit: 0,
            group_id: 1,
        });
        node.leader_contact_at_override(Instant::now() - Duration::from_secs(1));

        // Older last term.
        assert!(!node.handle_pre_vote(&probe(3, 5, 1)).vote_granted);
        // Same term, shorter log.
        assert!(!node.handle_pre_vote(&probe(3, 0, 2)).vote_granted);
        // Same term, equal length.
        assert!(node.handle_pre_vote(&probe(3, 1, 2)).vote_granted);
    }

    #[test]
    fn a_hypothetical_term_at_or_below_the_current_one_is_refused() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        node.handle_request_vote(&RequestVoteRequest {
            term: 5,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
        });
        assert_eq!(node.current_term(), 5);

        assert!(!node.handle_pre_vote(&probe(5, 0, 0)).vote_granted);
        assert!(!node.handle_pre_vote(&probe(4, 0, 0)).vote_granted);
        assert!(node.handle_pre_vote(&probe(6, 0, 0)).vote_granted);
    }

    #[test]
    fn a_higher_term_response_steps_the_prober_down_and_ends_the_round() {
        let mut prober = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        prober.election_deadline_override(Instant::now() - Duration::from_millis(1));
        prober.tick();
        assert!(prober.pre_vote.is_some());

        prober.handle_pre_vote_response(
            2,
            &PreVoteResponse {
                term: 7,
                vote_granted: false,
            },
        );

        assert_eq!(prober.role(), NodeRole::Follower);
        assert_eq!(prober.current_term(), 7);
        assert!(prober.pre_vote.is_none());
    }

    #[test]
    fn a_single_voter_cluster_wins_on_one_tick() {
        let mut node = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();

        assert_eq!(node.role(), NodeRole::Leader);
        assert_eq!(node.current_term(), 1);
        assert!(
            node.pre_vote.is_none(),
            "a single voter has nobody to probe"
        );
        assert!(node.take_ready().pre_vote_requests.is_empty());
    }
}
