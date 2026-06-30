// SPDX-License-Identifier: BUSL-1.1

//! `TimeoutNow` request handler (leadership transfer).

use crate::message::TimeoutNowRequest;
use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Handle an incoming `TimeoutNow` RPC.
    ///
    /// Accepts the trigger only when this node is a follower whose current
    /// term and known leader match the request — i.e. it is a current
    /// follower of the leader that initiated the transfer. In that case it
    /// immediately starts an election (bumping its own term and campaigning
    /// via the normal `RequestVote` path). All other cases — non-follower,
    /// stale/future term, or a different leader — are ignored. The trigger
    /// grants no vote and does not change the term itself.
    pub fn handle_timeout_now(&mut self, req: &TimeoutNowRequest) {
        if self.role == NodeRole::Follower
            && req.term == self.hard_state.current_term
            && req.leader_id == self.leader_id
        {
            self.start_election();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::error::RaftError;
    use crate::message::{
        AppendEntriesRequest, AppendEntriesResponse, RequestVoteResponse, TimeoutNowRequest,
    };
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;

    fn cfg(node_id: u64, peers: Vec<u64>, learners: Vec<u64>) -> RaftConfig {
        RaftConfig {
            node_id,
            group_id: 1,
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

    /// Elect node 1 leader in a 3-voter group (peers 2,3) and drain its ready.
    fn leader_3voter() -> RaftNode<MemStorage> {
        let mut node = RaftNode::new(cfg(1, vec![2, 3], vec![]), MemStorage::new());
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

    /// A follower of leader 1 at term 1 (peers 1,3).
    fn follower_of_leader1() -> RaftNode<MemStorage> {
        let mut node = RaftNode::new(cfg(2, vec![1, 3], vec![]), MemStorage::new());
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
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.leader_id(), 1);
        node
    }

    fn ack(idx: u64) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: idx,
        }
    }

    // t1: transfer to caught-up target emits exactly one TimeoutNow; delivering
    // it makes the target campaign at term+1.
    #[test]
    fn transfer_to_caught_up_target_emits_and_target_campaigns() {
        let mut leader = leader_3voter();
        let frontier = leader.last_log_index();
        leader.handle_append_entries_response(2, &ack(frontier));
        let _ = leader.take_ready();

        leader.transfer_leadership(2).unwrap();
        let ready = leader.take_ready();
        assert_eq!(ready.timeout_now.len(), 1);
        let (dest, req) = ready.timeout_now[0].clone();
        assert_eq!(dest, 2);
        assert_eq!(req.leader_id, 1);
        assert_eq!(req.term, 1);
        assert_eq!(req.group_id, 1);

        let mut target = follower_of_leader1();
        let term_before = target.current_term();
        target.handle_timeout_now(&req);
        assert_eq!(target.role(), NodeRole::Candidate);
        assert_eq!(target.current_term(), term_before + 1);
    }

    // t2: transfer to a lagging target defers; the trigger fires once the
    // catch-up ack advances match_index to the frontier.
    #[test]
    fn transfer_to_lagging_target_defers_then_emits() {
        let mut leader = leader_3voter();
        let frontier = leader.last_log_index();

        // Target 2 is at match_index 0 (< frontier) → no emit yet.
        leader.transfer_leadership(2).unwrap();
        assert!(leader.take_ready().timeout_now.is_empty());

        // Ack advances match_index to the frontier → emit.
        leader.handle_append_entries_response(2, &ack(frontier));
        assert_eq!(leader.take_ready().timeout_now.len(), 1);
    }

    // t3: handle_timeout_now ignores wrong term / wrong leader_id / non-follower.
    #[test]
    fn handle_timeout_now_ignored_on_guard_failure() {
        // Wrong term.
        let mut f = follower_of_leader1();
        f.handle_timeout_now(&TimeoutNowRequest {
            term: 2,
            leader_id: 1,
            group_id: 1,
        });
        assert_eq!(f.role(), NodeRole::Follower);
        assert_eq!(f.current_term(), 1);

        // Wrong leader_id.
        let mut f = follower_of_leader1();
        f.handle_timeout_now(&TimeoutNowRequest {
            term: 1,
            leader_id: 9,
            group_id: 1,
        });
        assert_eq!(f.role(), NodeRole::Follower);
        assert_eq!(f.current_term(), 1);

        // Not a follower (a leader).
        let mut l = leader_3voter();
        let term = l.current_term();
        l.handle_timeout_now(&TimeoutNowRequest {
            term,
            leader_id: 1,
            group_id: 1,
        });
        assert_eq!(l.role(), NodeRole::Leader);
        assert_eq!(l.current_term(), term);
    }

    // t4: deadline abort clears the transfer on tick and unblocks proposals.
    #[test]
    fn deadline_abort_clears_transfer_and_unblocks_propose() {
        let mut leader = leader_3voter();
        leader.transfer_leadership(2).unwrap();
        assert!(leader.leadership_transfer_in_progress());
        assert!(matches!(
            leader.propose(b"x".to_vec()),
            Err(RaftError::LeadershipTransferInProgress)
        ));

        leader.transfer_deadline_override(Instant::now() - Duration::from_millis(1));
        leader.tick();
        assert!(!leader.leadership_transfer_in_progress());
        assert!(leader.propose(b"x".to_vec()).is_ok());
    }

    // t5: propose is blocked while a transfer is pending.
    #[test]
    fn propose_blocked_during_transfer() {
        let mut leader = leader_3voter();
        leader.transfer_leadership(2).unwrap();
        assert!(matches!(
            leader.propose(b"x".to_vec()),
            Err(RaftError::LeadershipTransferInProgress)
        ));
    }

    // t6: stepping down (higher term) clears the transfer.
    #[test]
    fn become_follower_clears_transfer() {
        let mut leader = leader_3voter();
        leader.transfer_leadership(2).unwrap();
        assert!(leader.leadership_transfer_in_progress());

        // A higher-term response forces step-down.
        leader.handle_append_entries_response(
            3,
            &AppendEntriesResponse {
                term: 5,
                success: false,
                last_log_index: 0,
            },
        );
        assert_eq!(leader.role(), NodeRole::Follower);
        assert!(!leader.leadership_transfer_in_progress());
    }

    // t7: transfer rejected for learner / self / non-peer targets.
    #[test]
    fn transfer_rejected_for_invalid_targets() {
        let mut node = RaftNode::new(cfg(1, vec![2], vec![3]), MemStorage::new());
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

        // Learner target.
        assert!(matches!(
            node.transfer_leadership(3),
            Err(RaftError::InvalidTransferTarget { target: 3 })
        ));
        // Self.
        assert!(matches!(
            node.transfer_leadership(1),
            Err(RaftError::InvalidTransferTarget { target: 1 })
        ));
        // Non-peer.
        assert!(matches!(
            node.transfer_leadership(99),
            Err(RaftError::InvalidTransferTarget { target: 99 })
        ));
    }

    // t8 (HOLE 1): after a caught-up emit, further acks do NOT re-emit.
    #[test]
    fn emit_once_across_multiple_acks() {
        let mut leader = leader_3voter();
        let frontier = leader.last_log_index();

        // First ack catches the target up (transfer not yet pending → no emit).
        leader.handle_append_entries_response(2, &ack(frontier));
        // Transfer emits exactly one.
        leader.transfer_leadership(2).unwrap();
        // Two further acks must not push additional triggers.
        leader.handle_append_entries_response(2, &ack(frontier));
        leader.handle_append_entries_response(2, &ack(frontier));

        assert_eq!(leader.take_ready().timeout_now.len(), 1);
    }
}
