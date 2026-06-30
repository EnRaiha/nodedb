// SPDX-License-Identifier: BUSL-1.1

//! `AppendEntries` request and response handlers.

use tracing::warn;

use crate::message::{AppendEntriesRequest, AppendEntriesResponse};
use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Handle incoming AppendEntries RPC.
    pub fn handle_append_entries(&mut self, req: &AppendEntriesRequest) -> AppendEntriesResponse {
        if req.term < self.hard_state.current_term {
            return AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                last_log_index: self.log.last_index(),
            };
        }

        if req.term > self.hard_state.current_term
            || self.role == NodeRole::Candidate
            || (self.role == NodeRole::Leader && req.leader_id != self.config.node_id)
        {
            // `become_follower` preserves Learner role — see internal.rs.
            // A leader must also step down for a competing leader's valid
            // same-term AppendEntries. Keeping both in Leader role makes the
            // split permanent even though each records the other leader_id.
            self.become_follower(req.term);
        }

        self.leader_id = req.leader_id;
        self.reset_election_timeout();

        // Check prev_log consistency.
        if req.prev_log_index > 0 {
            match self.log.term_at(req.prev_log_index) {
                Some(term) if term == req.prev_log_term => {}
                _ => {
                    return AppendEntriesResponse {
                        term: self.hard_state.current_term,
                        success: false,
                        last_log_index: self.log.last_index(),
                    };
                }
            }
        }

        if let Err(e) = self.log.append_entries(req.prev_log_index, &req.entries) {
            warn!(group = self.config.group_id, error = %e, "append_entries failed");
            return AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                last_log_index: self.log.last_index(),
            };
        }

        if req.leader_commit > self.volatile.commit_index {
            self.volatile.commit_index = req.leader_commit.min(self.log.last_index());
            self.collect_committed_entries();
        }

        AppendEntriesResponse {
            term: self.hard_state.current_term,
            success: true,
            last_log_index: self.log.last_index(),
        }
    }

    /// Handle AppendEntries response from a peer (leader only).
    ///
    /// For voter peers: update match/next index and attempt commit advancement.
    /// For learner peers: update match/next index only (no quorum contribution).
    /// For observer peers: update observer state advisorily — no quorum
    /// contribution, no commit advancement. Observer acks release backpressure
    /// so the leader resumes sending to that observer.
    pub fn handle_append_entries_response(&mut self, peer: u64, resp: &AppendEntriesResponse) {
        if resp.term > self.hard_state.current_term {
            self.become_follower(resp.term);
            return;
        }

        if self.role != NodeRole::Leader {
            return;
        }

        let peer_is_voter = self.config.peers.contains(&peer);
        let peer_is_observer = self.config.observers.contains(&peer);

        // Observer acks are advisory: update observer state and release
        // backpressure, but never advance commit index.
        if peer_is_observer {
            let leader = match self.leader_state.as_mut() {
                Some(ls) => ls,
                None => return,
            };
            if resp.success {
                if let Some(state) = leader.observer_state_mut(peer) {
                    let new_match = resp.last_log_index;
                    if new_match > state.match_index {
                        state.match_index = new_match;
                        state.next_index = new_match + 1;
                    }
                    // Release backpressure: observer drained some entries.
                    state.pending_count = state.pending_count.saturating_sub(1);
                }
            } else {
                if let Some(state) = leader.observer_state_mut(peer) {
                    let new_next = resp.last_log_index + 1;
                    if new_next < state.next_index {
                        state.next_index = new_next.max(1);
                    } else {
                        state.next_index = state.next_index.saturating_sub(1).max(1);
                    }
                    state.pending_count = state.pending_count.saturating_sub(1);
                }
                self.send_append_entries_to_observer(peer);
            }
            // Observer acks never trigger commit advancement — return here.
            return;
        }

        let leader = match self.leader_state.as_mut() {
            Some(ls) => ls,
            None => return,
        };

        if resp.success {
            let new_match = resp.last_log_index;
            if new_match > leader.match_index_for(peer) {
                leader.set_match_index(peer, new_match);
                leader.set_next_index(peer, new_match + 1);
            }
            if peer_is_voter {
                self.try_advance_commit_index();
            }
        } else {
            let new_next = resp.last_log_index + 1;
            let current_next = leader.next_index_for(peer);
            if new_next < current_next {
                leader.set_next_index(peer, new_next.max(1));
            } else {
                leader.set_next_index(peer, current_next.saturating_sub(1).max(1));
            }
            self.send_append_entries(peer);
        }

        // If a leadership transfer is pending toward this peer and it has now
        // reached the log frontier, emit the `TimeoutNow` trigger. Self-guards
        // on target/emitted/caught-up, so this is a no-op otherwise.
        self.try_emit_timeout_now();
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::message::{
        AppendEntriesRequest, AppendEntriesResponse, LogEntry, RequestVoteResponse,
    };
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::node::rpc::test_helpers::{setup_leader_with_observer, test_config};
    use crate::state::NodeRole;
    use crate::storage::MemStorage;

    #[test]
    fn follower_rejects_old_term() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());
        node.hard_state.current_term = 5;

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
        };

        let resp = node.handle_append_entries(&req);
        assert!(!resp.success);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn follower_accepts_valid_append() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                LogEntry {
                    term: 1,
                    index: 1,
                    data: b"a".to_vec(),
                },
                LogEntry {
                    term: 1,
                    index: 2,
                    data: b"b".to_vec(),
                },
            ],
            leader_commit: 1,
            group_id: 1,
        };

        let resp = node.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(resp.last_log_index, 2);
        assert_eq!(node.commit_index(), 1);
        assert_eq!(node.leader_id(), 2);
    }

    #[test]
    fn learner_accepts_append_entries_and_stays_learner() {
        let mut config = test_config(2, vec![1]);
        config.starts_as_learner = true;
        let mut node = RaftNode::new(config, MemStorage::new());

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                data: b"x".to_vec(),
            }],
            leader_commit: 1,
            group_id: 1,
        };

        let resp = node.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(node.commit_index(), 1);
        // Crucially, the learner did not turn into a Follower.
        assert_eq!(node.role(), NodeRole::Learner);
        assert_eq!(node.leader_id(), 1);
    }

    #[test]
    fn leader_steps_down_on_higher_term() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        let _ready = node.take_ready();
        let resp = RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        node.handle_request_vote_response(2, &resp);
        assert_eq!(node.role(), NodeRole::Leader);

        let req = AppendEntriesRequest {
            term: 5,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
        };
        node.handle_append_entries(&req);
        assert_eq!(node.role(), NodeRole::Follower);
        assert_eq!(node.current_term(), 5);
        assert_eq!(node.leader_id(), 2);
    }

    #[test]
    fn leader_steps_down_for_competing_same_term_append_entries() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        node.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(node.role(), NodeRole::Leader);

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
        };
        node.handle_append_entries(&req);

        assert_eq!(node.role(), NodeRole::Follower);
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.leader_id(), 2);
    }

    /// Learner AE responses update match_index but must NOT trigger a
    /// commit advancement that relies on the learner counting toward
    /// quorum.
    #[test]
    fn learner_ae_response_does_not_drive_commit() {
        // 3 voters + 1 learner cluster: quorum = 2. Without any voter ACK,
        // a learner "ack" must not advance commit_index.
        let mut config = test_config(1, vec![2, 3]);
        config.learners = vec![4];
        let mut node = RaftNode::new(config, MemStorage::new());

        // Force leader.
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        // Grant self-vote via two voter responses.
        let yes = RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        node.handle_request_vote_response(2, &yes);
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();

        // Propose an entry at index 2 (no-op is index 1).
        let idx = node.propose(b"cmd".to_vec()).unwrap();
        assert_eq!(idx, 2);
        let _ = node.take_ready();

        // Baseline: commit_index should still be <2 (no voter ACKs yet for index 2).
        let baseline_commit = node.commit_index();
        assert!(baseline_commit < 2);

        // Learner (peer 4) ACKs index 2. This must NOT advance commit.
        let ae_ok = AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: 2,
        };
        node.handle_append_entries_response(4, &ae_ok);
        assert_eq!(
            node.commit_index(),
            baseline_commit,
            "learner ACK must not contribute to commit quorum"
        );

        // Now a voter (peer 2) ACKs index 2. Quorum = 2 (self + peer 2) — commit advances.
        node.handle_append_entries_response(2, &ae_ok);
        assert_eq!(node.commit_index(), 2);
    }

    #[test]
    fn three_node_replication() {
        let config1 = test_config(1, vec![2, 3]);
        let config2 = test_config(2, vec![1, 3]);

        let mut node1 = RaftNode::new(config1, MemStorage::new());
        let mut node2 = RaftNode::new(config2, MemStorage::new());

        node1.election_deadline = Instant::now() - Duration::from_millis(1);
        node1.tick();
        let ready = node1.take_ready();
        let resp2 = node2.handle_request_vote(&ready.vote_requests[0].1);
        node1.handle_request_vote_response(2, &resp2);
        assert_eq!(node1.role(), NodeRole::Leader);

        let heartbeat_ready = node1.take_ready();
        for (peer_id, msg) in &heartbeat_ready.messages {
            if *peer_id == 2 {
                let resp = node2.handle_append_entries(msg);
                node1.handle_append_entries_response(2, &resp);
            }
        }

        let idx = node1.propose(b"cmd1".to_vec()).unwrap();
        assert_eq!(idx, 2);

        let ready = node1.take_ready();
        for (peer_id, msg) in &ready.messages {
            if *peer_id == 2 {
                let resp = node2.handle_append_entries(msg);
                assert!(resp.success);
                node1.handle_append_entries_response(2, &resp);
            }
        }

        let ready = node1.take_ready();
        let committed: Vec<_> = ready
            .committed_entries
            .iter()
            .filter(|e| !e.data.is_empty())
            .collect();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].data, b"cmd1");
    }

    /// An observer receives AppendEntries, applies them, and stays in the
    /// Observer role. Its ack must NOT advance the source commit index.
    #[test]
    fn observer_receives_entries_but_does_not_contribute_to_quorum() {
        let (mut leader, mut obs) = setup_leader_with_observer();

        // Propose an entry. Quorum = 2 (self + peer 2).
        let idx = leader.propose(b"x".to_vec()).unwrap();
        assert_eq!(idx, 2);
        let ready = leader.take_ready();

        let baseline_commit = leader.commit_index();
        assert!(
            baseline_commit < 2,
            "commit should not advance without voter ACK"
        );

        // Observer receives the entry.
        let obs_msg = ready
            .messages
            .iter()
            .find(|(id, _)| *id == 5)
            .map(|(_, m)| m.clone());
        let obs_msg = obs_msg.expect("leader must send to observer");
        let obs_resp = obs.handle_append_entries(&obs_msg);
        assert!(obs_resp.success);
        assert_eq!(
            obs.role(),
            NodeRole::Observer,
            "observer must stay Observer"
        );

        // Feed observer ack back to leader. Commit must NOT advance.
        leader.handle_append_entries_response(5, &obs_resp);
        assert_eq!(
            leader.commit_index(),
            baseline_commit,
            "observer ack must not contribute to commit quorum"
        );

        // Now voter 2 ACKs — quorum (self + peer 2) is met and commit advances.
        let ae_ok = AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: idx,
        };
        leader.handle_append_entries_response(2, &ae_ok);
        assert_eq!(leader.commit_index(), idx);
    }

    /// 3 voters + 1 observer: kill 2 voters → cluster loses quorum even
    /// though the observer is still up and acking.
    #[test]
    fn observer_does_not_restore_lost_quorum() {
        // Node 1 is leader, voters 2 + 3, observer 5.
        let mut node1 = RaftNode::new(
            RaftConfig {
                node_id: 1,
                group_id: 1,
                peers: vec![2, 3],
                learners: vec![],
                observers: vec![5],
                starts_as_learner: false,
                starts_as_observer: false,
                election_timeout_min: Duration::from_millis(150),
                election_timeout_max: Duration::from_millis(300),
                heartbeat_interval: Duration::from_millis(50),
                log_compaction_threshold: None,
            },
            MemStorage::new(),
        );
        // Elect node 1.
        node1.election_deadline = Instant::now() - Duration::from_millis(1);
        node1.tick();
        let _ = node1.take_ready();
        for v in [2u64, 3] {
            node1.handle_request_vote_response(
                v,
                &RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                },
            );
        }
        assert_eq!(node1.role(), NodeRole::Leader);
        let _ = node1.take_ready();

        // Propose an entry at index 2.
        let idx = node1.propose(b"cmd".to_vec()).unwrap();
        let _ = node1.take_ready();
        let pre_commit = node1.commit_index();
        assert!(pre_commit < idx);

        // Voters 2 and 3 are "dead". Only observer 5 acks.
        let obs_ack = AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: idx,
        };
        node1.handle_append_entries_response(5, &obs_ack);
        assert_eq!(
            node1.commit_index(),
            pre_commit,
            "quorum is lost (2 voters dead); observer ack must not restore it"
        );
    }

    /// An offline observer does not stall the source: voters commit normally
    /// with no observer acks arriving at all.
    #[test]
    fn observer_crash_does_not_stall_source() {
        let (mut leader, _obs) = setup_leader_with_observer();

        let idx = leader.propose(b"y".to_vec()).unwrap();
        assert_eq!(idx, 2);
        let ready = leader.take_ready();

        // Voter 2 acks. Observer 5 is "offline" (no ack received).
        let voter_ack = AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: idx,
        };
        leader.handle_append_entries_response(2, &voter_ack);
        assert_eq!(
            leader.commit_index(),
            idx,
            "source must commit without observer ack (observer crash)"
        );
        let _ = ready;
    }
}
