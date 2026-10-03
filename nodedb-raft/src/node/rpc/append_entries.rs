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
                round: req.round,
                needs_snapshot: false,
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
        // The leader's knowledge of every voter's log, whether or not this
        // log matches.
        self.replicated_floor = self.replicated_floor.max(req.replicated_floor);
        // Recorded before the log checks: contact happened and the leader's
        // commit index is authoritative whether or not our log matches. A
        // mismatched follower is behind, which is exactly what the staleness
        // bound must notice.
        self.leader_contact = Some(crate::node::core::LeaderContact {
            leader_commit: req.leader_commit,
            at: std::time::Instant::now(),
        });

        // A follower with no state to resume from takes no entries. The
        // leader answers the refusal with a snapshot.
        if self.snapshot_required {
            return AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                last_log_index: self.log.last_index(),
                round: req.round,
                needs_snapshot: true,
            };
        }

        // Committed entries this node never applied are gone from its log.
        // Rejecting at `last_applied` walks the leader's `next_index` below its
        // compacted prefix, where the leader sends InstallSnapshot instead.
        if self.has_apply_gap() {
            return AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                last_log_index: self.volatile.last_applied,
                round: req.round,
                needs_snapshot: false,
            };
        }

        // Check prev_log consistency.
        if req.prev_log_index > 0 {
            match self.log.term_at(req.prev_log_index) {
                Some(term) if term == req.prev_log_term => {}
                _ => {
                    return AppendEntriesResponse {
                        term: self.hard_state.current_term,
                        success: false,
                        last_log_index: self.log.last_index(),
                        round: req.round,
                        needs_snapshot: false,
                    };
                }
            }
        }

        let wrote = match self.log.append_entries(req.prev_log_index, &req.entries) {
            Ok(wrote) => wrote,
            Err(e) => {
                warn!(group = self.config.group_id, error = %e, "append_entries failed");
                return AppendEntriesResponse {
                    term: self.hard_state.current_term,
                    success: false,
                    last_log_index: self.log.last_index(),
                    round: req.round,
                    needs_snapshot: false,
                };
            }
        };

        // The last entry this log now shares with the leader. Entries past it
        // can be a stale suffix of an earlier term.
        let matched = req.entries.last().map_or(req.prev_log_index, |e| e.index);
        let commit = req.leader_commit.min(matched);
        if commit > self.volatile.commit_index {
            self.volatile.commit_index = commit;
            self.collect_committed_entries();
        }

        AppendEntriesResponse {
            term: self.hard_state.current_term,
            success: true,
            last_log_index: self.claimable_match(matched, wrote),
            round: req.round,
            needs_snapshot: false,
        }
    }

    /// The match index a success reply claims, from the last entry shared
    /// with the leader.
    ///
    /// The leader counts the claim as entries durable on this node. When the
    /// request took a storage write, the caller makes every write staged so
    /// far durable before the reply leaves. Every held entry is then durable,
    /// so the claim is `matched`. Otherwise the reply waits on no disk write,
    /// and the claim stops at the durable prefix. A later round reports the
    /// rest once the disk holds it.
    fn claimable_match(&self, matched: u64, wrote: bool) -> u64 {
        if wrote {
            matched
        } else {
            matched.min(self.log.stable_index())
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
                    let backed_off = if new_next < state.next_index {
                        new_next
                    } else {
                        state.next_index.saturating_sub(1)
                    };
                    // A stale rejection never moves below the match.
                    state.next_index = backed_off.max(state.match_index.saturating_add(1)).max(1);
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

        // Before the success/failure split: a rejection still proves the peer
        // recognises this term, which is all a leadership check needs.
        leader.record_ack(peer);

        // Same signal drives check-quorum and the lease. A rejection counts:
        // the follower recorded this leader's contact before its log check.
        // The lease settles at the round's send time. Check-quorum dates the
        // answer at its arrival.
        if peer_is_voter && resp.term == self.hard_state.current_term {
            let now = std::time::Instant::now();
            self.record_lease_ack(peer, resp.round, now);
            self.settle_lease();
            self.refresh_quorum_contact(now);
        }
        let leader = match self.leader_state.as_mut() {
            Some(ls) => ls,
            None => return,
        };

        if resp.needs_snapshot {
            leader.awaiting_snapshot.insert(peer);
        } else {
            leader.awaiting_snapshot.remove(&peer);
        }
        if resp.success {
            let new_match = resp.last_log_index;
            if new_match > leader.match_index_for(peer) {
                leader.set_match_index(peer, new_match);
                leader.set_next_index(peer, new_match + 1);
            }
            if peer_is_voter {
                self.try_advance_commit_index();
                self.replicated_floor = self.replicated_floor();
            }
        } else if resp.needs_snapshot {
            // The peer holds no state to resume from: entries from any index
            // leave it as it is, so it gets a snapshot.
            if !self.ready.snapshots_needed.contains(&peer) {
                self.ready.snapshots_needed.push(peer);
            }
        } else {
            let new_next = resp.last_log_index + 1;
            let current_next = leader.next_index_for(peer);
            let backed_off = if new_next < current_next {
                new_next
            } else {
                current_next.saturating_sub(1)
            };
            // The peer holds every entry through its `match_index`. A stale
            // rejection, from a request sent before a later one matched, never
            // moves the next index below it: that would send entries the peer
            // holds, or a snapshot once the log compacted them.
            let floor = leader.match_index_for(peer).saturating_add(1);
            leader.set_next_index(peer, backed_off.max(floor).max(1));
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
    use std::time::Duration;

    use crate::message::{
        AppendEntriesRequest, AppendEntriesResponse, LogEntry, RequestVoteResponse,
    };
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::node::leader_lease::UNTRACKED_ROUND;
    use crate::node::rpc::test_helpers::{setup_leader_with_observer, test_config};
    use crate::state::NodeRole;
    use crate::storage::MemStorage;
    use crate::test_support::force_election;

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
            round: 1,
            replicated_floor: 0,
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
            round: 1,
            replicated_floor: 0,
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
            round: 1,
            replicated_floor: 0,
        };

        let resp = node.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(node.commit_index(), 1);
        // Crucially, the learner did not turn into a Follower.
        assert_eq!(node.role(), NodeRole::Learner);
        assert_eq!(node.leader_id(), 1);
    }

    /// A follower that requires a snapshot refuses entries with
    /// `needs_snapshot`, and its leader flags it for a snapshot even though
    /// the leader's log holds every entry.
    #[test]
    fn a_follower_that_requires_a_snapshot_gets_one() {
        let mut follower = RaftNode::new(test_config(2, vec![1]), MemStorage::new());
        follower.set_snapshot_required(true);
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
            round: 1,
            replicated_floor: 0,
        };
        let refusal = follower.handle_append_entries(&req);
        assert!(!refusal.success);
        assert!(refusal.needs_snapshot);
        assert_eq!(follower.last_log_index(), 0, "no entry was appended");
        assert_eq!(follower.leader_id(), 1, "the leader's contact still counts");

        let mut leader = RaftNode::new(test_config(1, vec![2]), MemStorage::new());
        force_election(&mut leader);
        leader.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(leader.role(), NodeRole::Leader);
        let _ = leader.take_ready();
        leader.handle_append_entries_response(2, &refusal);
        assert!(leader.take_ready().snapshots_needed.contains(&2));

        follower.set_snapshot_required(false);
        assert!(follower.handle_append_entries(&req).success);
    }

    /// A stale rejection, answering a request sent before a later one
    /// matched, never moves the peer's next index below its match.
    #[test]
    fn a_stale_rejection_keeps_the_next_index_above_the_match() {
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
        let answer = |success, last_log_index| AppendEntriesResponse {
            term: 1,
            success,
            last_log_index,
            round: 1,
            needs_snapshot: false,
        };
        node.handle_append_entries_response(2, &answer(true, 1));
        node.handle_append_entries_response(2, &answer(false, 0));
        let leader = node.leader_state.as_ref().expect("leader state");
        assert_eq!(leader.match_index_for(2), 1);
        assert_eq!(leader.next_index_for(2), 2);
    }

    /// A node that steps down within the term it voted in keeps that vote.
    /// It never votes twice in one term.
    #[test]
    fn a_step_down_within_a_term_keeps_its_vote() {
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

        node.become_follower(1);
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.hard_state.voted_for, 1, "the vote of term 1 stands");

        node.become_follower(2);
        assert_eq!(node.hard_state.voted_for, 0, "a new term frees the vote");
    }

    #[test]
    fn leader_steps_down_on_higher_term() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        force_election(&mut node);
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
            round: 1,
            replicated_floor: 0,
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

        force_election(&mut node);
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
            round: 1,
            replicated_floor: 0,
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
        force_election(&mut node);
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
            round: UNTRACKED_ROUND,
            needs_snapshot: false,
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
        node2.expire_boot_vote_fence();

        force_election(&mut node1);
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
            round: UNTRACKED_ROUND,
            needs_snapshot: false,
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
        force_election(&mut node1);
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
            round: UNTRACKED_ROUND,
            needs_snapshot: false,
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
            round: UNTRACKED_ROUND,
            needs_snapshot: false,
        };
        leader.handle_append_entries_response(2, &voter_ack);
        assert_eq!(
            leader.commit_index(),
            idx,
            "source must commit without observer ack (observer crash)"
        );
        let _ = ready;
    }

    /// Storage that takes every write at once and reports a durable prefix
    /// the test sets.
    #[derive(Default)]
    struct StagingStorage {
        inner: MemStorage,
        stable: (u64, u64),
    }

    impl crate::storage::LogStorage for StagingStorage {
        fn append(&mut self, entries: &[LogEntry]) -> crate::error::Result<()> {
            self.inner.append(entries)
        }
        fn truncate(&mut self, index: u64) -> crate::error::Result<()> {
            self.inner.truncate(index)
        }
        fn load_entries_after(&self, snapshot_index: u64) -> crate::error::Result<Vec<LogEntry>> {
            self.inner.load_entries_after(snapshot_index)
        }
        fn compact(&mut self, index: u64, term: u64) -> crate::error::Result<()> {
            self.inner.compact(index, term)
        }
        fn snapshot_metadata(&self) -> (u64, u64) {
            self.inner.snapshot_metadata()
        }
        fn save_hard_state(&mut self, state: &crate::state::HardState) -> crate::error::Result<()> {
            self.inner.save_hard_state(state)
        }
        fn load_hard_state(&self) -> crate::error::Result<crate::state::HardState> {
            self.inner.load_hard_state()
        }
        fn save_applied_index(&mut self, index: u64) -> crate::error::Result<()> {
            self.inner.save_applied_index(index)
        }
        fn load_applied_index(&self) -> crate::error::Result<u64> {
            self.inner.load_applied_index()
        }
        fn stable_through(&self) -> Option<(u64, u64)> {
            Some(self.stable)
        }
    }

    fn entry(term: u64, index: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            data: vec![index as u8],
        }
    }

    fn append_request(
        term: u64,
        prev: (u64, u64),
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id: 2,
            prev_log_index: prev.0,
            prev_log_term: prev.1,
            entries,
            leader_commit,
            group_id: 1,
            round: 1,
            replicated_floor: 0,
        }
    }

    /// A request that writes entries claims them: its caller makes them
    /// durable before the reply leaves. A resend of the same range writes
    /// nothing and waits on no disk, so it claims only the durable prefix.
    #[test]
    fn a_reply_without_a_write_claims_only_the_durable_prefix() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), StagingStorage::default());
        let req = append_request(1, (0, 0), vec![entry(1, 1), entry(1, 2)], 0);

        let resp = node.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(resp.last_log_index, 2, "the written entries are claimed");

        let resp = node.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(resp.last_log_index, 0, "nothing is durable yet");

        node.log.storage_mut().stable = (1, 1);
        let heartbeat = append_request(1, (2, 1), vec![], 0);
        let resp = node.handle_append_entries(&heartbeat);
        assert!(resp.success);
        assert_eq!(
            resp.last_log_index, 1,
            "the claim stops at the durable entry"
        );

        node.log.storage_mut().stable = (2, 1);
        let resp = node.handle_append_entries(&heartbeat);
        assert_eq!(resp.last_log_index, 2);
    }

    /// Entries past the request's last entry can be a stale suffix of an
    /// earlier term. A success reply never claims them, and the follower
    /// never commits them.
    #[test]
    fn a_reply_never_claims_a_stale_suffix() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        let old = append_request(1, (0, 0), vec![entry(1, 1), entry(1, 2), entry(1, 3)], 0);
        assert!(node.handle_append_entries(&old).success);

        // A leader of term 2 shares only entry 1 with this log.
        let heartbeat = append_request(2, (1, 1), vec![], 3);
        let resp = node.handle_append_entries(&heartbeat);
        assert!(resp.success);
        assert_eq!(
            resp.last_log_index, 1,
            "entries 2 and 3 are not the leader's"
        );
        assert_eq!(node.commit_index(), 1);
    }

    /// A reordered request that matches less of the log never pulls the
    /// commit index back.
    #[test]
    fn a_reordered_request_never_lowers_the_commit_index() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        let full = append_request(1, (0, 0), vec![entry(1, 1), entry(1, 2), entry(1, 3)], 3);
        assert!(node.handle_append_entries(&full).success);
        assert_eq!(node.commit_index(), 3);

        let late = append_request(1, (0, 0), vec![entry(1, 1)], 4);
        assert!(node.handle_append_entries(&late).success);
        assert_eq!(node.commit_index(), 3);
    }
}
