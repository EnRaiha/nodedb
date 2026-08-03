// SPDX-License-Identifier: BUSL-1.1

//! Internal state transitions and replication logic.

use std::time::{Duration, Instant};

use rand::RngExt;
use tracing::{debug, info};

use crate::error::RaftError;
use crate::message::{AppendEntriesRequest, LogEntry, RequestVoteRequest};
use crate::state::{LeaderState, NodeRole};
use crate::storage::LogStorage;

use super::core::RaftNode;

impl<S: LogStorage> RaftNode<S> {
    pub(super) fn start_election(&mut self) {
        // Learners and observers never stand for election — defensive check
        // in case `tick()` is bypassed (e.g., tests forcing a deadline).
        match self.role {
            NodeRole::Learner | NodeRole::Observer => return,
            NodeRole::Follower | NodeRole::Candidate | NodeRole::Leader => {}
        }

        self.hard_state.current_term += 1;
        self.role = NodeRole::Candidate;
        self.hard_state.voted_for = self.config.node_id;
        self.votes_received.clear();
        self.leader_id = 0;

        self.persist_hard_state();
        self.reset_election_timeout();

        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            term = self.hard_state.current_term,
            "starting election"
        );

        // Single-voter cluster: win immediately. (Learners in the group do
        // not count — single voter + N learners still elects the single
        // voter as leader.)
        if self.config.peers.is_empty() {
            self.become_leader();
            return;
        }

        for &peer in &self.config.peers {
            self.ready.vote_requests.push((
                peer,
                RequestVoteRequest {
                    term: self.hard_state.current_term,
                    candidate_id: self.config.node_id,
                    last_log_index: self.log.last_index(),
                    last_log_term: self.log.last_term(),
                    group_id: self.config.group_id,
                },
            ));
        }
    }

    /// Step down to follower (or keep learner/observer role if the node is one).
    ///
    /// Learners and observers that receive an `AppendEntries` with a higher
    /// term update their term but do not transition to `Follower` — they stay
    /// in their non-election role so the tick loop continues to skip timeouts.
    pub(super) fn become_follower(&mut self, term: u64) {
        let was_leader = self.role == NodeRole::Leader;
        match self.role {
            NodeRole::Learner | NodeRole::Observer => {
                // Preserve the non-election role.
            }
            NodeRole::Follower | NodeRole::Candidate | NodeRole::Leader => {
                self.role = NodeRole::Follower;
            }
        }
        self.hard_state.current_term = term;
        self.hard_state.voted_for = 0;
        self.leader_state = None;
        self.votes_received.clear();
        // Any in-progress leadership transfer is moot once we step down.
        self.leadership_transfer = None;
        self.persist_hard_state();
        self.reset_election_timeout();

        if was_leader {
            info!(
                node = self.config.node_id,
                group = self.config.group_id,
                term,
                "stepped down from leader"
            );
        }
    }

    pub(super) fn become_leader(&mut self) {
        self.role = NodeRole::Leader;
        self.leader_id = self.config.node_id;

        // Leader tracks voter peers, learner peers, and observer peers for
        // replication. Only voters count toward the commit quorum (see
        // `try_advance_commit_index`). Learners still receive entries so they
        // can catch up for promotion. Observers receive entries and ack
        // advisorily but are never counted in quorum.
        let mut ls = LeaderState::new(
            &self.config.peers,
            &self.config.observers,
            self.log.last_index(),
        );
        for &learner in &self.config.learners {
            ls.add_peer(learner, self.log.last_index());
        }
        self.leader_state = Some(ls);

        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            term = self.hard_state.current_term,
            voters = self.config.peers.len(),
            learners = self.config.learners.len(),
            "became leader"
        );

        // Raft paper §5.4.2: leader appends a no-op entry.
        let noop = LogEntry {
            term: self.hard_state.current_term,
            index: self.log.last_index() + 1,
            data: Vec::new(),
        };
        let _ = self.log.append(noop);

        // Single-voter cluster: commit the no-op immediately.
        if self.config.cluster_size() == 1 {
            self.volatile.commit_index = self.log.last_index();
            self.collect_committed_entries();
        }

        self.replicate_to_all();
    }

    /// Send `AppendEntries` to every tracked peer (voters + learners + observers).
    ///
    /// Observers are skipped when their advisory send queue is full — source
    /// commits are never gated on observer apply pace.
    pub(super) fn replicate_to_all(&mut self) {
        let voters_and_learners: Vec<u64> = self
            .config
            .peers
            .iter()
            .chain(self.config.learners.iter())
            .copied()
            .collect();
        for peer in voters_and_learners {
            self.send_append_entries(peer);
        }

        let observers: Vec<u64> = self.config.observers.clone();
        for observer in observers {
            self.send_append_entries_to_observer(observer);
        }
    }

    pub(super) fn send_append_entries(&mut self, peer: u64) {
        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        let next_index = leader.next_index_for(peer);
        let prev_log_index = next_index.saturating_sub(1);

        let prev_log_term = match self.log.term_at(prev_log_index) {
            Some(term) => term,
            None => {
                debug!(
                    node = self.config.node_id,
                    group = self.config.group_id,
                    peer,
                    next_index,
                    snapshot_index = self.log.snapshot_index(),
                    "peer needs snapshot (log compacted)"
                );
                self.ready.snapshots_needed.push(peer);
                return;
            }
        };

        let entries = if next_index <= self.log.last_index() {
            match self.log.entries_range(next_index, self.log.last_index()) {
                Ok(slice) => slice.to_vec(),
                Err(RaftError::LogCompacted { .. }) => {
                    debug!(
                        node = self.config.node_id,
                        group = self.config.group_id,
                        peer,
                        next_index,
                        "peer needs snapshot (entries compacted)"
                    );
                    self.ready.snapshots_needed.push(peer);
                    return;
                }
                Err(_) => vec![],
            }
        } else {
            vec![]
        };

        self.ready.messages.push((
            peer,
            AppendEntriesRequest {
                term: self.hard_state.current_term,
                leader_id: self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.volatile.commit_index,
                group_id: self.config.group_id,
            },
        ));
    }

    /// Send `AppendEntries` to an observer peer.
    ///
    /// If the observer's advisory send queue is full (backpressure threshold
    /// reached), the send is skipped. The observer will fall behind and recover
    /// via snapshot when it reconnects. Source commits are never delayed.
    pub(super) fn send_append_entries_to_observer(&mut self, observer: u64) {
        let can_receive = match &self.leader_state {
            Some(ls) => ls.observer_can_receive(observer),
            None => return,
        };
        if !can_receive {
            debug!(
                node = self.config.node_id,
                group = self.config.group_id,
                observer,
                "observer send queue full; skipping (advisory backpressure)"
            );
            return;
        }

        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        let obs_state = match leader
            .observer_states
            .iter()
            .find(|(id, _)| *id == observer)
        {
            Some((_, s)) => s.clone(),
            None => return,
        };

        let next_index = obs_state.next_index;
        let prev_log_index = next_index.saturating_sub(1);

        let prev_log_term = match self.log.term_at(prev_log_index) {
            Some(term) => term,
            None => {
                debug!(
                    node = self.config.node_id,
                    group = self.config.group_id,
                    observer,
                    next_index,
                    snapshot_index = self.log.snapshot_index(),
                    "observer needs snapshot (log compacted)"
                );
                self.ready.snapshots_needed.push(observer);
                return;
            }
        };

        let entries = if next_index <= self.log.last_index() {
            match self.log.entries_range(next_index, self.log.last_index()) {
                Ok(slice) => slice.to_vec(),
                Err(crate::error::RaftError::LogCompacted { .. }) => {
                    debug!(
                        node = self.config.node_id,
                        group = self.config.group_id,
                        observer,
                        next_index,
                        "observer needs snapshot (entries compacted)"
                    );
                    self.ready.snapshots_needed.push(observer);
                    return;
                }
                Err(_) => vec![],
            }
        } else {
            vec![]
        };

        let entry_count = entries.len() as u32;

        self.ready.messages.push((
            observer,
            crate::message::AppendEntriesRequest {
                term: self.hard_state.current_term,
                leader_id: self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.volatile.commit_index,
                group_id: self.config.group_id,
            },
        ));

        // Increment pending count for advisory backpressure tracking.
        if let Some(ls) = self.leader_state.as_mut()
            && let Some(state) = ls.observer_state_mut(observer)
        {
            state.pending_count = state.pending_count.saturating_add(entry_count.max(1));
        }
    }

    /// Try to advance `commit_index` based on the voter quorum only.
    ///
    /// Learners' `match_index` is tracked (so we know when they are caught
    /// up for promotion) but intentionally excluded from this calculation
    /// so adding a learner never weakens the commit quorum.
    pub(super) fn try_advance_commit_index(&mut self) {
        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        let last = self.log.last_index();
        for n in (self.volatile.commit_index + 1..=last).rev() {
            let term_at_n = match self.log.term_at(n) {
                Some(t) => t,
                None => continue,
            };

            if term_at_n != self.hard_state.current_term {
                continue;
            }

            let mut count = 1u64; // self counts.
            for &peer in &self.config.peers {
                if leader.match_index_for(peer) >= n {
                    count += 1;
                }
            }

            if count as usize >= self.config.quorum() {
                self.volatile.commit_index = n;
                self.collect_committed_entries();
                break;
            }
        }
    }

    pub(super) fn collect_committed_entries(&mut self) {
        let from = self.volatile.last_applied + 1;
        let to = self.volatile.commit_index;
        if from > to {
            return;
        }
        if let Ok(entries) = self.log.entries_range(from, to) {
            self.ready.committed_entries.extend(entries.iter().cloned());
        }
    }

    pub(super) fn persist_hard_state(&mut self) {
        self.ready.hard_state = Some(self.hard_state.clone());
    }

    pub(super) fn reset_election_timeout(&mut self) {
        let mut rng = rand::rng();
        let min = self.config.election_timeout_min.as_millis() as u64;
        let max = self.config.election_timeout_max.as_millis() as u64;
        let timeout = Duration::from_millis(rng.random_range(min..=max));
        self.election_deadline = Instant::now() + timeout;
    }
}
