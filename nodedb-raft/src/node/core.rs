// SPDX-License-Identifier: BUSL-1.1

//! `RaftNode` struct, constructors, simple accessors, `tick`, and `propose`.
//!
//! Membership mutation (add/remove voter, add/remove/promote learner) lives
//! in [`super::membership`]. State transitions (election, `become_leader`,
//! replication) live in [`super::internal`]. RPC handlers live in
//! [`super::rpc`].

use std::collections::HashSet;
use std::time::Instant;

use crate::error::{RaftError, Result};
use crate::log::RaftLog;
use crate::message::{AppendEntriesRequest, LogEntry};
use crate::state::{HardState, LeaderState, NodeRole, VolatileState};
use crate::storage::LogStorage;

use super::config::RaftConfig;

/// Output actions produced by a tick or RPC handler.
///
/// The caller (Multi-Raft coordinator) is responsible for executing these
/// via the transport and applying committed entries to the state machine.
#[derive(Debug, Default)]
pub struct Ready {
    /// Hard state to persist (if changed).
    pub hard_state: Option<HardState>,
    /// Entries to send to specific peers (peer_id, request).
    pub messages: Vec<(u64, AppendEntriesRequest)>,
    /// Vote requests to send (peer_id, request).
    pub vote_requests: Vec<(u64, crate::message::RequestVoteRequest)>,
    /// Newly committed entries to apply to the state machine.
    pub committed_entries: Vec<LogEntry>,
    /// Peers that need an InstallSnapshot RPC because their next_index
    /// falls behind the leader's snapshot_index (log compacted).
    pub snapshots_needed: Vec<u64>,
}

impl Ready {
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.messages.is_empty()
            && self.vote_requests.is_empty()
            && self.committed_entries.is_empty()
            && self.snapshots_needed.is_empty()
    }
}

/// A single Raft group's state machine.
///
/// This is a deterministic, event-driven core. It does NOT own any threads
/// or timers — the caller drives it via `tick()` and RPC handler methods,
/// and reads output via `take_ready()`.
pub struct RaftNode<S: LogStorage> {
    pub(super) config: RaftConfig,
    pub(super) role: NodeRole,
    pub(super) hard_state: HardState,
    pub(super) volatile: VolatileState,
    pub(super) leader_state: Option<LeaderState>,
    pub(super) log: RaftLog<S>,
    /// When the next election timeout fires.
    pub(super) election_deadline: Instant,
    /// When the next heartbeat should be sent (leader only).
    pub(super) heartbeat_deadline: Instant,
    /// Votes received in current election.
    pub(super) votes_received: HashSet<u64>,
    /// Pending ready output.
    pub(super) ready: Ready,
    /// Known leader ID (0 = unknown).
    pub(super) leader_id: u64,
}

impl<S: LogStorage> RaftNode<S> {
    /// Create a new Raft node. Call `restore()` before ticking.
    ///
    /// If `config.starts_as_learner` is `true`, the node boots in the
    /// `Learner` role and will never run an election timeout or become a
    /// leader until it is promoted via `promote_self_to_voter`.
    pub fn new(config: RaftConfig, storage: S) -> Self {
        let now = Instant::now();
        let role = if config.starts_as_observer {
            NodeRole::Observer
        } else if config.starts_as_learner {
            NodeRole::Learner
        } else {
            NodeRole::Follower
        };
        Self {
            log: RaftLog::new(storage),
            role,
            hard_state: HardState::new(),
            volatile: VolatileState::new(),
            leader_state: None,
            election_deadline: now + config.election_timeout_max,
            heartbeat_deadline: now,
            votes_received: HashSet::new(),
            ready: Ready::default(),
            leader_id: 0,
            config,
        }
    }

    /// Restore state from persistent storage. Must be called before ticking.
    pub fn restore(&mut self) -> Result<()> {
        self.hard_state = self.log.storage().load_hard_state()?;
        self.log.restore()?;
        self.reset_election_timeout();
        Ok(())
    }

    pub fn node_id(&self) -> u64 {
        self.config.node_id
    }

    pub fn group_id(&self) -> u64 {
        self.config.group_id
    }

    pub fn role(&self) -> NodeRole {
        self.role
    }

    pub fn leader_id(&self) -> u64 {
        self.leader_id
    }

    pub fn current_term(&self) -> u64 {
        self.hard_state.current_term
    }

    pub fn commit_index(&self) -> u64 {
        self.volatile.commit_index
    }

    pub fn last_applied(&self) -> u64 {
        self.volatile.last_applied
    }

    pub fn last_log_index(&self) -> u64 {
        self.log.last_index()
    }

    /// Override election deadline (for testing).
    pub fn election_deadline_override(&mut self, deadline: Instant) {
        self.election_deadline = deadline;
    }

    /// Take the pending `Ready` output. Caller must execute messages,
    /// persist hard state, and apply committed entries.
    pub fn take_ready(&mut self) -> Ready {
        std::mem::take(&mut self.ready)
    }

    /// Advance `last_applied` after the caller has applied entries.
    pub fn advance_applied(&mut self, applied_to: u64) {
        self.volatile.last_applied = applied_to;
    }

    /// Query a peer's match_index from the leader's replication state.
    /// Returns `None` if this node is not the leader or the peer is unknown.
    pub fn match_index_for(&self, peer: u64) -> Option<u64> {
        self.leader_state
            .as_ref()
            .map(|ls| ls.match_index_for(peer))
    }

    pub fn log_snapshot_index(&self) -> u64 {
        self.log.snapshot_index()
    }

    pub fn log_snapshot_term(&self) -> u64 {
        self.log.snapshot_term()
    }

    /// Return committed log entries in the inclusive range `[lo, hi]`.
    ///
    /// Clamps `hi` to `commit_index` so callers that pass `u64::MAX` never
    /// read uncommitted entries.  Returns `Err(RaftError::LogCompacted)` if
    /// `lo` has already been compacted into a snapshot.
    pub fn log_entries_range(
        &self,
        lo: u64,
        hi: u64,
    ) -> crate::error::Result<&[crate::message::LogEntry]> {
        let hi = hi.min(self.volatile.commit_index);
        self.log.entries_range(lo, hi)
    }

    /// Current voter peer list (excluding self).
    pub fn peers(&self) -> &[u64] {
        &self.config.peers
    }

    /// Current voter peer list — alias for `peers()`, clearer at call sites
    /// that need to distinguish voters from learners.
    pub fn voters(&self) -> &[u64] {
        &self.config.peers
    }

    /// Current learner peer list (excluding self).
    pub fn learners(&self) -> &[u64] {
        &self.config.learners
    }

    /// Current observer peer list tracked by this leader (excluding self).
    pub fn observers(&self) -> &[u64] {
        &self.config.observers
    }

    /// Whether `peer` is currently tracked as a learner in this group.
    pub fn is_learner_peer(&self, peer: u64) -> bool {
        self.config.learners.contains(&peer)
    }

    /// Drive time-based events: election timeout, heartbeat.
    pub fn tick(&mut self) {
        let now = Instant::now();

        match self.role {
            NodeRole::Follower | NodeRole::Candidate => {
                if now >= self.election_deadline {
                    self.start_election();
                }
            }
            NodeRole::Leader => {
                if now >= self.heartbeat_deadline {
                    self.replicate_to_all();
                    self.heartbeat_deadline = now + self.config.heartbeat_interval;
                }
            }
            NodeRole::Learner => {
                // Learners never run election timeouts. They catch up
                // passively via AppendEntries from the leader.
            }
            NodeRole::Observer => {
                // Observers never run election timeouts. They receive entries
                // from the source leader and apply them locally. Acks are
                // advisory and never gate commit on the source.
            }
        }
    }

    /// Propose a new entry (leader only). Returns the log index.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<u64> {
        if self.role != NodeRole::Leader {
            return Err(RaftError::NotLeader {
                leader_hint: if self.leader_id != 0 {
                    Some(self.leader_id)
                } else {
                    None
                },
            });
        }

        let index = self.log.last_index() + 1;
        let entry = LogEntry {
            term: self.hard_state.current_term,
            index,
            data,
        };

        self.log.append(entry)?;
        self.replicate_to_all();

        // Single-voter cluster: commit immediately. Learners do not count.
        if self.config.cluster_size() == 1 {
            self.volatile.commit_index = index;
            self.collect_committed_entries();
        }

        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;
    use std::time::Duration;

    fn test_config(node_id: u64, peers: Vec<u64>) -> RaftConfig {
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
        }
    }

    #[test]
    fn single_node_election() {
        let config = test_config(1, vec![]);
        let mut node = RaftNode::new(config, MemStorage::new());

        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();

        assert_eq!(node.role(), NodeRole::Leader);
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.leader_id(), 1);
    }

    #[test]
    fn single_node_propose_and_commit() {
        let config = test_config(1, vec![]);
        let mut node = RaftNode::new(config, MemStorage::new());
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);

        let ready = node.take_ready();
        assert!(!ready.committed_entries.is_empty());
        node.advance_applied(ready.committed_entries.last().unwrap().index);

        let idx = node.propose(b"hello".to_vec()).unwrap();
        assert_eq!(idx, 2);

        let ready = node.take_ready();
        assert_eq!(ready.committed_entries.len(), 1);
        assert_eq!(ready.committed_entries[0].data, b"hello");
    }

    #[test]
    fn propose_as_follower_fails() {
        let config = test_config(1, vec![2, 3]);
        let node = &mut RaftNode::new(config, MemStorage::new());
        let err = node.propose(b"data".to_vec()).unwrap_err();
        assert!(matches!(err, RaftError::NotLeader { .. }));
    }

    #[test]
    fn snapshot_needed_after_compaction() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        let _ready = node.take_ready();
        let resp = crate::message::RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        node.handle_request_vote_response(2, &resp);
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();

        for i in 0..9 {
            node.propose(vec![i]).unwrap();
        }
        let _ = node.take_ready();

        node.log.apply_snapshot(8, 1);

        node.replicate_to_all();
        let ready = node.take_ready();

        assert!(
            !ready.snapshots_needed.is_empty(),
            "expected snapshots_needed to be non-empty"
        );
    }

    #[test]
    fn starts_as_learner_role() {
        let mut cfg = test_config(2, vec![1]);
        cfg.starts_as_learner = true;
        let node = RaftNode::new(cfg, MemStorage::new());
        assert_eq!(node.role(), NodeRole::Learner);
    }

    #[test]
    fn learner_tick_does_not_start_election() {
        let mut cfg = test_config(2, vec![1]);
        cfg.starts_as_learner = true;
        let mut node = RaftNode::new(cfg, MemStorage::new());
        // Force "election deadline" in the past: a follower would immediately
        // start an election, but a learner must ignore it.
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        assert_eq!(node.role(), NodeRole::Learner);
        assert_eq!(node.current_term(), 0);
    }
}
