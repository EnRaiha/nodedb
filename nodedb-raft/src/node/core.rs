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
use crate::message::{AppendEntriesRequest, LogEntry, TimeoutNowRequest};
use crate::state::{HardState, LeaderState, LeadershipTransfer, NodeRole, VolatileState};
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
    /// `TimeoutNow` triggers to send to leadership-transfer targets
    /// (dest_node_id, request). Drained and dispatched by the caller; until a
    /// caller wires the transport this field is simply ignored.
    pub timeout_now: Vec<(u64, TimeoutNowRequest)>,
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
            && self.timeout_now.is_empty()
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
    /// In-progress leadership transfer, if any (leader-side, volatile).
    pub(super) leadership_transfer: Option<LeadershipTransfer>,
    /// Highest log index whose apply is durable on this node, mirroring
    /// `LogStorage::save_applied_index`.
    ///
    /// Deliberately distinct from `volatile.last_applied`, which advances the
    /// moment an entry is DELIVERED to the state machine. This index only
    /// advances once that entry's effects are durable, which makes it two
    /// things `last_applied` cannot be: the floor a restart resumes delivery
    /// from, and the ceiling compaction may discard up to.
    pub(super) durable_applied: u64,
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
            leadership_transfer: None,
            durable_applied: 0,
            config,
        }
    }

    /// Restore state from persistent storage. Must be called before ticking.
    ///
    /// Seeds `volatile.last_applied` from the durable applied index so
    /// delivery resumes at the first entry whose effects are NOT already
    /// durable. Storage written before the durable index existed reports 0 and
    /// degrades to a full replay of the retained log.
    pub fn restore(&mut self) -> Result<()> {
        self.hard_state = self.log.storage().load_hard_state()?;
        self.durable_applied = self.log.storage().load_applied_index()?;
        self.volatile = VolatileState::restored(self.durable_applied);
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

    /// Whether a leadership transfer is currently in progress.
    pub fn leadership_transfer_in_progress(&self) -> bool {
        self.leadership_transfer.is_some()
    }

    /// Override the in-progress leadership-transfer deadline (for testing).
    /// No-op when no transfer is pending.
    pub fn transfer_deadline_override(&mut self, deadline: Instant) {
        if let Some(t) = self.leadership_transfer.as_mut() {
            t.deadline = deadline;
        }
    }

    /// Take the pending `Ready` output. Caller must execute messages,
    /// persist hard state, and apply committed entries.
    pub fn take_ready(&mut self) -> Ready {
        std::mem::take(&mut self.ready)
    }

    /// Durably persist HardState iff it changed since the last persist.
    /// Must run before a vote grant / vote requests leave this node
    /// (Raft: persist voted_for/current_term to stable storage before replying).
    pub fn persist_hard_state_if_dirty(&mut self) -> crate::error::Result<()> {
        if self.ready.hard_state.is_some() {
            self.log.storage_mut().save_hard_state(&self.hard_state)?;
            self.ready.hard_state = None;
        }
        Ok(())
    }

    /// Advance `last_applied` after the caller has applied entries.
    ///
    /// This is the DELIVERY watermark: it advances as entries are handed to
    /// the state machine, before their effects are necessarily durable. Use
    /// [`Self::save_durable_applied_index`] for the durability floor.
    pub fn advance_applied(&mut self, applied_to: u64) {
        self.volatile.last_applied = applied_to;
    }

    /// Highest log index whose apply is durable on this node.
    pub fn durable_applied_index(&self) -> u64 {
        self.durable_applied
    }

    /// The lowest log index still available in the retained (post-compaction)
    /// log — `snapshot_index + 1`. A committed-entry read below this yields
    /// [`RaftError::LogCompacted`]. Used to arm a Calvin scheduler catch-up from
    /// the earliest replayable index so the drain never faults on a compacted
    /// range.
    pub fn first_available_index(&self) -> u64 {
        self.log.snapshot_index() + 1
    }

    /// Persist `index` as the durable applied floor.
    ///
    /// The caller MUST only pass an index whose state-machine effects are
    /// already durable — for data groups, an index whose redo record the WAL
    /// has fsynced. The next boot resumes delivery at `index + 1`, so an index
    /// saved ahead of durability silently drops the entries in between.
    ///
    /// Monotonic: an `index` at or below the current floor is a no-op, so an
    /// out-of-order or retrying caller can never move the floor backwards and
    /// re-expose an entry to a second apply.
    pub fn save_durable_applied_index(&mut self, index: u64) -> Result<()> {
        if index <= self.durable_applied {
            return Ok(());
        }
        self.log.storage_mut().save_applied_index(index)?;
        self.durable_applied = index;
        Ok(())
    }

    /// Auto-compaction threshold: entries retained past `snapshot_index`
    /// before the log is compacted. `None` disables auto-compaction.
    pub fn log_compaction_threshold(&self) -> Option<u64> {
        self.config.log_compaction_threshold
    }

    /// Compact the log up to `up_to_index` after the DATA-PLANE state
    /// machine has durably applied every entry `<= up_to_index`.
    ///
    /// Resolves the term at `up_to_index` from the in-memory log and
    /// calls [`RaftLog::apply_snapshot`], which discards entries
    /// `<= up_to_index` and persists the new snapshot boundary. The
    /// snapshot bytes themselves are NOT materialized here — the
    /// `SnapshotBuilder` hook rebuilds them on demand from live engine
    /// state when a lagging follower needs an `InstallSnapshot`.
    ///
    /// # Safety / gating
    ///
    /// The CALLER MUST pass an `up_to_index` that the DATA-PLANE state
    /// machine has durably applied. Compacting past a data-plane-unapplied
    /// index would let the `SnapshotBuilder` serialize incomplete state.
    /// The sole caller path (`run_apply_loop` → [`Self::maybe_compact_log`])
    /// guarantees this: it only compacts an index after the SPSC round-trip
    /// that applies that entry to the Data Plane has returned.
    ///
    /// This method additionally clamps to the DURABLE applied index
    /// (returning [`RaftError::CompactionAheadOfApplied`] otherwise).
    /// Deliberately not `volatile.last_applied`: that advances at
    /// commit/enqueue time, so clamping to it would let compaction discard
    /// entries whose redo record is not yet fsynced — losing the only recovery
    /// source for the memory-only engines.
    ///
    /// Returns `Ok(false)` when there is nothing to compact
    /// (`up_to_index <= snapshot_index`). Returns
    /// `Err(RaftError::LogCompacted)` if the term at `up_to_index` is no
    /// longer available (already compacted away).
    pub fn compact_log_up_to(&mut self, up_to_index: u64) -> Result<bool> {
        if up_to_index <= self.log.snapshot_index() {
            return Ok(false);
        }
        if up_to_index > self.durable_applied {
            return Err(RaftError::CompactionAheadOfApplied {
                requested: up_to_index,
                last_applied: self.durable_applied,
            });
        }
        let term = self
            .log
            .term_at(up_to_index)
            .ok_or(RaftError::LogCompacted {
                requested: up_to_index,
                first_available: self.log.snapshot_index() + 1,
            })?;
        self.log.apply_snapshot(up_to_index, term);
        Ok(true)
    }

    /// Check the configured auto-compaction threshold against the
    /// data-plane applied index `applied_index` and compact the log up to
    /// `applied_index` if the retained-entry count has reached the
    /// threshold.
    ///
    /// `applied_index` is the index the DATA-PLANE state machine has
    /// durably applied up to (NOT raft's commit index) — see
    /// [`RaftConfig::log_compaction_threshold`]. No-op when the threshold
    /// is `None` or the retained count is below it.
    ///
    /// Returns `Ok(true)` when a compaction was performed.
    pub fn maybe_compact_log(&mut self, applied_index: u64) -> Result<bool> {
        let Some(threshold) = self.config.log_compaction_threshold else {
            return Ok(false);
        };
        let snapshot_index = self.log.snapshot_index();
        if applied_index <= snapshot_index {
            return Ok(false);
        }
        if applied_index - snapshot_index < threshold {
            return Ok(false);
        }
        // Never compact past an entry whose apply is not yet durable.
        let up_to = applied_index.min(self.durable_applied);
        self.compact_log_up_to(up_to)
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
                // Abort an in-progress leadership transfer whose deadline has
                // passed: clear the volatile state so proposals unblock and
                // the leader resumes normal operation.
                let transfer_expired = self
                    .leadership_transfer
                    .as_ref()
                    .is_some_and(|t| now >= t.deadline);
                if transfer_expired {
                    self.leadership_transfer = None;
                }
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

        // While a leadership transfer is pending the leader holds the log
        // frontier fixed so the target can catch up to it. Reject new
        // proposals (retryable) until the transfer completes or aborts.
        if self.leadership_transfer.is_some() {
            return Err(RaftError::LeadershipTransferInProgress);
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
            log_compaction_threshold: None,
        }
    }

    /// Drive a single-voter node to leadership and apply its initial
    /// election no-op so `last_applied` tracks the log.
    fn leader_with_applied_noop(config: RaftConfig) -> RaftNode<MemStorage> {
        let mut node = RaftNode::new(config, MemStorage::new());
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);
        let ready = node.take_ready();
        if let Some(last) = ready.committed_entries.last() {
            node.advance_applied(last.index);
        }
        node
    }

    /// Stand in for a data-plane apply that reached durability: advance the
    /// delivery watermark AND the durable floor, as the apply loop does once
    /// the write funnel's fsync barrier has returned.
    fn apply_durably(node: &mut RaftNode<MemStorage>, index: u64) {
        node.advance_applied(index);
        node.save_durable_applied_index(index).unwrap();
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
    fn threshold_some_compacts_after_enough_applied() {
        // Single-voter group so every propose commits immediately.
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(4);
        let mut node = leader_with_applied_noop(cfg);

        // Propose entries and apply each as the data plane would.
        for _ in 0..8 {
            let idx = node.propose(b"write".to_vec()).unwrap();
            let _ = node.take_ready();
            apply_durably(&mut node, idx);

            // Trigger gated on the data-plane applied watermark (= idx here).
            node.maybe_compact_log(idx).unwrap();
        }

        let snap = node.log_snapshot_index();
        // With threshold 4, the log keeps at most 4 entries past the
        // snapshot boundary; the boundary must have advanced.
        assert!(
            snap > 0,
            "snapshot_index should have advanced past 0, got {snap}"
        );
        assert!(
            node.last_log_index() - snap <= 4,
            "retained entries ({}) must be <= threshold (4)",
            node.last_log_index() - snap
        );

        // Entries at or before the snapshot boundary are discarded.
        assert!(
            node.log.entry_at(snap).is_none(),
            "entry at snapshot boundary must be gone"
        );
        assert!(
            node.log.entries_range(1, snap).is_err(),
            "range into compacted region must fail"
        );
    }

    #[test]
    fn threshold_none_never_compacts() {
        let cfg = test_config(1, vec![]); // log_compaction_threshold: None
        let mut node = leader_with_applied_noop(cfg);

        for _ in 0..12 {
            let idx = node.propose(b"write".to_vec()).unwrap();
            let _ = node.take_ready();
            apply_durably(&mut node, idx);
            // No-op: threshold is None.
            assert!(!node.maybe_compact_log(idx).unwrap());
        }

        assert_eq!(
            node.log_snapshot_index(),
            0,
            "no compaction must occur when threshold is None"
        );
        // Every entry from index 1 is still present.
        assert!(node.log.entry_at(1).is_some());
        assert!(node.log.entries_range(1, node.last_log_index()).is_ok());
    }

    #[test]
    fn compact_log_up_to_rejects_ahead_of_applied() {
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(2);
        let mut node = leader_with_applied_noop(cfg);

        let idx = node.propose(b"write".to_vec()).unwrap();
        let _ = node.take_ready();
        // Deliberately do NOT apply past the noop — the data plane has not
        // applied `idx` yet.
        let err = node.compact_log_up_to(idx).unwrap_err();
        assert!(matches!(err, RaftError::CompactionAheadOfApplied { .. }));
    }

    /// Compaction gates on the DURABLE applied floor, not the delivery
    /// watermark. An entry that has been handed to the state machine but whose
    /// redo is not yet fsynced must NOT be compacted away: the log is the only
    /// thing that can rebuild the memory-only engines for it.
    #[test]
    fn compact_log_up_to_rejects_delivered_but_not_durable() {
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(2);
        let mut node = leader_with_applied_noop(cfg);

        let idx = node.propose(b"write".to_vec()).unwrap();
        let _ = node.take_ready();
        // Delivery watermark advances; the durable floor does not.
        node.advance_applied(idx);

        let err = node.compact_log_up_to(idx).unwrap_err();
        assert!(matches!(err, RaftError::CompactionAheadOfApplied { .. }));

        // Once the apply is durable the same index compacts.
        node.save_durable_applied_index(idx).unwrap();
        assert!(node.compact_log_up_to(idx).unwrap());
    }

    /// The durable floor never moves backwards, however a caller retries.
    #[test]
    fn durable_applied_index_is_monotonic() {
        let mut node = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        assert_eq!(node.durable_applied_index(), 0);

        node.save_durable_applied_index(5).unwrap();
        assert_eq!(node.durable_applied_index(), 5);

        node.save_durable_applied_index(3).unwrap();
        assert_eq!(node.durable_applied_index(), 5);
    }

    /// A restart resumes delivery ABOVE the durable floor: entries whose
    /// effects are already durable must never be handed to the state machine a
    /// second time.
    #[test]
    fn restore_seeds_last_applied_from_durable_index() {
        let mut storage = MemStorage::new();
        storage
            .append(&[
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
                LogEntry {
                    term: 1,
                    index: 3,
                    data: b"c".to_vec(),
                },
            ])
            .unwrap();
        storage.save_applied_index(2).unwrap();

        let mut node = RaftNode::new(test_config(1, vec![]), storage);
        node.restore().unwrap();
        assert_eq!(node.last_applied(), 2);
        assert_eq!(node.durable_applied_index(), 2);

        // Learning the commit index re-delivers ONLY the tail above the floor.
        node.volatile.commit_index = 3;
        node.collect_committed_entries();
        let ready = node.take_ready();
        assert_eq!(ready.committed_entries.len(), 1);
        assert_eq!(ready.committed_entries[0].index, 3);
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
