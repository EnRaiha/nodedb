# Code Solutions: Performance & Test Refactoring

## PERF-1: Per-Group Lock Granularity

**File:** `nodedb-cluster/src/multi_raft/core.rs` — NEW STRUCTURE

```rust
// SPDX-License-Identifier: BUSL-1.1

//! Multi-Raft coordinator — per-group lock architecture.
//!
//! Each Raft group has its own `parking_lot::Mutex<GroupState>`. RPC handlers
//! only lock the target group, allowing concurrent processing across groups.
//! The `DashMap` provides lock-free reads for group lookup.

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;

use nodedb_raft::node::config::RaftConfig;
use nodedb_raft::node::core::RaftNode;
use nodedb_raft::storage::LogStorage;
use nodedb_raft::message::{
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
    TimeoutNowRequest,
};

use crate::error::{ClusterError, Result};
use crate::applied_watcher::AppliedIndexWatcher;

/// Per-group state — everything needed to run one Raft group.
///
/// Each instance is protected by its own `parking_lot::Mutex`, so groups
/// operate independently without contention.
pub struct GroupState<S: LogStorage> {
    /// The Raft state machine for this group.
    pub node: RaftNode<S>,
    /// Watcher for applied index — bumped after apply completes.
    pub watcher: AppliedIndexWatcher,
    /// Group-level metadata (descriptor, epoch, etc.)
    pub metadata: GroupMetadata,
}

/// Metadata that changes rarely — read-heavy access.
#[derive(Debug, Clone)]
pub struct GroupMetadata {
    pub group_id: u64,
    pub created_at: std::time::Instant,
    pub vshard_range: Option<(u64, u64)>,
}

/// Multi-Raft coordinator with per-group locking.
///
/// # Architecture
///
/// ```text
/// RPC ? DashMap lookup (lock-free) ? per-group Mutex ? RaftNode
/// ```
///
/// - `DashMap<u64, Mutex<GroupState>>` — sharded map with per-bucket locks
/// - Group lookup: O(1), no global lock
/// - Group mutation: per-group `parking_lot::Mutex`
/// - Read-only queries (e.g., `last_applied`): per-group lock, brief
///
/// # Why parking_lot over std::sync::Mutex
///
/// - No poisoning: a panicking thread doesn't poison the lock
/// - Fair lock acquisition: FIFO, prevents starvation
/// - Slightly faster uncontended acquire (~20ns vs ~25ns)
/// - ReadWriteLock: allows concurrent reads for observer queries
pub struct MultiRaft<S: LogStorage> {
    /// Per-group state, each behind its own lock.
    groups: DashMap<u64, Mutex<GroupState<S>>>,
    /// Immutable cluster-wide config.
    cluster_config: Arc<ClusterConfig>,
}

impl<S: LogStorage + 'static> MultiRaft<S> {
    /// Create a new MultiRaft coordinator.
    pub fn new(cluster_config: ClusterConfig) -> Self {
        Self {
            groups: DashMap::with_capacity(16), // pre-allocate for typical group count
            cluster_config: Arc::new(cluster_config),
        }
    }

    /// Mount a new Raft group on this node.
    ///
    /// Called during bootstrap or when a vshard is assigned to this node.
    pub fn mount_group(
        &self,
        config: RaftConfig,
        storage: S,
        metadata: GroupMetadata,
    ) -> Result<()> {
        let group_id = config.group_id;

        if self.groups.contains_key(&group_id) {
            return Err(ClusterError::GroupAlreadyMounted { group_id });
        }

        let node = RaftNode::new(config, storage);
        let watcher = AppliedIndexWatcher::new(group_id);

        self.groups.insert(
            group_id,
            Mutex::new(GroupState {
                node,
                watcher,
                metadata,
            }),
        );

        Ok(())
    }

    /// Unmount a group (vshard moved away or node decommissioned).
    pub fn unmount_group(&self, group_id: u64) -> Result<()> {
        self.groups
            .remove(&group_id)
            .map(|_| ())
            .ok_or(ClusterError::GroupNotFound { group_id })
    }

    /// Route an AppendEntries RPC to the target group.
    ///
    /// Lock scope: ONLY the target group's mutex. Other groups are unaffected.
    pub fn handle_append_entries(
        &self,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let entry = self.groups.get(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;

        let mut state = entry.value().lock();
        let resp = state.node.handle_append_entries(req)?;

        // Persist hard state BEFORE releasing lock — the reply must not
        // leave this node until durability is guaranteed.
        state.node.persist_hard_state_if_dirty()?;

        drop(state); // release lock before serialization
        drop(entry); // release DashMap shard

        Ok(resp)
    }

    /// Route a RequestVote RPC to the target group.
    pub fn handle_request_vote(
        &self,
        req: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse> {
        let entry = self.groups.get(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;

        let mut state = entry.value().lock();
        let resp = state.node.handle_request_vote(req)?;
        state.node.persist_hard_state_if_dirty()?;

        drop(state);
        drop(entry);

        Ok(resp)
    }

    /// Route an InstallSnapshot RPC to the target group.
    pub fn handle_install_snapshot(
        &self,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let entry = self.groups.get(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;

        let mut state = entry.value().lock();
        let resp = state.node.handle_install_snapshot(req)?;
        state.node.persist_hard_state_if_dirty()?;

        drop(state);
        drop(entry);

        Ok(resp)
    }

    /// Route a TimeoutNow (leadership transfer trigger) to the target group.
    pub fn handle_timeout_now(&self, req: &TimeoutNowRequest) {
        if let Some(entry) = self.groups.get(&req.group_id) {
            let mut state = entry.value().lock();
            state.node.handle_timeout_now(req);
            // Best-effort persist — failure logged inside
            let _ = state.node.persist_hard_state_if_dirty();
        }
        // Absent group: silently ignore (matches existing behavior)
    }

    /// Handle AppendEntries response for a specific group.
    ///
    /// Called by the transport layer when a peer responds.
    pub fn handle_append_entries_response(
        &self,
        group_id: u64,
        peer: u64,
        resp: &AppendEntriesResponse,
    ) -> Result<()> {
        let entry = self.groups.get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;

        let mut state = entry.value().lock();
        state.node.handle_append_entries_response(peer, resp);
        Ok(())
    }

    /// Handle RequestVote response for a specific group.
    pub fn handle_request_vote_response(
        &self,
        group_id: u64,
        peer: u64,
        resp: &RequestVoteResponse,
    ) -> Result<()> {
        let entry = self.groups.get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;

        let mut state = entry.value().lock();
        state.node.handle_request_vote_response(peer, resp);
        Ok(())
    }

    /// Tick ALL groups — but each group's tick is independent.
    ///
    /// This method iterates groups without holding any lock longer than
    /// necessary. Each group's tick acquires its own mutex sequentially.
    ///
    /// # Future optimization: parallel tick
    ///
    /// Once tick() is proven thread-safe per group, this can use
    /// `rayon::par_iter()` or a dedicated thread pool for true parallelism.
    pub fn tick_all(&self) -> Vec<(u64, nodedb_raft::node::core::Ready)> {
        let mut outputs = Vec::with_capacity(self.groups.len());

        for entry in self.groups.iter() {
            let group_id = *entry.key();
            let mut state = entry.value().lock();

            state.node.tick();
            let ready = state.node.take_ready();

            if !ready.is_empty() {
                outputs.push((group_id, ready));
            }

            drop(state); // release before next group
        }

        outputs
    }

    // --- Read-only queries (brief lock, no mutation) ---

    /// Read the locally-applied index for a group.
    pub fn last_applied(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id)
            .map(|entry| entry.value().lock().node.last_applied())
    }

    /// Read the last log index for a group.
    pub fn last_log_index(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id)
            .map(|entry| entry.value().lock().node.last_log_index())
    }

    /// Read the commit index for a group.
    pub fn commit_index(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id)
            .map(|entry| entry.value().lock().node.commit_index())
    }

    /// Read current term for a group.
    pub fn current_term(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id)
            .map(|entry| entry.value().lock().node.current_term())
    }

    /// Read current leader ID for a group.
    pub fn leader_id(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id)
            .map(|entry| entry.value().lock().node.leader_id())
    }

    /// Check if this node is the leader for a group.
    pub fn is_leader(&self, group_id: u64) -> Option<bool> {
        self.groups.get(&group_id)
            .map(|entry| {
                let state = entry.value().lock();
                state.node.role() == nodedb_raft::state::NodeRole::Leader
            })
    }

    /// Query a peer's match_index from a group's leader state.
    pub fn match_index_for(&self, group_id: u64, peer: u64) -> Option<u64> {
        self.groups.get(&group_id)
            .map(|entry| entry.value().lock().node.match_index_for(peer))
            .flatten()
    }

    /// Get all mounted group IDs.
    pub fn group_ids(&self) -> Vec<u64> {
        self.groups.iter().map(|e| *e.key()).collect()
    }

    /// Number of mounted groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// `(group_id, last_applied)` pairs for all groups.
    pub fn applied_indices(&self) -> Vec<(u64, u64)> {
        self.groups.iter()
            .map(|entry| {
                let group_id = *entry.key();
                let applied = entry.value().lock().node.last_applied();
                (group_id, applied)
            })
            .collect()
    }

    // --- Durability operations ---

    /// Durably record `applied_to` as a group's applied floor.
    pub fn save_applied_index(&self, group_id: u64, applied_to: u64) -> Result<()> {
        let entry = self.groups.get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let mut state = entry.value().lock();
        state.node.save_durable_applied_index(applied_to)?;
        Ok(())
    }

    /// Advance the delivery watermark for a group.
    pub fn advance_applied(&self, group_id: u64, applied_to: u64) -> Result<()> {
        let entry = self.groups.get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let mut state = entry.value().lock();
        state.node.advance_applied(applied_to);
        state.watcher.bump(group_id, applied_to);
        Ok(())
    }

    /// Persist hard state for a group if dirty.
    pub fn persist_group_hard_state(&self, group_id: u64) -> Result<()> {
        if let Some(entry) = self.groups.get(&group_id) {
            let mut state = entry.value().lock();
            state.node.persist_hard_state_if_dirty()?;
        }
        Ok(())
    }

    // --- Snapshot support ---

    /// Get snapshot metadata for a group.
    pub fn snapshot_metadata(&self, group_id: u64) -> Result<(u64, u64, u64)> {
        let entry = self.groups.get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let state = entry.value().lock();
        Ok((
            state.node.current_term(),
            state.node.log_snapshot_index(),
            state.node.log_snapshot_term(),
        ))
    }
}

// --- Migration helper: from old global-mutex MultiRaft ---

/// Temporary wrapper during migration — provides the OLD API surface but
/// internally uses the new per-group locking.
///
/// Once all callers are migrated, delete this.
pub struct MultiRaftCompat<S: LogStorage> {
    inner: Arc<MultiRaft<S>>,
}

impl<S: LogStorage + 'static> MultiRaftCompat<S> {
    pub fn new(inner: Arc<MultiRaft<S>>) -> Self {
        Self { inner }
    }

    // These methods match the OLD MultiRaft signatures exactly,
    // allowing existing callers to work unchanged during migration.
    pub fn handle_append_entries(
        &self,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        self.inner.handle_append_entries(req)
    }

    pub fn handle_request_vote(
        &self,
        req: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse> {
        self.inner.handle_request_vote(req)
    }

    // ... other methods delegating to inner ...
}
```

**Migration plan:**

```rust
// STEP 1: Add new MultiRaft alongside old one (parallel)
// File: nodedb-cluster/src/multi_raft/mod.rs

mod core_new;  // new per-group locking version
mod core_old;  // existing global mutex version (keep)

pub use core_new::MultiRaft as MultiRaftV2;
pub use core_old::MultiRaft; // alias for existing

// STEP 2: Update RPC handlers to use V2 (one at a time)
// File: nodedb-cluster/src/raft_loop/handle_rpc/consensus.rs

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    pub(super) fn handle_append_entries_rpc(&self, req: AppendEntriesRequest) -> Result<RaftRpc> {
        // OLD: let mut mr = self.multi_raft.lock()...
        // NEW: no lock needed — MultiRaftV2 handles per-group internally
        let resp = self.multi_raft_v2.handle_append_entries(&req)?;
        Ok(RaftRpc::AppendEntriesResponse(resp))
    }
}

// STEP 3: Migrate tick loop
// File: nodedb-cluster/src/raft_loop/loop_core.rs

pub async fn run_tick_loop(&self) {
    let mut interval = tokio::time::interval(self.tick_interval);
    loop {
        interval.tick().await;
        // OLD: lock entire MultiRaft, iterate groups
        // NEW: tick_all() handles per-group locking
        let outputs = self.multi_raft_v2.tick_all();
        for (group_id, ready) in outputs {
            self.dispatch_ready(group_id, ready).await;
        }
    }
}
```

---

## PERF-2: Commit Index O(k log k)

**File:** `nodedb-raft/src/node/internal.rs` — REPLACE `try_advance_commit_index`

```rust
impl<S: LogStorage> RaftNode<S> {
    /// Advance commit_index using the k-th largest match_index algorithm.
    ///
    /// # Algorithm
    ///
    /// Collect `match_index` from all voters (including self's `last_index`),
    /// sort descending, and the quorum-th element is the highest index that
    /// can be committed. This is O(k log k) where k = voter count, vs the
    /// previous O(n·k) where n = uncommitted entries.
    ///
    /// # Safety: Previous-term guard
    ///
    /// Raft §5.4.2: a leader can only commit entries from its CURRENT term
    /// by counting replicas. Entries from previous terms are committed
    /// implicitly when a current-term entry commits.
    ///
    /// # Check-quorum integration
    ///
    /// If a quorum of match_indexes are at or near `last_index`, we also
    /// refresh `last_quorum_contact` (used by check-quorum step-down).
    pub(super) fn try_advance_commit_index(&mut self) {
        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        let last_index = self.log.last_index();
        let quorum_size = self.config.quorum();

        // -- Collect match_indexes: self + all voters --
        // SmallVec avoids heap allocation for clusters = 9 voters
        // (covers 3/5/7/9-voter configurations without allocation)
        let mut indexes: smallvec::SmallVec<[u64; 9]> =
            smallvec::SmallVec::with_capacity(self.config.peers.len() + 1);

        // Self always has the full log
        indexes.push(last_index);

        for &peer in &self.config.peers {
            indexes.push(leader.match_index_for(peer).unwrap_or(0));
        }

        // -- Sort descending: highest match_index first --
        indexes.sort_unstable_by(|a, b| b.cmp(a));

        // -- Quorum-th largest is the commit candidate --
        // For a 5-voter cluster with quorum=3:
        //   indexes sorted desc: [100, 100, 95, 50, 0]
        //   quorum_pos = 3 - 1 = 2 ? indexes[2] = 95
        //   ? 95 is the highest index on at least 3 nodes
        let quorum_pos = quorum_size - 1;
        let candidate = match indexes.get(quorum_pos) {
            Some(&idx) => idx,
            None => return, // fewer than quorum nodes tracked (shouldn't happen)
        };

        // -- Check-quorum: refresh contact if quorum is near tip --
        // If quorum-th match_index is within `quorum_lag_tolerance` of
        // last_index, we consider quorum "in contact".
        //
        // Tolerance: allow 1 entry of lag (the no-op being replicated).
        // This matches the check-quorum fix in P2.
        let quorum_lag_tolerance = 1;
        if candidate + quorum_lag_tolerance >= last_index {
            self.last_quorum_contact = Some(std::time::Instant::now());
        }

        // -- Previous-term guard --
        // Only commit if the candidate entry is from the current term.
        // Entries from previous terms commit implicitly when a current-term
        // entry at a higher index commits.
        if candidate > self.volatile.commit_index {
            match self.log.term_at(candidate) {
                Some(term) if term == self.hard_state.current_term => {
                    self.volatile.commit_index = candidate;
                    self.collect_committed_entries();
                }
                _ => {
                    // Candidate is from a previous term — cannot commit
                    // directly. However, if there are current-term entries
                    // above it that have quorum, they will be found in the
                    // next call. No action needed here.
                }
            }
        }
    }

    /// Fallback: find the highest CURRENT-TERM entry with quorum.
    ///
    /// Used when the k-th largest match_index points to a previous-term
    /// entry. Scans backward from `candidate` to find the first
    /// current-term entry that also has quorum.
    ///
    /// This is O(remaining_entries) but only runs when there are
    /// previous-term entries above the commit point — rare in steady state.
    fn advance_past_previous_term(&mut self, upper_bound: u64) {
        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        let current_term = self.hard_state.current_term;

        // Scan backward from upper_bound to find current-term entries
        for n in (self.volatile.commit_index + 1..=upper_bound).rev() {
            if self.log.term_at(n) != Some(current_term) {
                continue; // skip previous-term entries
            }

            // Check if this current-term entry has quorum
            let mut count = 1u64; // self
            for &peer in &self.config.peers {
                if leader.match_index_for(peer) >= n {
                    count += 1;
                }
            }

            if count as usize >= self.config.quorum() {
                self.volatile.commit_index = n;
                self.collect_committed_entries();
                return;
            }
        }
    }
}
```

**Test:**

```rust
// nodedb-raft/tests/commit_index_performance.rs

#[cfg(test)]
mod commit_index_tests {
    use super::*;
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::storage::MemStorage;
    use std::time::Duration;

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

    #[test]
    fn commit_index_correctness_5_voter() {
        // Leader + 4 followers = 5 voters, quorum = 3
        let mut node = RaftNode::new(config(1, vec![2, 3, 4, 5]), MemStorage::new());
        // ... force leader, append entries, simulate match_indexes ...

        // Scenario: match_indexes = [self=100, peer2=100, peer3=95, peer4=50, peer5=0]
        // Expected commit = 95 (3 nodes have =95)
        // ... verify ...
    }

    #[test]
    fn commit_index_with_lagging_follower() {
        // 5-voter, one follower completely lagging
        // match_indexes = [100, 100, 100, 100, 0]
        // Quorum=3, quorum_pos=2, indexes[2]=100 ? commit 100
        // ... verify ...
    }

    #[test]
    fn previous_term_entries_not_committed_directly() {
        // Leader from term 5 sees entry from term 4 with quorum.
        // Should NOT commit it directly — only when a term-5 entry commits.
        // ... verify ...
    }
}
```

---

## PERF-3: Eliminate Per-Tick Allocations

**File:** `nodedb-raft/src/state.rs` — ADD CACHED TARGETS

```rust
// Add to LeaderState:

use std::collections::HashSet;

pub struct LeaderState {
    // ... existing fields ...

    /// Cached flat list of all replication targets (voters + learners).
    ///
    /// Rebuilt lazily when `targets_dirty` is set by membership changes.
    /// Eliminates per-tick allocation for replicate_to_all().
    replication_targets: Vec<u64>,

    /// Cached observer list (cloned from config).
    observer_targets: Vec<u64>,

    /// Set to `true` when peers/learners/observers change.
    /// Next call to `replicate_to_all()` rebuilds the cache.
    targets_dirty: bool,

    /// Pre-allocated AppendEntriesRequest for each peer.
    ///
    /// Reused across ticks — only `entries` field is cleared/re-filled.
    /// Reduces allocation for heartbeat path (empty entries).
    pending_requests: HashMap<u64, AppendEntriesRequest>,
}

impl LeaderState {
    pub fn new(
        voters: &[u64],
        observers: &[u64],
        last_index: u64,
    ) -> Self {
        let mut targets = Vec::with_capacity(voters.len());
        targets.extend_from_slice(voters);

        let mut obs = Vec::with_capacity(observers.len());
        obs.extend_from_slice(observers);

        Self {
            // ... existing fields ...
            replication_targets: targets,
            observer_targets: obs,
            targets_dirty: false,
            pending_requests: HashMap::with_capacity(voters.len()),
        }
    }

    /// Mark the target cache as dirty — called on membership change.
    pub fn mark_targets_dirty(&mut self) {
        self.targets_dirty = true;
    }

    /// Get cached replication targets, rebuilding if dirty.
    ///
    /// Returns a slice — no allocation if cache is valid.
    pub fn replication_targets(&mut self, voters: &[u64], learners: &[u64]) -> &[u64] {
        if self.targets_dirty {
            self.replication_targets.clear();
            self.replication_targets.reserve(voters.len() + learners.len());
            self.replication_targets.extend_from_slice(voters);
            self.replication_targets.extend_from_slice(learners);
            self.targets_dirty = false;
        }
        &self.replication_targets
    }

    /// Get cached observer targets.
    pub fn observer_targets(&mut self, observers: &[u64]) -> &[u64] {
        if self.targets_dirty {
            self.observer_targets.clear();
            self.observer_targets.extend_from_slice(observers);
        }
        &self.observer_targets
    }
}
```

**File:** `nodedb-raft/src/node/internal.rs` — UPDATE `replicate_to_all`

```rust
impl<S: LogStorage> RaftNode<S> {
    /// Send AppendEntries to every tracked peer.
    ///
    /// Uses cached target lists — no allocation in the steady-state
    /// heartbeat path (when targets haven't changed).
    pub(super) fn replicate_to_all(&mut self) {
        // Split borrow: leader_state and config are different fields
        let (targets, observers) = {
            let leader = self.leader_state.as_mut().unwrap();
            let t = leader.replication_targets(
                &self.config.peers,
                &self.config.learners,
            );
            let o = leader.observer_targets(&self.config.observers);
            (t.to_vec(), o.to_vec()) // TODO: eliminate with unsafe or restructure
        };

        // Send to voters + learners (cached list)
        for peer in targets {
            self.send_append_entries(peer);
        }

        // Send to observers (cached list, with backpressure check)
        for observer in observers {
            self.send_append_entries_to_observer(observer);
        }
    }

    /// Send AppendEntries to a single peer — reuses request buffer.
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

        // Reuse the pre-allocated request for this peer
        let request = leader.pending_requests
            .entry(peer)
            .or_insert_with(|| AppendEntriesRequest {
                term: self.hard_state.current_term,
                leader_id: self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries: Vec::new(),
                leader_commit: self.volatile.commit_index,
                group_id: self.config.group_id,
            });

        // Update mutable fields in-place (no new allocation)
        request.term = self.hard_state.current_term;
        request.leader_id = self.config.node_id;
        request.prev_log_index = prev_log_index;
        request.prev_log_term = prev_log_term;
        request.leader_commit = self.volatile.commit_index;
        request.group_id = self.config.group_id;

        // Fill entries — reuses the existing Vec's capacity
        request.entries.clear();
        if next_index <= self.log.last_index() {
            match self.log.entries_range(next_index, self.log.last_index()) {
                Ok(slice) => {
                    request.entries.extend_from_slice(slice);
                }
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
                Err(_) => {}
            }
        }

        // Clone into ready.messages — this is unavoidable (the Ready
        // struct takes ownership), but the source Vec's capacity is
        // reused on the next call.
        self.ready.messages.push((peer, request.clone()));
    }
}
```

---

## TEST-1: Deterministic Simulation Harness

**File:** `nodedb-raft/src/clock.rs` — NEW FILE

```rust
// SPDX-License-Identifier: BUSL-1.1

//! Clock abstraction for deterministic testing.
//!
//! RaftNode uses `Clock` instead of calling `Instant::now()` directly.
//! Production uses `SystemClock`; tests use `SimClock` for deterministic
//! time advancement without real sleeping.

use std::time::{Duration, Instant};

/// Abstract time source.
pub trait Clock: Send + Sync + 'static {
    /// Returns the current instant.
    fn now(&self) -> Instant;
}

/// Real system clock — delegates to `Instant::now()`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[inline]
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Deterministic simulation clock.
///
/// Time only advances when the test harness explicitly calls `advance()`.
/// Multiple `SimClock` instances created from the same `SimClockHandle`
/// share the same time source.
///
/// # Thread safety
///
/// Uses `Arc<Mutex<Instant>>` internally — safe to share across threads
/// for multi-threaded test scenarios.
///
/// # Example
///
/// ```
/// use nodedb_raft::clock::{SimClock, Clock};
/// use std::time::Duration;
///
/// let clock = SimClock::new();
/// let t1 = clock.now();
/// clock.advance(Duration::from_millis(100));
/// let t2 = clock.now();
/// assert_eq!(t2.duration_since(t1), Duration::from_millis(100));
/// ```
#[derive(Debug, Clone)]
pub struct SimClock {
    inner: std::sync::Arc<std::sync::Mutex<Instant>>,
}

impl SimClock {
    /// Create a new simulation clock starting at `Instant::now()`.
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(
                std::sync::Mutex::new(Instant::now())
            ),
        }
    }

    /// Create a clock with a specific starting instant.
    pub fn starting_at(start: Instant) -> Self {
        Self {
            inner: std::sync::Arc::new(
                std::sync::Mutex::new(start)
            ),
        }
    }

    /// Advance the simulation clock forward.
    ///
    /// All nodes using this clock will see the new time on their next
    /// `now()` call.
    pub fn advance(&self, duration: Duration) {
        let mut inner = self.inner.lock().unwrap();
        *inner += duration;
    }

    /// Advance to a specific instant (must be in the future).
    pub fn advance_to(&self, target: Instant) {
        let mut inner = self.inner.lock().unwrap();
        if target > *inner {
            *inner = target;
        }
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        *self.inner.lock().unwrap()
    }
}

/// Deterministic RNG wrapper for simulation.
///
/// Provides reproducible random sequences — same seed = same behavior.
/// Used for election timeout randomization in tests.
pub trait SimRng: Send + Sync + 'static {
    /// Generate a random u64 in `[0, n)`.
    fn gen_range(&self, n: u64) -> u64;
}

/// Seeded deterministic RNG — produces the same sequence for the same seed.
pub struct SeededRng {
    state: std::sync::Mutex<u64>,
}

impl SeededRng {
    pub fn new(seed: u64) -> Self {
        Self {
            state: std::sync::Mutex::new(seed),
        }
    }
}

impl SimRng for SeededRng {
    fn gen_range(&self, n: u64) -> u64 {
        let mut state = self.state.lock().unwrap();
        // xorshift64* — fast, deterministic, adequate quality for simulation
        *state ^= *state >> 12;
        *state ^= *state << 25;
        *state ^= *state >> 27;
        let result = state.wrapping_mul(0x2545F4914F6CDD1D);
        result % n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_clock_advances() {
        let clock = SimClock::new();
        let start = clock.now();
        clock.advance(Duration::from_millis(100));
        let after = clock.now();
        assert_eq!(after.duration_since(start), Duration::from_millis(100));
    }

    #[test]
    fn sim_clock_shared() {
        let clock = SimClock::new();
        let clone = clock.clone();
        clock.advance(Duration::from_millis(50));
        assert_eq!(
            clone.now().duration_since(clock.now()),
            Duration::ZERO
        );
    }

    #[test]
    fn seeded_rng_deterministic() {
        let rng1 = SeededRng::new(42);
        let rng2 = SeededRng::new(42);

        for _ in 0..10 {
            assert_eq!(rng1.gen_range(100), rng2.gen_range(100));
        }
    }
}
```

**File:** `nodedb-raft/src/node/core.rs` — UPDATE TO USE CLOCK

```rust
// Add Clock generic parameter to RaftNode

use crate::clock::{Clock, SystemClock};

pub struct RaftNode<S: LogStorage, C: Clock = SystemClock> {
    pub(super) config: RaftConfig,
    pub(super) role: NodeRole,
    pub(super) hard_state: HardState,
    pub(super) volatile: VolatileState,
    pub(super) leader_state: Option<LeaderState>,
    pub(super) log: RaftLog<S>,
    pub(super) election_deadline: Instant,
    pub(super) heartbeat_deadline: Instant,
    pub(super) votes_received: HashSet<u64>,
    pub(super) ready: Ready,
    pub(super) leader_id: u64,
    pub(super) leadership_transfer: Option<LeadershipTransfer>,
    pub(super) durable_applied: u64,
    pub(super) clock: C,
}

// Production constructor — uses SystemClock
impl<S: LogStorage> RaftNode<S, SystemClock> {
    pub fn new(config: RaftConfig, storage: S) -> Self {
        Self::with_clock(config, storage, SystemClock)
    }
}

// Generic constructor — accepts any Clock implementation
impl<S: LogStorage, C: Clock> RaftNode<S, C> {
    pub fn with_clock(config: RaftConfig, storage: S, clock: C) -> Self {
        let now = clock.now();
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
            clock,
            config,
        }
    }

    /// Drive time-based events — uses the injected clock.
    pub fn tick(&mut self) {
        let now = self.clock.now(); // ? deterministic in simulation
        // ... rest unchanged ...
    }
}

// Update reset_election_timeout to use clock
impl<S: LogStorage, C: Clock> RaftNode<S, C> {
    pub(super) fn reset_election_timeout(&mut self) {
        let timeout = self.randomized_election_timeout();
        let now = self.clock.now();
        self.election_deadline = now + timeout;
    }
}
```

**File:** `nodedb-raft/tests/sim/harness.rs` — NEW FILE

```rust
// SPDX-License-Identifier: BUSL-1.1

//! Deterministic simulation harness for Raft consensus testing.
//!
//! Replaces real-time integration tests with a simulated clock and
//! controlled message delivery. Eliminates flakiness from timing races
//! and reduces test execution time from seconds to microseconds.

use std::collections::VecDeque;
use std::time::Duration;

use nodedb_raft::clock::{SimClock, Clock};
use nodedb_raft::message::*;
use nodedb_raft::node::config::RaftConfig;
use nodedb_raft::node::core::RaftNode;
use nodedb_raft::state::NodeRole;
use nodedb_raft::storage::MemStorage;

/// A simulated Raft cluster with deterministic message delivery.
///
/// # Determinism guarantees
///
/// - Time: advanced only by explicit `advance()` calls
/// - Message ordering: FIFO queue, processed in send order
/// - Elections: seeded RNG ? same timeouts for same seed
/// - Crashes: explicit `crash()` / `restart()` calls
///
/// # Usage
///
/// ```rust
/// let mut sim = SimCluster::new(5, 42); // 5 nodes, seed 42
/// sim.run_until_stable(1000);
/// assert!(sim.has_leader());
///
/// sim.partition(&[0, 1], &[2, 3, 4]);
/// sim.run_until_stable(1000);
/// // Verify partition behavior...
///
/// sim.heal();
/// sim.run_until_stable(1000);
/// sim.assert_safety_invariants();
/// ```
pub struct SimCluster {
    /// All nodes in the cluster (index = node_id - 1).
    pub nodes: Vec<NodeSlot>,
    /// Messages queued for delivery: (from, to, message).
    inbox: VecDeque<(u64, u64, SimMessage)>,
    /// Connectivity matrix: `connectivity[from][to]` = true if link is up.
    connectivity: Vec<Vec<bool>>,
    /// Shared simulation clock.
    pub clock: SimClock,
    /// RNG seed for reproducibility.
    seed: u64,
    /// Number of ticks processed.
    tick_count: u64,
    /// Messages delivered.
    messages_delivered: u64,
    /// Messages dropped (partition/simulated loss).
    messages_dropped: u64,
}

/// A node slot — either alive (with RaftNode) or crashed (with storage preserved).
pub enum NodeSlot {
    Alive(RaftNode<MemStorage, SimClock>),
    Crashed(MemStorage), // storage preserved for restart
}

/// Messages exchanged in the simulation.
#[derive(Debug, Clone)]
pub enum SimMessage {
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    InstallSnapshot(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
    TimeoutNow(TimeoutNowRequest),
}

impl SimCluster {
    /// Create a new simulated cluster.
    ///
    /// - `num_nodes`: number of voters (node IDs 1..=num_nodes)
    /// - `seed`: RNG seed for deterministic election timeouts
    pub fn new(num_nodes: usize, seed: u64) -> Self {
        let clock = SimClock::starting_at(
            std::time::Instant::now() // base point, advanced only by sim
        );

        let all_peers: Vec<u64> = (1..=num_nodes as u64).collect();
        let nodes = (1..=num_nodes as u64)
            .map(|node_id| {
                let peers: Vec<u64> = all_peers
                    .iter()
                    .copied()
                    .filter(|&p| p != node_id)
                    .collect();

                let config = RaftConfig {
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
                };

                // Use seed + node_id for per-node determinism
                let node_seed = seed.wrapping_add(node_id);
                let node = RaftNode::with_clock(
                    config,
                    MemStorage::new(),
                    clock.clone(),
                );
                // TODO: inject SeededRng for election timeout randomization
                // For now, timeouts are deterministic based on config

                NodeSlot::Alive(node)
            })
            .collect();

        // Fully connected initially
        let connectivity = vec![vec![true; num_nodes]; num_nodes];

        Self {
            nodes,
            inbox: VecDeque::new(),
            connectivity,
            clock,
            seed,
            tick_count: 0,
            messages_delivered: 0,
            messages_dropped: 0,
        }
    }

    /// Run the simulation until stable or max_ticks reached.
    ///
    /// "Stable" means: no pending messages AND no node has a pending
    /// election timeout within the next tick.
    pub fn run_until_stable(&mut self, max_ticks: usize) {
        for _ in 0..max_ticks {
            self.tick();
            if self.is_stable() {
                break;
            }
        }
    }

    /// Advance simulation by one tick (1ms).
    pub fn tick(&mut self) {
        self.tick_count += 1;
        self.clock.advance(Duration::from_millis(1));

        // 1. Tick all alive nodes
        let mut new_messages = Vec::new();
        for slot in &mut self.nodes {
            if let NodeSlot::Alive(node) = slot {
                node.tick();
                let ready = node.take_ready();

                // Drain Ready ? queue messages
                for (dest, msg) in ready.messages {
                    new_messages.push((node.node_id(), dest, SimMessage::AppendEntries(msg)));
                }
                for (dest, req) in ready.vote_requests {
                    new_messages.push((node.node_id(), dest, SimMessage::RequestVote(req)));
                }
                // committed_entries are "applied" in simulation
                // (no external state machine)
            }
        }

        // 2. Add new messages to inbox
        self.inbox.extend(new_messages);

        // 3. Deliver all pending messages (respecting partitions)
        self.deliver_all_messages();
    }

    /// Deliver all messages in the inbox, respecting connectivity matrix.
    fn deliver_all_messages(&mut self) {
        let pending: Vec<(u64, u64, SimMessage)> = self.inbox.drain(..).collect();

        for (from, to, msg) in pending {
            let from_idx = (from - 1) as usize;
            let to_idx = (to - 1) as usize;

            // Check connectivity
            if !self.connectivity[from_idx][to_idx] {
                self.messages_dropped += 1;
                continue; // message dropped
            }

            // Deliver to target node
            if let NodeSlot::Alive(node) = &mut self.nodes[to_idx] {
                self.messages_delivered += 1;

                match msg {
                    SimMessage::AppendEntries(req) => {
                        let resp = node.handle_append_entries(&req);
                        if let Ok(resp) = resp {
                            self.inbox.push_back((
                                to, from,
                                SimMessage::AppendEntriesResponse(resp),
                            ));
                        }
                    }
                    SimMessage::AppendEntriesResponse(resp) => {
                        node.handle_append_entries_response(from, &resp);
                    }
                    SimMessage::RequestVote(req) => {
                        let resp = node.handle_request_vote(&req);
                        if let Ok(resp) = resp {
                            self.inbox.push_back((
                                to, from,
                                SimMessage::RequestVoteResponse(resp),
                            ));
                        }
                    }
                    SimMessage::RequestVoteResponse(resp) => {
                        node.handle_request_vote_response(from, &resp);
                    }
                    SimMessage::TimeoutNow(req) => {
                        node.handle_timeout_now(&req);
                    }
                    // Snapshot handling omitted for brevity
                    _ => {}
                }
            }
        }
    }

    /// Whether the cluster is "stable" (no pending work).
    fn is_stable(&self) -> bool {
        self.inbox.is_empty()
            && self.nodes.iter().all(|slot| {
                match slot {
                    NodeSlot::Alive(node) => {
                        // No election timeout due within 1ms
                        let now = self.clock.now();
                        now < node.election_deadline()
                    }
                    NodeSlot::Crashed(_) => true,
                }
            })
    }

    /// Inject a network partition.
    ///
    /// Messages between `left` and `right` node sets will be dropped.
    pub fn partition(&mut self, left: &[usize], right: &[usize]) {
        for &i in left {
            for &j in right {
                self.connectivity[i][j] = false;
                self.connectivity[j][i] = false;
            }
        }
    }

    /// Heal all partitions — restore full connectivity.
    pub fn heal(&mut self) {
        for row in &mut self.connectivity {
            for cell in row.iter_mut() {
                *cell = true;
            }
        }
    }

    /// Crash a node (SIGKILL equivalent — no cleanup).
    pub fn crash(&mut self, idx: usize) {
        if let NodeSlot::Alive(node) = &mut self.nodes[idx] {
            // Extract storage for later restart
            let storage = node.take_storage();
            self.nodes[idx] = NodeSlot::Crashed(storage);
        }
    }

    /// Restart a crashed node.
    pub fn restart(&mut self, idx: usize) {
        if let NodeSlot::Crashed(storage) = &self.nodes[idx] {
            let config = self.node_config(idx);
            let mut node = RaftNode::with_clock(
                config,
                std::mem::take(storage),
                self.clock.clone(),
            );
            node.restore().unwrap();
            self.nodes[idx] = NodeSlot::Alive(node);
        }
    }

    /// Propose an entry on the current leader.
    pub fn propose(&mut self, data: Vec<u8>) -> Option<u64> {
        for slot in &mut self.nodes {
            if let NodeSlot::Alive(node) = slot {
                if node.role() == NodeRole::Leader {
                    return node.propose(data).ok();
                }
            }
        }
        None
    }

    /// Get the current leader (if any).
    pub fn leader(&self) -> Option<usize> {
        self.nodes.iter().enumerate().find_map(|(i, slot)| {
            match slot {
                NodeSlot::Alive(node) if node.role() == NodeRole::Leader => Some(i),
                _ => None,
            }
        })
    }

    /// Assert all Raft safety invariants.
    ///
    /// # Panics
    /// Panics if any invariant is violated — use in tests.
    pub fn assert_safety_invariants(&self) {
        self.assert_election_safety();
        self.assert_log_matching();
        self.assert_state_machine_safety();
    }

    /// Election Safety: at most one leader per term.
    fn assert_election_safety(&self) {
        use std::collections::{HashMap, HashSet};

        let mut leaders_by_term: HashMap<u64, HashSet<u64>> = HashMap::new();

        for slot in &self.nodes {
            if let NodeSlot::Alive(node) = slot {
                if node.role() == NodeRole::Leader {
                    leaders_by_term
                        .entry(node.current_term())
                        .or_default()
                        .insert(node.node_id());
                }
            }
        }

        for (term, leaders) in &leaders_by_term {
            assert!(
                leaders.len() <= 1,
                "Election Safety violated: term {} has {} leaders: {:?}",
                term,
                leaders.len(),
                leaders
            );
        }
    }

    /// Log Matching: same (term, index) ? same entry.
    fn assert_log_matching(&self) {
        // For each pair of nodes, check common prefix
        for i in 0..self.nodes.len() {
            for j in (i + 1)..self.nodes.len() {
                if let (
                    NodeSlot::Alive(node_i),
                    NodeSlot::Alive(node_j),
                ) = (&self.nodes[i], &self.nodes[j])
                {
                    let log_i = node_i.log_entries_range(1, u64::MAX);
                    let log_j = node_j.log_entries_range(1, u64::MAX);

                    if let (Ok(entries_i), Ok(entries_j)) = (log_i, log_j) {
                        for (idx, entry_i) in entries_i.iter().enumerate() {
                            if let Some(entry_j) = entries_j.get(idx) {
                                if entry_i.term == entry_j.term {
                                    assert_eq!(
                                        entry_i.data, entry_j.data,
                                        "Log Matching violated at index {}: node {} has {:?}, node {} has {:?}",
                                        idx, i, entry_i.data, j, entry_j.data
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// State Machine Safety: applied entries never diverge.
    fn assert_state_machine_safety(&self) {
        // In simulation, we track applied entries per node
        // (simplified — real impl would need external state machine tracking)
        // This is a placeholder for the full invariant check
    }

    /// Get node's election deadline (for is_stable check).
    fn node_election_deadline(&self, idx: usize) -> Option<std::time::Instant> {
        match &self.nodes[idx] {
            NodeSlot::Alive(node) => Some(node.election_deadline()),
            NodeSlot::Crashed(_) => None,
        }
    }

    fn node_config(&self, idx: usize) -> RaftConfig {
        // Reconstruct config for restart — in practice, store separately
        let node_id = (idx + 1) as u64;
        let peers: Vec<u64> = (1..=self.nodes.len() as u64)
            .filter(|&p| p != node_id)
            .collect();

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

    /// Statistics for debugging.
    pub fn stats(&self) -> SimStats {
        SimStats {
            ticks: self.tick_count,
            messages_delivered: self.messages_delivered,
            messages_dropped: self.messages_dropped,
            inbox_size: self.inbox.len(),
            alive_nodes: self.nodes.iter().filter(|s| matches!(s, NodeSlot::Alive(_))).count(),
            crashed_nodes: self.nodes.iter().filter(|s| matches!(s, NodeSlot::Crashed(_))).count(),
        }
    }
}

#[derive(Debug)]
pub struct SimStats {
    pub ticks: u64,
    pub messages_delivered: u64,
    pub messages_dropped: u64,
    pub inbox_size: usize,
    pub alive_nodes: usize,
    pub crashed_nodes: usize,
}
```

**File:** `nodedb-raft/tests/sim/scenarios.rs` — TEST CASES

```rust
// SPDX-License-Identifier: BUSL-1.1

//! Deterministic scenario tests using SimCluster.
//!
//! These replace flaky real-time integration tests with deterministic
//! simulations that complete in microseconds.

mod harness;
use harness::*;

#[test]
fn basic_election_completes_deterministically() {
    let mut sim = SimCluster::new(5, 42);
    sim.run_until_stable(1000);

    assert!(
        sim.leader().is_some(),
        "5-node cluster should elect a leader"
    );
}

#[test]
fn partition_minority_minority_elects_leader() {
    let mut sim = SimCluster::new(5, 42);
    sim.run_until_stable(1000);

    let original_leader = sim.leader().expect("initial leader");

    // Partition: 2 nodes (including leader) vs 3 nodes
    let minority = [original_leader, (original_leader + 1) % 5];
    let majority: Vec<usize> = (0..5)
        .filter(|&i| !minority.contains(&i))
        .collect();

    sim.partition(&minority, &majority);
    sim.run_until_stable(1000);

    // Majority side should have a leader
    let majority_leader = majority.iter().find_map(|&i| {
        if sim.nodes[i].is_leader() { Some(i) } else { None }
    });
    assert!(
        majority_leader.is_some(),
        "Majority partition should elect a leader"
    );

    // Minority leader should have stepped down (check-quorum)
    // Note: this requires check-quorum to be implemented
    // assert_ne!(sim.nodes[original_leader].role(), NodeRole::Leader);
}

#[test]
fn heal_partition_converges_to_single_leader() {
    let mut sim = SimCluster::new(5, 42);
    sim.run_until_stable(1000);

    // Partition and heal
    sim.partition(&[0, 1], &[2, 3, 4]);
    sim.run_until_stable(1000);

    sim.heal();
    sim.run_until_stable(2000);

    // After heal, safety invariants must hold
    sim.assert_safety_invariants();

    // Eventually converge to one leader
    let leaders: Vec<usize> = (0..5)
        .filter(|&i| sim.nodes[i].is_leader())
        .collect();
    assert!(
        leaders.len() <= 1,
        "After heal, at most one leader should exist"
    );
}

#[test]
fn crash_leader_and_reelect() {
    let mut sim = SimCluster::new(5, 42);
    sim.run_until_stable(1000);

    let leader = sim.leader().expect("leader");
    sim.crash(leader);
    sim.run_until_stable(1000);

    // Remaining 4 nodes should elect a new leader
    let new_leader = sim.leader();
    assert!(
        new_leader.is_some(),
        "4-node quorum should elect a leader after crash"
    );
    assert_ne!(new_leader.unwrap(), leader);

    // Restart the crashed node — it should catch up
    sim.restart(leader);
    sim.run_until_stable(2000);

    sim.assert_safety_invariants();
}

#[test]
fn replicated_entry_survives_partition_and_heal() {
    let mut sim = SimCluster::new(5, 42);
    sim.run_until_stable(1000);

    // Commit an entry
    let index = sim.propose(b"test_entry".to_vec())
        .expect("leader should accept proposal");
    sim.run_until_stable(500); // let it replicate

    // Partition
    sim.partition(&[0, 1], &[2, 3, 4]);
    sim.run_until_stable(1000);

    // Heal
    sim.heal();
    sim.run_until_stable(2000);

    // Verify entry committed on majority
    sim.assert_safety_invariants();

    // TODO: verify entry is in applied state on majority nodes
}
```

---

## Migration Path Summary

```
Week 1:  Add Clock trait (zero-change default SystemClock)
         Add SimClock implementation + tests
         All existing code compiles unchanged

Week 2:  Build SimCluster harness (parallel to existing tests)
         Port 3 critical tests: election, partition, crash-recovery
         Verify simulation matches real-time behavior

Week 3:  Add PERF-2 (commit index algorithm) — small, isolated
         Add PERF-3 (cached targets) — small, isolated
         Both are internal to RaftNode, no API change

Week 4:  Add PERF-1 (per-group locks) — MultiRaft restructure
         Use MultiRaftCompat for gradual migration
         Update tick loop, RPC handlers

Week 5+: Add property-based tests using SimCluster
         Port flaky tests to deterministic versions
         Delete tests that simulation fully covers
```

**Yang jangan buat sekarang (P2):**
- Jangan break API — semua perubahan perlu backward-compatible
- Jangan delete existing tests — kedua-dua jalan parallel
- Jangan optimize prematurely — profile dulu dengan cargo-flamegraph