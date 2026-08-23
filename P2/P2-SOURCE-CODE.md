> **SNAPSHOT NOTE (2026-08-24):** Ini adalah dump source @ `461c3ad` (07-08) — **PRE-FIX, PRE-REFACTOR**.
> Main telah advance selepas snapshot ini. Antara perubahan utama yang TIDAK kelihatan di sini:
>
> - ReadIndex quorum confirmation (`f9910444b`, `b1f52e9a4` — 23-08): `nodedb-raft/src/node/read_index.rs`, `nodedb-cluster/src/read_index_wait.rs`
> - Persist-before-reply HardState (`9469296c2`): `consensus.rs`, `dispatch_outbound.rs`, `snapshot_dispatch.rs`
> - Bounded-staleness refactor (`b1f52e9a4`): `closed_timestamp.rs` → `multi_raft/read_index.rs` + `nodedb-raft/src/node/staleness.rs` (lag-vs-leader)
> - Scatter-gather guard (`b037bab06`), pre-vote (`dd91eed70`), leadership transfer (`ae45049bf`), decommission (`7056676cc`)
>   Fix P2 kita (seed commit_index, check-quorum, learner gate, lease) TIDAK dalam snapshot ini — rujuk branch `p2-rebased-v2`.
>   Untuk audit semasa guna tree terkini (main HEAD), bukan dump ini.
========== FILE: nodedb-cluster/src/raft_loop/handle_rpc/consensus.rs ==========
// SPDX-License-Identifier: BUSL-1.1

//! Raft consensus RPC bodies: AppendEntries, RequestVote, InstallSnapshot,
//! and the TimeoutNow election trigger.

use crate::error::Result;
use crate::forward::PlanExecutor;
use crate::rpc_codec::RaftRpc;
use nodedb_raft::message::{
    AppendEntriesRequest, InstallSnapshotRequest, RequestVoteRequest, TimeoutNowRequest,
};

use super::super::loop_core::{CommitApplier, RaftLoop};
use super::membership::TOPOLOGY_GROUP_ID;

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    pub(super) fn handle_append_entries_rpc(&self, req: AppendEntriesRequest) -> Result<RaftRpc> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let resp = mr.handle_append_entries(&req)?;
        // Persist any term bump (become_follower) durably before the
        // reply leaves this node, so a restart cannot forget it.
        mr.persist_group_hard_state(req.group_id)?;
        Ok(RaftRpc::AppendEntriesResponse(resp))
    }

    pub(super) fn handle_request_vote_rpc(&self, req: RequestVoteRequest) -> Result<RaftRpc> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let resp = mr.handle_request_vote(&req)?;
        // Persist voted_for/current_term to stable storage BEFORE the
        // grant leaves this node, so a restart cannot double-vote.
        mr.persist_group_hard_state(req.group_id)?;
        Ok(RaftRpc::RequestVoteResponse(resp))
    }

    /// Apply snapshot bytes only after the cluster transport has authenticated
    /// the sender's mTLS identity, HMAC envelope, and replay sequence. CRC and
    /// chunk framing below detect corruption; they are not authenticity checks.
    pub(super) async fn handle_install_snapshot_rpc(
        &self,
        mut req: InstallSnapshotRequest,
    ) -> Result<RaftRpc> {
        // Validate snapshot framing for any non-empty chunk, then STRIP
        // the frame header so everything below this RPC boundary
        // (`receiver::handle_chunk`, `finalize::commit`, the
        // `SnapshotApplier`) sees the raw payload it expects — the
        // partial-file bytes, the whole-snapshot CRC, and the applier's
        // `zerompk::from_msgpack` all operate on the unframed payload.
        // Empty data is the bootstrap stub (no engine data yet); skip
        // framing in that case. The sender frames every non-empty chunk
        // with `encode_snapshot_chunk`.
        if !req.data.is_empty() {
            // Short-circuit immediately if this chunk has already been
            // quarantined after two consecutive CRC failures. Without
            // this check a quarantined chunk would re-attempt the
            // (always-failing) decode on every incoming RPC and never
            // surface a stable, operator-visible error.
            if let Some(ref hook) = self.snapshot_quarantine_hook
                && hook.is_quarantined(req.group_id, req.last_included_index)
            {
                return Err(crate::error::ClusterError::Codec {
                    detail: format!(
                        "InstallSnapshot chunk quarantined: group={} index={}",
                        req.group_id, req.last_included_index
                    ),
                });
            }

            match nodedb_raft::decode_snapshot_chunk(&req.data) {
                Ok((_engine_id, payload)) => {
                    // Successful decode — reset any prior strike so a
                    // single transient CRC error does not permanently
                    // count against a healthy peer.
                    let stripped = payload.to_vec();
                    if let Some(ref hook) = self.snapshot_quarantine_hook {
                        hook.record_success(req.group_id, req.last_included_index);
                    }
                    // Replace the framed chunk with its raw payload so the
                    // accumulator writes unframed bytes (offsets/CRC below
                    // are payload-space).
                    req.data = stripped;
                }
                Err(e) => {
                    let is_crc_class = matches!(
                        e,
                        nodedb_raft::snapshot_framing::SnapshotFramingError::CrcMismatch { .. }
                            | nodedb_raft::snapshot_framing::SnapshotFramingError::Truncated(_)
                    );
                    if is_crc_class && let Some(ref hook) = self.snapshot_quarantine_hook {
                        hook.record_failure(req.group_id, req.last_included_index, &e.to_string());
                    }
                    return Err(crate::error::ClusterError::Codec {
                        detail: format!("InstallSnapshot framing: {e}"),
                    });
                }
            }
        }

        let last_included_index = req.last_included_index;
        let group_id = req.group_id;

        // Route through the chunk accumulator when a data directory is
        // configured. The accumulator writes chunks to a `.partial` file,
        // validates the full CRC on the final chunk, and then calls
        // `mr.handle_install_snapshot` after atomic rename.
        //
        // When `data_dir` is `None` (unit tests that don't set a data
        // directory) fall through to the original direct call so test
        // coverage for Raft state-machine transitions is unaffected.
        //
        // Quarantine accounting for offset regression and CRC errors is
        // preserved: the `SnapshotOffsetRegression` and
        // `SnapshotCrcMismatch` error paths in the receiver both surface
        // as `ClusterError` variants that are propagated here.
        if let Some(ref data_dir) = self.data_dir {
            match crate::install_snapshot::receiver::handle_chunk(
                &req,
                &self.partial_snapshots,
                data_dir,
                &self.multi_raft,
                self.snapshot_applier.as_ref(),
            )
            .await
            {
                Ok(crate::install_snapshot::ChunkOutcome::Committed(snap_resp)) => {
                    // Final chunk committed — bump watcher for metadata group.
                    if group_id == TOPOLOGY_GROUP_ID {
                        self.group_watchers.bump(group_id, last_included_index);
                    }
                    return Ok(RaftRpc::InstallSnapshotResponse(snap_resp));
                }
                Ok(crate::install_snapshot::ChunkOutcome::Pending) => {
                    // Non-final chunk — pass a done=false stub to MultiRaft so
                    // it resets its election timeout and returns the current term.
                    let pending_req = nodedb_raft::InstallSnapshotRequest {
                        term: req.term,
                        leader_id: req.leader_id,
                        last_included_index: req.last_included_index,
                        last_included_term: req.last_included_term,
                        offset: req.offset,
                        data: vec![],
                        done: false,
                        group_id,
                        total_size: 0,
                    };
                    let resp = {
                        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                        let resp = mr.handle_install_snapshot(&pending_req)?;
                        // Persist any term bump before replying.
                        mr.persist_group_hard_state(group_id)?;
                        resp
                    };
                    return Ok(RaftRpc::InstallSnapshotResponse(resp));
                }
                Err(e @ crate::error::ClusterError::SnapshotOffsetRegression { .. }) => {
                    // Record the regression as a quarantine strike so the
                    // sender knows to retransmit from offset 0.
                    if let Some(ref hook) = self.snapshot_quarantine_hook {
                        hook.record_failure(group_id, last_included_index, &e.to_string());
                    }
                    // Reset partial state so the next offset-0 chunk starts fresh.
                    self.partial_snapshots
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(&group_id);
                    return Err(e);
                }
                Err(e @ crate::error::ClusterError::SnapshotCrcMismatch { .. }) => {
                    if let Some(ref hook) = self.snapshot_quarantine_hook {
                        hook.record_failure(group_id, last_included_index, &e.to_string());
                    }
                    return Err(e);
                }
                Err(e) => return Err(e),
            }
        }

        // Fallback: no data_dir — direct call (unit test path).
        let resp = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let resp = mr.handle_install_snapshot(&req)?;
            // Persist any term bump before replying.
            mr.persist_group_hard_state(group_id)?;
            resp
        };
        // Watcher contract: `applied_index` means "state visible
        // on this node up to index N", NOT "raft has advanced to
        // N". Bumping the watcher must therefore mirror actual
        // state-machine progress.
        //
        // - Metadata group: `mr.handle_install_snapshot` restores
        //   the metadata state machine synchronously before
        //   returning, so the watcher can be bumped here — state
        //   IS visible at `last_included_index`.
        //
        // - Data groups: snapshot install fast-forwards raft's
        //   `last_applied` but does NOT restore the data-plane
        //   state machine (no committed entries are produced for
        //   `run_apply_loop`, and there is currently no
        //   data-group state-machine snapshot restore path).
        //   Bumping the watcher here would wake waiters that
        //   then read missing state — silent data-loss-shaped
        //   bug. The data-group watcher is bumped only by the
        //   host crate's apply loop after the SPSC round-trip
        //   completes; that path is the single source of truth
        //   for "state visible".
        //
        // When data-group state-machine snapshots are
        // implemented, the restore path must bump the watcher
        // itself — not this handler.
        if group_id == TOPOLOGY_GROUP_ID {
            self.group_watchers.bump(group_id, last_included_index);
        }
        Ok(RaftRpc::InstallSnapshotResponse(resp))
    }

    pub(super) async fn on_timeout_now_impl(&self, req: TimeoutNowRequest) {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.handle_timeout_now(&req);
        // A TimeoutNow triggers an immediate election (term bump + self-vote);
        // persist that HardState before the resulting vote requests are
        // dispatched by the tick loop, so a restart cannot forget the term.
        if let Err(e) = mr.persist_group_hard_state(req.group_id) {
            tracing::error!(
                group_id = req.group_id,
                error = %e,
                "failed to persist hard state after timeout-now election trigger"
            );
        }
    }
}

========== FILE: nodedb-cluster/src/multi_raft/rpc_dispatch.rs ==========
// SPDX-License-Identifier: BUSL-1.1

//! Inbound RPC dispatch — look up the target group and delegate.
//!
//! Also holds the response handlers (`handle_append_entries_response`,
//! `handle_request_vote_response`) and the helpers for the tick loop
//! (`snapshot_metadata`, `advance_applied`, `match_index_for`).

use nodedb_raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest,
};

use crate::error::{ClusterError, Result};

use super::core::MultiRaft;

impl MultiRaft {
    /// Route an AppendEntries RPC to the correct group.
    pub fn handle_append_entries(
        &mut self,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_append_entries(req))
    }

    /// Route a RequestVote RPC to the correct group.
    pub fn handle_request_vote(&mut self, req: &RequestVoteRequest) -> Result<RequestVoteResponse> {
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_request_vote(req))
    }

    /// Route an InstallSnapshot RPC to the correct group.
    pub fn handle_install_snapshot(
        &mut self,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_install_snapshot(req)?)
    }

    /// Route a TimeoutNow RPC to the correct group.
    ///
    /// One-way — no response is produced. Silently ignored if the group is
    /// not mounted on this node (mirrors `handle_request_vote` for absent
    /// groups). The term+leader_id guard inside `RaftNode::handle_timeout_now`
    /// remains in place as an additional correctness check.
    pub fn handle_timeout_now(&mut self, req: &TimeoutNowRequest) {
        if let Some(node) = self.groups.get_mut(&req.group_id) {
            node.handle_timeout_now(req);
        }
    }

    /// Durably persist a group's HardState (current_term/voted_for) if it
    /// changed since the last persist. Must run under the `MultiRaft` lock
    /// before an RPC reply that granted a vote or bumped the term leaves this
    /// node, so a restart cannot forget the vote and let two leaders form.
    ///
    /// No-op when the group is not mounted on this node.
    pub fn persist_group_hard_state(&mut self, group_id: u64) -> Result<()> {
        if let Some(node) = self.groups.get_mut(&group_id) {
            node.persist_hard_state_if_dirty()?;
        }
        Ok(())
    }

    /// Get the current term and snapshot metadata for a group (for building
    /// InstallSnapshot RPCs).
    pub fn snapshot_metadata(&self, group_id: u64) -> Result<(u64, u64, u64)> {
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        Ok((
            node.current_term(),
            node.log_snapshot_index(),
            node.log_snapshot_term(),
        ))
    }

    /// Handle AppendEntries response for a specific group.
    pub fn handle_append_entries_response(
        &mut self,
        group_id: u64,
        peer: u64,
        resp: &AppendEntriesResponse,
    ) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.handle_append_entries_response(peer, resp);
        Ok(())
    }

    /// Handle RequestVote response for a specific group.
    pub fn handle_request_vote_response(
        &mut self,
        group_id: u64,
        peer: u64,
        resp: &RequestVoteResponse,
    ) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.handle_request_vote_response(peer, resp);
        Ok(())
    }

    /// Advance applied index for a group after processing committed entries.
    ///
    /// This is the DELIVERY watermark. See [`Self::save_applied_index`] for the
    /// durable floor a restart resumes from.
    pub fn advance_applied(&mut self, group_id: u64, applied_to: u64) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.advance_applied(applied_to);
        Ok(())
    }

    /// Durably record `applied_to` as the group's applied floor.
    ///
    /// `applied_to` MUST name an entry whose state-machine effects are already
    /// durable — for data groups, one whose redo record the WAL has fsynced.
    /// The next boot resumes delivery at `applied_to + 1`, so this is what
    /// keeps WAL replay and Raft replay from applying the same entry twice.
    ///
    /// Monotonic per group: an index at or below the current floor is a no-op.
    pub fn save_applied_index(&mut self, group_id: u64, applied_to: u64) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.save_durable_applied_index(applied_to)?;
        Ok(())
    }

    /// Query a peer's match_index from a specific Raft group's leader state.
    pub fn match_index_for(&self, group_id: u64, peer: u64) -> Option<u64> {
        self.groups.get(&group_id)?.match_index_for(peer)
    }

    /// Read the locally-applied index for a Raft group hosted on this
    /// node. Returns `None` if the group is not mounted here.
    ///
    /// Used by the tick loop to mirror `last_applied` into the
    /// per-group [`crate::applied_watcher::AppliedIndexWatcher`] —
    /// covers both the regular apply path and the snapshot-install
    /// path (which sets `last_applied = last_included_index`
    /// directly without producing committed entries).
    pub fn last_applied(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id).map(|n| n.last_applied())
    }

    /// Highest index present in a group's local log — committed or not — or
    /// `None` if the group is not mounted here.
    ///
    /// Read alongside [`Self::last_applied`] to answer "has this node applied
    /// everything its log holds?". That question needs the LOG TIP, not
    /// `commit_index`: a node that has just won an election observes its own
    /// `commit_index` still behind its log until its term's no-op commits, yet
    /// every entry already in a leader's log commits moments later — so only
    /// the tip bounds what the node is about to be responsible for.
    pub fn last_log_index(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id).map(|n| n.last_log_index())
    }

    /// `(group_id, last_applied)` pairs for every locally-mounted
    /// group. Cheap O(groups) snapshot — groups are few (one
    /// metadata + handful of vshard groups per node).
    pub fn applied_indices(&self) -> Vec<(u64, u64)> {
        self.groups
            .iter()
            .map(|(gid, node)| (*gid, node.last_applied()))
            .collect()
    }
}

========== FILE: nodedb-raft/src/node/core.rs ==========
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

========== FILE: nodedb-raft/src/node/internal.rs ==========
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

========== FILE: nodedb-raft/src/storage.rs ==========
// SPDX-License-Identifier: BUSL-1.1

use crate::error::Result;
use crate::message::LogEntry;
use crate::state::HardState;

/// Trait for persistent Raft log storage.
///
/// Implementors handle durability. The `nodedb-cluster` crate provides
/// a production implementation backed by `nodedb-wal`.
pub trait LogStorage: Send {
    /// Persist log entries (must be durable before returning).
    fn append(&mut self, entries: &[LogEntry]) -> Result<()>;

    /// Truncate log entries from `index` onward (inclusive).
    fn truncate(&mut self, index: u64) -> Result<()>;

    /// Load all entries after `snapshot_index` on startup.
    fn load_entries_after(&self, snapshot_index: u64) -> Result<Vec<LogEntry>>;

    /// Compact: discard entries up to `index`, save snapshot metadata.
    fn compact(&mut self, index: u64, term: u64) -> Result<()>;

    /// Return (last_included_index, last_included_term) of the latest snapshot.
    fn snapshot_metadata(&self) -> (u64, u64);

    /// Persist hard state (current_term, voted_for).
    fn save_hard_state(&mut self, state: &HardState) -> Result<()>;

    /// Load hard state on startup.
    fn load_hard_state(&self) -> Result<HardState>;

    /// Persist the durable applied index (must be durable before returning).
    ///
    /// `index` names an entry whose state-machine effects are ALREADY durable.
    /// The next boot resumes delivery at `index + 1`, so saving ahead of
    /// durability drops the entries in between; saving behind it re-delivers
    /// them.
    fn save_applied_index(&mut self, index: u64) -> Result<()>;

    /// Load the durable applied index on startup.
    ///
    /// Returns 0 when no index has ever been saved, which replays the whole
    /// retained log — the safe direction for storage written before this index
    /// existed.
    fn load_applied_index(&self) -> Result<u64>;
}

/// In-memory storage for testing.
#[derive(Debug, Default)]
pub struct MemStorage {
    entries: Vec<LogEntry>,
    hard_state: HardState,
    snapshot_index: u64,
    snapshot_term: u64,
    applied_index: u64,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LogStorage for MemStorage {
    fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        for entry in entries {
            // Overwrite or append.
            if let Some(pos) = self.entries.iter().position(|e| e.index == entry.index) {
                self.entries[pos] = entry.clone();
            } else {
                self.entries.push(entry.clone());
            }
        }
        Ok(())
    }

    fn truncate(&mut self, index: u64) -> Result<()> {
        self.entries.retain(|e| e.index < index);
        Ok(())
    }

    fn load_entries_after(&self, snapshot_index: u64) -> Result<Vec<LogEntry>> {
        Ok(self
            .entries
            .iter()
            .filter(|e| e.index > snapshot_index)
            .cloned()
            .collect())
    }

    fn compact(&mut self, index: u64, term: u64) -> Result<()> {
        self.entries.retain(|e| e.index > index);
        self.snapshot_index = index;
        self.snapshot_term = term;
        Ok(())
    }

    fn snapshot_metadata(&self) -> (u64, u64) {
        (self.snapshot_index, self.snapshot_term)
    }

    fn save_hard_state(&mut self, state: &HardState) -> Result<()> {
        self.hard_state = state.clone();
        Ok(())
    }

    fn load_hard_state(&self) -> Result<HardState> {
        Ok(self.hard_state.clone())
    }

    fn save_applied_index(&mut self, index: u64) -> Result<()> {
        self.applied_index = index;
        Ok(())
    }

    fn load_applied_index(&self) -> Result<u64> {
        Ok(self.applied_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_storage_append_and_load() {
        let mut s = MemStorage::new();
        let entries = vec![
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
        ];
        s.append(&entries).unwrap();

        let loaded = s.load_entries_after(0).unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn mem_storage_truncate() {
        let mut s = MemStorage::new();
        for i in 1..=5 {
            s.append(&[LogEntry {
                term: 1,
                index: i,
                data: vec![],
            }])
            .unwrap();
        }
        s.truncate(3).unwrap();
        let loaded = s.load_entries_after(0).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.last().unwrap().index, 2);
    }

    #[test]
    fn mem_storage_compact() {
        let mut s = MemStorage::new();
        for i in 1..=10 {
            s.append(&[LogEntry {
                term: 1,
                index: i,
                data: vec![],
            }])
            .unwrap();
        }
        s.compact(5, 1).unwrap();
        assert_eq!(s.snapshot_metadata(), (5, 1));
        let loaded = s.load_entries_after(5).unwrap();
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].index, 6);
    }

    #[test]
    fn mem_storage_hard_state() {
        let mut s = MemStorage::new();
        let hs = HardState {
            current_term: 5,
            voted_for: 2,
        };
        s.save_hard_state(&hs).unwrap();
        let loaded = s.load_hard_state().unwrap();
        assert_eq!(loaded, hs);
    }

    #[test]
    fn mem_storage_applied_index() {
        let mut s = MemStorage::new();
        // Never saved: replay the whole retained log.
        assert_eq!(s.load_applied_index().unwrap(), 0);

        s.save_applied_index(42).unwrap();
        assert_eq!(s.load_applied_index().unwrap(), 42);
    }
}

========== FILE: nodedb-cluster/src/install_snapshot/receiver.rs ==========
// SPDX-License-Identifier: BUSL-1.1

//! Follower-side chunk accumulator for `InstallSnapshot` RPCs.
//!
//! Each incoming `InstallSnapshotRequest` chunk is:
//! 1. Validated for offset monotonicity (`req.offset == next_expected_offset`).
//! 2. Written to `<data_dir>/recv_snapshots/<group_id>.partial` via
//!    `tokio::task::spawn_blocking` (standard `std::fs::File` — NOT O_DIRECT,
//!    NOT io_uring).
//! 3. The running CRC32C across all written bytes is updated.
//! 4. When `req.done == true`, [`super::finalize::commit`] is called.
//!
//! # Restart resume
//!
//! On `offset == 0`, we always truncate and rewrite the partial file. If a
//! `.partial` already exists from a prior interrupted transfer, the leader is
//! expected to restart from offset 0 (it detects the follower reset via the
//! response and retransmits from the beginning). This keeps the receiver
//! stateless across restarts: on startup the caller need not load partial state
//! into the map — an incoming `offset == 0` chunk rebuilds it naturally.
//!
//! Choice rationale: trusting the partial file and resuming mid-stream requires
//! re-hashing the file contents on startup to rebuild the running CRC, and
//! requires the leader to query the follower's current offset before sending.
//! Neither primitive exists yet. The simpler approach (always retransmit from
//! zero on leader-restart or follower-restart) is correct and the cost is one
//! extra round of RPC traffic.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use std::collections::HashMap;

use nodedb_raft::InstallSnapshotRequest;

use crate::error::ClusterError;
use crate::install_snapshot::finalize;
use crate::install_snapshot::state::PartialSnapshotState;
use crate::multi_raft::MultiRaft;
use crate::raft_loop::SnapshotApplier;

/// Thread-safe map of in-progress partial snapshot receives, keyed by `group_id`.
pub type PartialSnapshotMap = Mutex<HashMap<u64, PartialSnapshotState>>;

/// Outcome of processing a single incoming chunk.
#[derive(Debug)]
pub enum ChunkOutcome {
    /// More chunks are expected.
    Pending,
    /// The final chunk was received, CRC validated, and the snapshot committed.
    /// Contains the `InstallSnapshotResponse` from `MultiRaft::handle_install_snapshot`.
    Committed(nodedb_raft::InstallSnapshotResponse),
}

/// Process a single incoming `InstallSnapshotRequest` chunk.
///
/// Locks `partial_map` for the duration of state access but releases it before
/// any blocking I/O via `spawn_blocking`.
pub async fn handle_chunk(
    req: &InstallSnapshotRequest,
    partial_map: &PartialSnapshotMap,
    data_dir: &Path,
    multi_raft: &std::sync::Arc<std::sync::Mutex<MultiRaft>>,
    snapshot_applier: Option<&std::sync::Arc<dyn SnapshotApplier>>,
) -> Result<ChunkOutcome, ClusterError> {
    let group_id = req.group_id;
    let recv_dir = data_dir.join("recv_snapshots");

    // Ensure the receive directory exists.
    tokio::task::spawn_blocking({
        let recv_dir = recv_dir.clone();
        move || std::fs::create_dir_all(&recv_dir)
    })
    .await
    .map_err(|e| ClusterError::PartialSnapshotCorrupt {
        group_id,
        detail: format!("spawn_blocking join error: {e}"),
    })?
    .map_err(|e| ClusterError::Storage {
        detail: format!("create recv_snapshots dir: {e}"),
    })?;

    if req.offset == 0 {
        // Start (or restart) — open partial file with truncation.
        let partial_path = partial_path_for(&recv_dir, group_id);
        let partial_file = tokio::task::spawn_blocking({
            let path = partial_path.clone();
            move || {
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)
            }
        })
        .await
        .map_err(|e| ClusterError::PartialSnapshotCorrupt {
            group_id,
            detail: format!("spawn_blocking join error: {e}"),
        })?
        .map_err(|e| ClusterError::Storage {
            detail: format!("open partial file for group {group_id}: {e}"),
        })?;

        let state = PartialSnapshotState {
            group_id,
            leader_id: req.leader_id,
            term: req.term,
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            next_expected_offset: 0,
            running_crc: 0,
            running_crc_initialized: false,
            partial_file: Some(partial_file),
            partial_path,
        };

        let mut map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        map.insert(group_id, state);
    } else {
        // Continuation — validate the state entry exists and offset matches.
        let map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        match map.get(&group_id) {
            None => {
                // No partial state for this group. This happens after a
                // follower restart when the leader is mid-stream. Return
                // an offset regression error; the leader will restart
                // from offset 0.
                return Err(ClusterError::SnapshotOffsetRegression {
                    group_id,
                    expected: 0,
                    actual: req.offset,
                });
            }
            Some(state) if state.next_expected_offset != req.offset => {
                let expected = state.next_expected_offset;
                let actual = req.offset;
                // Drop the lock before returning the error. The caller
                // is responsible for resetting the partial state on regression.
                drop(map);
                return Err(ClusterError::SnapshotOffsetRegression {
                    group_id,
                    expected,
                    actual,
                });
            }
            Some(_) => {}
        }
        // Lock dropped here.
    }

    // Write chunk bytes to the partial file via spawn_blocking.
    let written_len = req.data.len() as u64;

    // Take the file out of the state, write via spawn_blocking, then restore it.
    let taken_file = {
        let mut map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        let state = map
            .get_mut(&group_id)
            .ok_or_else(|| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: "partial state disappeared during write".into(),
            })?;
        state
            .partial_file
            .take()
            .ok_or_else(|| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: "partial file already taken".into(),
            })?
    };
    let file = {
        tokio::task::spawn_blocking({
            let bytes = req.data.clone();
            move || -> std::io::Result<std::fs::File> {
                let mut f = taken_file;
                f.write_all(&bytes)?;
                f.flush()?;
                Ok(f)
            }
        })
        .await
        .map_err(|e| ClusterError::PartialSnapshotCorrupt {
            group_id,
            detail: format!("spawn_blocking join error during write: {e}"),
        })?
        .map_err(|e| ClusterError::Storage {
            detail: format!("write to partial file for group {group_id}: {e}"),
        })?
    };

    // Update running CRC and put the file back.
    {
        let mut map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        let state = map
            .get_mut(&group_id)
            .ok_or_else(|| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: "partial state disappeared after write".into(),
            })?;

        // Update running CRC over the raw chunk payload bytes.
        if written_len > 0 {
            if !state.running_crc_initialized {
                state.running_crc = crc32c::crc32c(&req.data);
                state.running_crc_initialized = true;
            } else {
                state.running_crc = crc32c::crc32c_append(state.running_crc, &req.data);
            }
        }

        state.next_expected_offset += written_len;
        state.partial_file = Some(file);
    }

    if !req.done {
        return Ok(ChunkOutcome::Pending);
    }

    // Final chunk: validate and commit.
    let state = {
        let mut map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(&group_id)
            .ok_or_else(|| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: "partial state disappeared before finalization".into(),
            })?
    };

    let resp = finalize::commit(state, multi_raft, snapshot_applier).await?;
    Ok(ChunkOutcome::Committed(resp))
}

pub fn partial_path_for(recv_dir: &Path, group_id: u64) -> PathBuf {
    recv_dir.join(format!("{group_id}.partial"))
}

========== FILE: nodedb-cluster/src/install_snapshot/finalize.rs ==========
// SPDX-License-Identifier: BUSL-1.1

//! Final snapshot commit: CRC validation → atomic rename → Raft log boundary advance.
//!
//! Called only when the last chunk (`done == true`) has been written to the
//! `.partial` file. Performs three operations in sequence:
//!
//! 1. **CRC validation** — re-reads the assembled file and recomputes the
//!    CRC32C. If it disagrees with the running CRC accumulated during chunk
//!    writes, the partial file is left in place and `SnapshotCrcMismatch` is
//!    returned. The partial file is intentionally *not* deleted on CRC failure
//!    so the operator can inspect it.
//!
//! 2. **Atomic rename** — the `.partial` file is renamed to `<group_id>.snap`.
//!    The rename is atomic on POSIX filesystems (same directory, same inode
//!    table). If the process crashes between steps 1 and 2, the partial file
//!    survives; the GC sweeper will remove it after `orphan_partial_max_age_secs`.
//!
//! 3. **Raft log boundary advance** — calls
//!    `MultiRaft::handle_install_snapshot` to advance the Raft log pointer to
//!    `last_included_index` / `last_included_term`. This is the same call the
//!    existing stub in `handle_rpc.rs` made; we now call it only here, after
//!    CRC validation, to prevent advancing Raft state on corrupt data.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use nodedb_raft::{InstallSnapshotRequest, InstallSnapshotResponse};

use crate::error::ClusterError;
use crate::install_snapshot::state::PartialSnapshotState;
use crate::multi_raft::MultiRaft;
use crate::raft_loop::SnapshotApplier;

/// Validate, rename, and advance Raft state after the last chunk.
///
/// Returns the `InstallSnapshotResponse` produced by
/// `MultiRaft::handle_install_snapshot` so callers can propagate the
/// Raft term back to the leader.
pub async fn commit(
    state: PartialSnapshotState,
    multi_raft: &Arc<Mutex<MultiRaft>>,
    snapshot_applier: Option<&Arc<dyn SnapshotApplier>>,
) -> Result<InstallSnapshotResponse, ClusterError> {
    let group_id = state.group_id;
    let partial_path = state.partial_path.clone();
    let expected_crc = state.running_crc;

    // Flush and close the partial file before reading it back.
    // `state.partial_file` may be `None` if the snapshot had zero bytes
    // (bootstrap stub). In that case skip the I/O validation.
    if let Some(file) = state.partial_file {
        tokio::task::spawn_blocking(move || -> std::io::Result<()> { file.sync_all() })
            .await
            .map_err(|e| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: format!("spawn_blocking join error on sync: {e}"),
            })?
            .map_err(|e| ClusterError::Storage {
                detail: format!("sync partial file for group {group_id}: {e}"),
            })?;
    }

    // CRC validation: re-read the file and compare against running CRC.
    // If the file is empty (bootstrap stub), skip.
    let file_bytes = tokio::task::spawn_blocking({
        let path = partial_path.clone();
        move || std::fs::read(&path)
    })
    .await
    .map_err(|e| ClusterError::PartialSnapshotCorrupt {
        group_id,
        detail: format!("spawn_blocking join error on read: {e}"),
    })?
    .map_err(|e| ClusterError::Storage {
        detail: format!("read partial file for group {group_id}: {e}"),
    })?;

    if !file_bytes.is_empty() {
        let computed = crc32c::crc32c(&file_bytes);
        if computed != expected_crc {
            return Err(ClusterError::SnapshotCrcMismatch {
                group_id,
                stored: expected_crc,
                computed,
            });
        }
    }

    // Atomic rename: .partial → .snap
    let snap_path = snap_path_for(&partial_path);
    tokio::task::spawn_blocking({
        let from = partial_path.clone();
        let to = snap_path.clone();
        move || std::fs::rename(&from, &to)
    })
    .await
    .map_err(|e| ClusterError::PartialSnapshotCorrupt {
        group_id,
        detail: format!("spawn_blocking join error on rename: {e}"),
    })?
    .map_err(|e| ClusterError::Storage {
        detail: format!("rename partial to snap for group {group_id}: {e}"),
    })?;

    // Apply the snapshot to the local Data-Plane state machine BEFORE advancing
    // Raft, so the data is visible on this node before the Raft log boundary
    // moves. An apply failure is fatal — we return WITHOUT advancing Raft so the
    // follower retries the install (no silent partial success). The empty
    // bootstrap stub (no engine data) is skipped: there is nothing to apply, and
    // group 0 (metadata) is a no-op the applier handles internally.
    if !file_bytes.is_empty()
        && let Some(applier) = snapshot_applier
    {
        applier
            .apply_snapshot(group_id, &file_bytes)
            .await
            .map_err(|e| ClusterError::SnapshotApplyFailed {
                group_id,
                detail: e.to_string(),
            })?;
    }

    // Advance Raft log boundary. Build a minimal InstallSnapshotRequest
    // that satisfies `handle_install_snapshot` — `data` is the assembled
    // bytes (may be empty for the bootstrap stub), `done` is always `true`.
    let req = InstallSnapshotRequest {
        term: state.term,
        leader_id: state.leader_id,
        last_included_index: state.last_included_index,
        last_included_term: state.last_included_term,
        offset: 0,
        data: file_bytes,
        done: true,
        group_id,
        total_size: 0,
    };

    let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
    let resp = mr.handle_install_snapshot(&req)?;
    // Persist any term bump (become_follower) durably before replying.
    mr.persist_group_hard_state(group_id)?;
    Ok(resp)
}

/// Derive the `.snap` path from the `.partial` path (same directory, stem only).
fn snap_path_for(partial: &std::path::Path) -> PathBuf {
    let parent = partial
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let stem = partial
        .file_stem()
        .unwrap_or_else(|| std::ffi::OsStr::new("unknown"));
    parent.join(format!("{}.snap", stem.to_string_lossy()))
}

========== FILE: nodedb-cluster/src/install_snapshot/sender.rs ==========
// SPDX-License-Identifier: BUSL-1.1

//! Leader-side chunked `InstallSnapshot` sender.
//!
//! Slices `snapshot_bytes` into chunks of at most `chunk_bytes`, wraps each
//! with [`nodedb_raft::encode_snapshot_chunk`] framing, and fires one
//! `InstallSnapshotRequest` RPC per chunk. When `snapshot_bytes` is empty,
//! exactly one chunk is emitted with `data = vec![]` and `done = true` — this
//! is the bootstrap stub path that keeps `tick.rs` correct even before any
//! engine ships real snapshot data.
//!
//! The caller is responsible for calling this inside a `tokio::spawn` task
//! (as the existing tick loop already does) so the RPC does not block the
//! tick pipeline.

use nodedb_raft::{
    InstallSnapshotRequest, SnapshotEngineId, encode_snapshot_chunk, transport::RaftTransport,
};

use crate::error::ClusterError;
use crate::transport::NexarTransport;

/// Parameters for [`send_chunked`].
pub struct SendChunkedParams<'a> {
    pub peer: u64,
    pub group_id: u64,
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub snapshot_bytes: &'a [u8],
    pub chunk_bytes: u64,
}

/// Leader-side chunked send for a single peer.
///
/// Emits `ceil(snapshot_bytes.len() / chunk_bytes)` RPCs (minimum 1 for an
/// empty snapshot). On RPC failure the function returns the error immediately;
/// the caller should log and not retry — the next Raft tick will re-schedule
/// the snapshot if the peer is still behind.
///
/// Returns the final `InstallSnapshotResponse.term` so the tick loop can
/// detect a higher-term response and step down.
pub async fn send_chunked(
    transport: &NexarTransport,
    params: SendChunkedParams<'_>,
) -> Result<u64, ClusterError> {
    let SendChunkedParams {
        peer,
        group_id,
        term,
        leader_id,
        last_included_index,
        last_included_term,
        snapshot_bytes,
        chunk_bytes,
    } = params;
    // For an empty snapshot we send exactly one stub chunk with done=true.
    if snapshot_bytes.is_empty() {
        let req = InstallSnapshotRequest {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            offset: 0,
            data: vec![],
            done: true,
            group_id,
            total_size: 0,
        };
        let resp =
            transport
                .install_snapshot(peer, req)
                .await
                .map_err(|e| ClusterError::Transport {
                    detail: format!("install_snapshot peer={peer} group={group_id}: {e}"),
                })?;
        return Ok(resp.term);
    }

    let chunk_size = chunk_bytes.max(1) as usize;
    let total = snapshot_bytes.len() as u64;
    let mut offset = 0usize;
    let mut last_term = term;

    while offset < snapshot_bytes.len() {
        let end = (offset + chunk_size).min(snapshot_bytes.len());
        let chunk_payload = &snapshot_bytes[offset..end];
        let done = end == snapshot_bytes.len();

        // Framing: wrap each chunk with the snapshot frame header (magic +
        // version + engine id + CRC) so the receiver's `handle_rpc` boundary can
        // validate per-chunk integrity before stripping the header and
        // accumulating the raw payload. The DataPlane snapshot payload is a
        // whole-tenant composite (`TenantDataSnapshot` spanning every engine),
        // so it is tagged with the `Composite` engine id rather than any single
        // engine's. `offset`/`total_size` below are payload-space (into
        // `snapshot_bytes`); the receiver reassembles the stripped payloads.
        let framed = encode_snapshot_chunk(SnapshotEngineId::Composite, chunk_payload);

        let req = InstallSnapshotRequest {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            offset: offset as u64,
            data: framed,
            done,
            group_id,
            total_size: total,
        };
        let resp =
            transport
                .install_snapshot(peer, req)
                .await
                .map_err(|e| ClusterError::Transport {
                    detail: format!(
                        "install_snapshot peer={peer} group={group_id} offset={offset}: {e}"
                    ),
                })?;
        last_term = resp.term;
        offset = end;
    }

    Ok(last_term)
}

========== FILE: nodedb-cluster/src/follower_read.rs ==========
// SPDX-License-Identifier: BUSL-1.1

//! Follower-read decision gate.
//!
//! [`FollowerReadGate`] answers a single question: "given the
//! session's `ReadConsistency` and the local node's role + closed
//! timestamp for the target Raft group, can this read be served
//! locally without forwarding to the leader?"
//!
//! ## Decision table
//!
//! | Consistency           | Local role  | Closed TS fresh? | Serve locally? |
//! |-----------------------|-------------|------------------|----------------|
//! | Strong                | *           | *                | Only if leader |
//! | BoundedStaleness(d)   | Follower    | ≤ d              | Yes            |
//! | BoundedStaleness(d)   | Follower    | > d              | No → forward   |
//! | BoundedStaleness(d)   | Leader      | *                | Yes            |
//! | Eventual              | *           | *                | Yes            |
//!
//! The gate is stateless — it reads from shared handles to the
//! closed-timestamp tracker and the raft-status provider.

use std::sync::Arc;
use std::time::Duration;

use crate::closed_timestamp::ClosedTimestampTracker;

/// Consistency level for a single read — mirrors the `ReadConsistency`
/// enum in the `nodedb` crate without coupling `nodedb-cluster` to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadLevel {
    Strong,
    BoundedStaleness(Duration),
    Eventual,
}

/// Answers "can this read be served locally?"
pub struct FollowerReadGate {
    closed_ts: Arc<ClosedTimestampTracker>,
    /// Type-erased function that returns true if this node is the
    /// leader for the given group. Injection seam — production wraps
    /// `MultiRaft::group_statuses`, tests supply a closure.
    is_leader_fn: Box<dyn Fn(u64) -> bool + Send + Sync>,
}

impl FollowerReadGate {
    pub fn new(
        closed_ts: Arc<ClosedTimestampTracker>,
        is_leader_fn: Box<dyn Fn(u64) -> bool + Send + Sync>,
    ) -> Self {
        Self {
            closed_ts,
            is_leader_fn,
        }
    }

    /// Returns `true` if the read can be served from this node's
    /// local replica without forwarding to the leader.
    pub fn can_serve_locally(&self, group_id: u64, level: ReadLevel) -> bool {
        match level {
            ReadLevel::Strong => (self.is_leader_fn)(group_id),
            ReadLevel::Eventual => true,
            ReadLevel::BoundedStaleness(max) => {
                if (self.is_leader_fn)(group_id) {
                    return true;
                }
                self.closed_ts.is_fresh_enough(group_id, max)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(leader_groups: &'static [u64]) -> FollowerReadGate {
        FollowerReadGate::new(
            Arc::new(ClosedTimestampTracker::new()),
            Box::new(move |gid| leader_groups.contains(&gid)),
        )
    }

    fn gate_with_tracker(
        leader_groups: &'static [u64],
        tracker: Arc<ClosedTimestampTracker>,
    ) -> FollowerReadGate {
        FollowerReadGate::new(tracker, Box::new(move |gid| leader_groups.contains(&gid)))
    }

    #[test]
    fn strong_requires_leader() {
        let g = gate(&[1]);
        assert!(g.can_serve_locally(1, ReadLevel::Strong));
        assert!(!g.can_serve_locally(2, ReadLevel::Strong));
    }

    #[test]
    fn eventual_always_local() {
        let g = gate(&[]);
        assert!(g.can_serve_locally(99, ReadLevel::Eventual));
    }

    #[test]
    fn bounded_staleness_leader_always_local() {
        let g = gate(&[1]);
        assert!(g.can_serve_locally(1, ReadLevel::BoundedStaleness(Duration::from_secs(5))));
    }

    #[test]
    fn bounded_staleness_follower_fresh_enough() {
        let tracker = Arc::new(ClosedTimestampTracker::new());
        tracker.mark_applied(2);
        let g = gate_with_tracker(&[], tracker);
        assert!(g.can_serve_locally(2, ReadLevel::BoundedStaleness(Duration::from_secs(5))));
    }

    #[test]
    fn bounded_staleness_follower_too_stale() {
        let tracker = Arc::new(ClosedTimestampTracker::new());
        let old = std::time::Instant::now() - Duration::from_secs(30);
        tracker.mark_applied_at(2, old);
        let g = gate_with_tracker(&[], tracker);
        assert!(!g.can_serve_locally(2, ReadLevel::BoundedStaleness(Duration::from_secs(5))));
    }

    #[test]
    fn bounded_staleness_unknown_group_not_local() {
        let g = gate(&[]);
        assert!(!g.can_serve_locally(99, ReadLevel::BoundedStaleness(Duration::from_secs(5))));
    }
}

========== FILE: nodedb-cluster/src/closed_timestamp.rs ==========
// SPDX-License-Identifier: BUSL-1.1

//! Per-group closed-timestamp tracker with HLC skew bounding.
//!
//! Every time a Raft group applies a committed entry, the applier
//! records the wall-clock instant as that group's "closed timestamp".
//! A follower whose closed timestamp for a group is within the
//! caller's staleness bound can serve reads locally — no gateway hop
//! to the leader.
//!
//! ## HLC integration
//!
//! The tracker also owns the node-wide [`HlcClock`]. When an apply
//! path knows the leader-stamped `Hlc` for the entry it is applying,
//! it should call [`ClosedTimestampTracker::fold_remote_hlc`] instead
//! of [`ClosedTimestampTracker::mark_applied`]. Folding the remote
//! HLC into the local clock bounds cross-node `_ts_system` skew at
//! this node: any subsequent local stamp is strictly greater than
//! every observed remote HLC, so versions written here can never
//! collide with — or appear earlier than — versions a leader has
//! already replicated.
//!
//! Apply-side wiring is intentionally optional. Code paths that don't
//! yet thread the leader's HLC keep using `mark_applied` and only
//! lose the cross-node skew bound; correctness of the local
//! `_ts_system` stamp is unaffected because [`HlcClock::now`] already
//! advances past the local wall clock.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nodedb_types::{Hlc, HlcClock};

/// Tracks the most recent apply instant per Raft group plus the
/// shared node-wide HLC.
pub struct ClosedTimestampTracker {
    groups: RwLock<HashMap<u64, Instant>>,
    hlc: Arc<HlcClock>,
}

impl ClosedTimestampTracker {
    /// Construct a tracker with a fresh, node-private HLC. Tests and
    /// stand-alone follower-read setups use this; production paths
    /// should call [`Self::with_hlc`] to share the node-wide clock.
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            hlc: Arc::new(HlcClock::new()),
        }
    }

    /// Construct a tracker wired to a caller-supplied HLC. Use this
    /// in production so the tracker's `fold_remote_hlc` advances the
    /// same clock that other subsystems read via `now()`.
    pub fn with_hlc(hlc: Arc<HlcClock>) -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            hlc,
        }
    }

    /// Read access to the shared HLC. Other apply-side subsystems
    /// (descriptor leases, metadata cache) advance and read it
    /// through this handle.
    pub fn hlc(&self) -> &Arc<HlcClock> {
        &self.hlc
    }

    /// Record that `group_id` just applied one or more entries.
    /// Called by the raft-loop applier after each apply batch.
    pub fn mark_applied(&self, group_id: u64) {
        let mut g = self.groups.write().unwrap_or_else(|p| p.into_inner());
        g.insert(group_id, Instant::now());
    }

    /// Record that `group_id` just applied, using a caller-supplied
    /// instant. Exposed for deterministic testing with paused time.
    pub fn mark_applied_at(&self, group_id: u64, at: Instant) {
        let mut g = self.groups.write().unwrap_or_else(|p| p.into_inner());
        g.insert(group_id, at);
    }

    /// Mark a group applied AND fold the leader-stamped `remote` HLC
    /// into the local clock. Returns the merged HLC that any local
    /// stamp emitted after this call is guaranteed to be strictly
    /// greater than.
    ///
    /// This is the production apply-path entry point: every committed
    /// entry that carries a leader HLC (descriptor leases, catalog
    /// DDL, drain events) should route through here so cross-node
    /// `_ts_system` skew is bounded at this node.
    pub fn fold_remote_hlc(&self, group_id: u64, remote: Hlc) -> Hlc {
        self.mark_applied(group_id);
        self.hlc.update(remote)
    }

    /// Check whether this node's replica of `group_id` has applied
    /// recently enough that a read with `max_staleness` can be
    /// served locally.
    ///
    /// Returns `false` if the group has never applied on this node
    /// (no closed timestamp recorded).
    pub fn is_fresh_enough(&self, group_id: u64, max_staleness: Duration) -> bool {
        let g = self.groups.read().unwrap_or_else(|p| p.into_inner());
        match g.get(&group_id) {
            Some(last) => last.elapsed() <= max_staleness,
            None => false,
        }
    }

    /// Return the age of the closed timestamp for a group, or `None`
    /// if the group has never applied on this node. Useful for
    /// observability (metrics, SHOW commands).
    pub fn staleness(&self, group_id: u64) -> Option<Duration> {
        let g = self.groups.read().unwrap_or_else(|p| p.into_inner());
        g.get(&group_id).map(|last| last.elapsed())
    }
}

impl Default for ClosedTimestampTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_group_is_not_fresh() {
        let tracker = ClosedTimestampTracker::new();
        assert!(!tracker.is_fresh_enough(99, Duration::from_secs(10)));
    }

    #[test]
    fn recently_applied_is_fresh() {
        let tracker = ClosedTimestampTracker::new();
        tracker.mark_applied(1);
        assert!(tracker.is_fresh_enough(1, Duration::from_secs(5)));
    }

    #[test]
    fn stale_group_is_not_fresh() {
        let tracker = ClosedTimestampTracker::new();
        let old = Instant::now() - Duration::from_secs(30);
        tracker.mark_applied_at(1, old);
        assert!(!tracker.is_fresh_enough(1, Duration::from_secs(5)));
    }

    #[test]
    fn staleness_returns_none_for_unknown() {
        let tracker = ClosedTimestampTracker::new();
        assert!(tracker.staleness(42).is_none());
    }

    #[test]
    fn staleness_returns_age_for_known() {
        let tracker = ClosedTimestampTracker::new();
        tracker.mark_applied(1);
        let s = tracker.staleness(1).unwrap();
        assert!(s < Duration::from_millis(100));
    }

    #[test]
    fn mark_applied_updates_monotonically() {
        let tracker = ClosedTimestampTracker::new();
        let old = Instant::now() - Duration::from_secs(10);
        tracker.mark_applied_at(1, old);
        assert!(!tracker.is_fresh_enough(1, Duration::from_secs(5)));
        tracker.mark_applied(1);
        assert!(tracker.is_fresh_enough(1, Duration::from_secs(5)));
    }

    #[test]
    fn fold_remote_hlc_bounds_cross_node_skew() {
        // Local clock is fresh — its first `now()` will sit near
        // current wall-clock. A leader far in the future stamps an
        // entry; folding it MUST advance the local clock past it so
        // any subsequent local stamp can never collide with or
        // precede the leader's observation.
        let tracker = ClosedTimestampTracker::new();
        let local_before = tracker.hlc().now();
        let remote = Hlc::new(local_before.wall_ns + 60_000_000_000, 7); // +60s
        let merged = tracker.fold_remote_hlc(1, remote);

        assert!(merged > remote, "merged HLC strictly greater than remote");
        assert!(
            merged > local_before,
            "merged HLC strictly greater than prior local"
        );
        assert!(tracker.is_fresh_enough(1, Duration::from_secs(5)));

        // Subsequent local `now()` is strictly greater than the merged
        // observation — the skew bound holds for every following stamp.
        let after = tracker.hlc().now();
        assert!(
            after > merged,
            "subsequent local stamp dominates folded remote"
        );
    }

    #[test]
    fn fold_remote_hlc_idempotent_under_replay() {
        // Replaying the same remote HLC must not regress the clock.
        let tracker = ClosedTimestampTracker::new();
        let remote = Hlc::new(1_000_000_000_000, 0);
        let first = tracker.fold_remote_hlc(1, remote);
        let second = tracker.fold_remote_hlc(1, remote);
        assert!(
            second > first,
            "replay still advances logical counter, never regresses"
        );
    }

    #[test]
    fn with_hlc_shares_clock_across_subsystems() {
        // Two trackers sharing one HlcClock observe each other's
        // remote folds. This is the production wiring shape:
        // ClosedTimestampTracker + MetadataCache + descriptor lease
        // applier all hold the same Arc<HlcClock>.
        let hlc = Arc::new(HlcClock::new());
        let t1 = ClosedTimestampTracker::with_hlc(Arc::clone(&hlc));
        let t2 = ClosedTimestampTracker::with_hlc(Arc::clone(&hlc));

        let remote = Hlc::new(2_000_000_000_000, 5);
        let merged = t1.fold_remote_hlc(1, remote);
        // t2's clock has already advanced past `remote` because the
        // Arc is shared.
        let observed = t2.hlc().now();
        assert!(observed > merged);
    }
}

========== FILE: nodedb-cluster/src/swim/bootstrap.rs ==========
// SPDX-License-Identifier: BUSL-1.1

//! SWIM subsystem bootstrap.
//!
//! [`spawn`] is the one-stop entry point callers (cluster startup or
//! tests) use to stand up a running failure detector:
//!
//! 1. Constructs a [`MembershipList`] containing the local node at
//!    incarnation 0.
//! 2. Seeds the list with an `Alive` entry for every address in
//!    `seeds`, using a synthetic `NodeId` of the form `"seed:<addr>"`.
//!    The first successful probe replaces the placeholder with the
//!    peer's real node id via the normal merge path.
//! 3. Validates [`SwimConfig`] and constructs a [`FailureDetector`].
//! 4. Spawns the detector's run loop on a fresh tokio task.
//! 5. Returns a [`SwimHandle`] the caller can use to read membership,
//!    access the dissemination queue, and shut the detector down.

use std::net::SocketAddr;
use std::sync::Arc;

use nodedb_types::NodeId;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::config::SwimConfig;
use super::detector::{FailureDetector, ProbeScheduler, Transport};
use super::dissemination::DisseminationQueue;
use super::error::SwimError;
use super::incarnation::Incarnation;
use super::member::MemberState;
use super::member::record::MemberUpdate;
use super::membership::MembershipList;
use super::subscriber::MembershipSubscriber;

/// Owns a running SWIM detector and its shutdown plumbing.
///
/// Dropping `SwimHandle` leaks the background task — callers should
/// always invoke [`SwimHandle::shutdown`] to request graceful drain.
pub struct SwimHandle {
    detector: Arc<FailureDetector>,
    membership: Arc<MembershipList>,
    shutdown_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl SwimHandle {
    /// Shared reference to the detector (for metrics, debugging, or
    /// injecting synthetic rumours in tests).
    pub fn detector(&self) -> &Arc<FailureDetector> {
        &self.detector
    }

    /// Shared reference to the membership list. Clone cheaply; the
    /// underlying `Arc` is identical to the detector's view.
    pub fn membership(&self) -> &Arc<MembershipList> {
        &self.membership
    }

    /// Shared reference to the dissemination queue. Used by callers
    /// that want to enqueue rumours from outside SWIM (e.g. the raft
    /// layer announcing a conf change).
    pub fn dissemination(&self) -> &Arc<DisseminationQueue> {
        self.detector.dissemination()
    }

    /// Signal the detector to shut down and await its task to finish.
    /// Returns whatever error the join handle surfaced (normally none).
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join.await;
    }
}

/// Bring up a SWIM failure detector.
///
/// * `cfg` — validated [`SwimConfig`]. An invalid config returns
///   [`SwimError::InvalidConfig`] before any task is spawned.
/// * `local_id` — this node's canonical id.
/// * `local_addr` — the socket address the transport is already bound
///   to. The membership list stores it verbatim for peers to echo back
///   in probe responses.
/// * `seeds` — initial peer addresses. Empty list is legal and yields a
///   solo-cluster detector that does nothing interesting until a peer
///   arrives via an external join.
/// * `transport` — any [`Transport`] impl (UDP in production, the
///   in-memory fabric in tests).
pub async fn spawn(
    cfg: SwimConfig,
    local_id: NodeId,
    local_addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    transport: Arc<dyn Transport>,
) -> Result<SwimHandle, SwimError> {
    spawn_with_subscribers(cfg, local_id, local_addr, seeds, transport, Vec::new()).await
}

/// Same as [`spawn`] but installs the given [`MembershipSubscriber`]s
/// on the detector before its run loop starts, so every state
/// transition is observed from the very first probe round.
pub async fn spawn_with_subscribers(
    cfg: SwimConfig,
    local_id: NodeId,
    local_addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    transport: Arc<dyn Transport>,
    subscribers: Vec<Arc<dyn MembershipSubscriber>>,
) -> Result<SwimHandle, SwimError> {
    cfg.validate()?;

    let membership = Arc::new(MembershipList::new_local(
        local_id.clone(),
        local_addr,
        cfg.initial_incarnation,
    ));

    // Seed the membership table so the first probe round has somewhere
    // to go. Placeholder ids are replaced on the first ack.
    for seed_addr in &seeds {
        if *seed_addr == local_addr {
            continue;
        }
        membership.apply(&MemberUpdate {
            // SocketAddr display always produces a valid ID: non-empty, well under cap, no NUL.
            node_id: NodeId::from_validated(format!("seed:{seed_addr}")),
            addr: seed_addr.to_string(),
            state: MemberState::Alive,
            incarnation: Incarnation::ZERO,
        });
    }

    let initial_inc = cfg.initial_incarnation;
    let detector = Arc::new(FailureDetector::with_subscribers(
        cfg,
        Arc::clone(&membership),
        transport,
        ProbeScheduler::new(),
        subscribers,
    ));

    // Prime the dissemination queue with our own Alive record so the
    // first outgoing probes advertise our canonical NodeId + addr to
    // every seed. Without this, seed placeholders would never be
    // replaced with real ids until some peer independently learned
    // our identity — which is not reliable from seed bootstrap alone.
    detector.dissemination().enqueue(MemberUpdate {
        node_id: local_id.clone(),
        addr: local_addr.to_string(),
        state: MemberState::Alive,
        incarnation: initial_inc,
    });

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let join = tokio::spawn({
        let detector = Arc::clone(&detector);
        async move { detector.run(shutdown_rx).await }
    });

    Ok(SwimHandle {
        detector,
        membership,
        shutdown_tx,
        join,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::detector::TransportFabric;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p)
    }

    fn cfg() -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            probe_timeout: Duration::from_millis(40),
            indirect_probes: 2,
            suspicion_mult: 4,
            min_suspicion: Duration::from_millis(500),
            initial_incarnation: Incarnation::ZERO,
            max_piggyback: 6,
            fanout_lambda: 3,
        }
    }

    #[tokio::test]
    async fn spawn_solo_cluster_has_only_local() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7100)).await);
        let handle = spawn(
            cfg(),
            NodeId::try_new("a").expect("test fixture"),
            addr(7100),
            vec![],
            transport,
        )
        .await
        .expect("spawn");
        assert_eq!(handle.membership().len(), 1);
        assert!(handle.membership().is_solo());
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn spawn_seeds_populates_membership() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7110)).await);
        let handle = spawn(
            cfg(),
            NodeId::try_new("a").expect("test fixture"),
            addr(7110),
            vec![addr(7111), addr(7112)],
            transport,
        )
        .await
        .expect("spawn");
        assert_eq!(handle.membership().len(), 3);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn spawn_skips_local_addr_in_seeds() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7120)).await);
        let handle = spawn(
            cfg(),
            NodeId::try_new("a").expect("test fixture"),
            addr(7120),
            vec![addr(7120), addr(7121)],
            transport,
        )
        .await
        .expect("spawn");
        // Local + one real seed = 2.
        assert_eq!(handle.membership().len(), 2);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_config_rejected_before_task_spawned() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7130)).await);
        let mut bad = cfg();
        bad.probe_timeout = bad.probe_interval; // violates the strict-less rule
        let res = spawn(
            bad,
            NodeId::try_new("a").expect("test fixture"),
            addr(7130),
            vec![],
            transport,
        )
        .await;
        match res {
            Err(SwimError::InvalidConfig { .. }) => {}
            Err(other) => panic!("expected InvalidConfig, got {other:?}"),
            Ok(_) => panic!("expected InvalidConfig error"),
        }
    }

    #[tokio::test]
    async fn shutdown_joins_promptly() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7140)).await);
        let handle = spawn(
            cfg(),
            NodeId::try_new("a").expect("test fixture"),
            addr(7140),
            vec![],
            transport,
        )
        .await
        .expect("spawn");
        let start = std::time::Instant::now();
        tokio::time::timeout(Duration::from_millis(500), handle.shutdown())
            .await
            .expect("shutdown did not join within budget");
        assert!(start.elapsed() < Duration::from_millis(500));
    }
}

========== FILE: nodedb-cluster/src/install_snapshot/gc.rs ==========
// SPDX-License-Identifier: BUSL-1.1

//! Orphan partial-snapshot cleanup.
//!
//! Scans `<data_dir>/recv_snapshots/` for `.partial` files whose last
//! modification time is older than `max_age_secs`. Removes those files.
//! Fresh `.partial` files (recently-modified) are left untouched.
//!
//! # When to call
//!
//! `sweep_orphans` is called at two points in the node lifecycle:
//! - Once at node startup (via [`crate::raft_loop::loop_core::RaftLoop::run`]),
//!   to remove leftover files from previous runs that did not complete.
//! - Periodically (~every 60 s) from the Raft tick loop, to reclaim disk space
//!   from partial files that accumulate during the node's lifetime without
//!   requiring a restart.

use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::error::ClusterError;

/// Remove orphaned `.partial` snapshot files older than `max_age_secs`.
///
/// Errors on individual files are returned as `PartialSnapshotCleanupFailed`
/// variants inside the result `Vec`. All files are attempted; a failure on one
/// does not abort the sweep. The caller may log or surface these errors.
///
/// Returns `Ok(removed_count)` even if some individual removals failed (those
/// failures are in the returned error vec). Returns `Err` only on directory
/// enumeration failure.
pub fn sweep_orphans(
    data_dir: &Path,
    max_age_secs: u64,
) -> Result<(usize, Vec<ClusterError>), ClusterError> {
    let recv_dir = data_dir.join("recv_snapshots");

    // If the directory doesn't exist yet there is nothing to sweep.
    if !recv_dir.exists() {
        return Ok((0, vec![]));
    }

    let entries = std::fs::read_dir(&recv_dir).map_err(|e| ClusterError::Storage {
        detail: format!("read_dir recv_snapshots: {e}"),
    })?;

    let max_age = Duration::from_secs(max_age_secs);
    let now = SystemTime::now();

    let mut removed = 0usize;
    let mut errors = Vec::new();

    for entry_result in entries {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                errors.push(ClusterError::Storage {
                    detail: format!("iterate recv_snapshots: {e}"),
                });
                continue;
            }
        };

        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("partial") {
            continue;
        }

        // Extract group_id from the file stem for error messages.
        let group_id: u64 = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let age = match entry.metadata().and_then(|m| m.modified()) {
            Ok(mtime) => match now.duration_since(mtime) {
                Ok(d) => d,
                Err(_) => Duration::ZERO, // mtime in the future; treat as fresh
            },
            Err(e) => {
                errors.push(ClusterError::PartialSnapshotCleanupFailed {
                    group_id,
                    detail: format!("stat {}: {e}", path.display()),
                });
                continue;
            }
        };

        if age < max_age {
            continue;
        }

        if let Err(e) = std::fs::remove_file(&path) {
            errors.push(ClusterError::PartialSnapshotCleanupFailed {
                group_id,
                detail: format!("remove {}: {e}", path.display()),
            });
        } else {
            removed += 1;
        }
    }

    Ok((removed, errors))
}

========== FILE: nodedb-types/src/wire_version.rs ==========
// SPDX-License-Identifier: Apache-2.0

//! Single source of truth for the `WIRE_FORMAT_VERSION` constant
//! shared between every crate that needs to stamp or interpret it.
//!
//! This is the *cluster-wide* wire format version, distinct from:
//! - `nodedb_cluster::wire::WIRE_VERSION` (the binary frame layout
//!   version of the `VShardEnvelope`),
//! - the RPC frame header version in
//!   `nodedb_cluster::rpc_codec::header` (a private constant of that
//!   module).
//!
//! # DO NOT BUMP THIS BEFORE 1.0
//!
//! It stays at `1` until the first stable release. Read this before
//! changing it — the reflex to bump on any wire-shape change is wrong here:
//!
//! - **There is nothing to be compatible with.** Pre-1.0 there are no
//!   deployed clusters, so there is no older peer a new build must talk to.
//! - **A bump cannot buy a rolling upgrade.** `MIN_WIRE_FORMAT_VERSION ==
//!   WIRE_FORMAT_VERSION` (floor == ceiling), so a node rejects *any* peer
//!   whose version differs. Mixed-version clusters cannot form at all, which
//!   makes every `wire_version >= V` feature gate dead code: inside a cluster
//!   that exists, all nodes are provably on this exact version. Adding such a
//!   gate is unreachable-branch hardening, not safety.
//! - **This value is NOT persisted.** It is stamped on `NodeInfo` for the
//!   handshake and drives `ClusterVersionView`, nothing more. The version
//!   written into stored raft-log and metadata entries is
//!   `nodedb_cluster::wire_version::WireVersion::CURRENT`, which is separate
//!   and independent. Changing the constant here therefore cannot orphan or
//!   corrupt anything already on disk.
//!
//! So: adding a new enum variant, RPC, or payload field needs NO bump. Every
//! node in a working cluster runs the same build by construction. Ratcheting
//! this pre-1.0 only invents a stop-the-world upgrade requirement that does
//! not otherwise exist, and would leave 1.0 shipping as "wire version 20" for
//! no reason.
//!
//! After 1.0, when real deployments exist and a genuine compatibility window
//! is introduced, this becomes meaningful — bump it then, deliberately, and
//! only alongside an actual `MIN_WIRE_FORMAT_VERSION < WIRE_FORMAT_VERSION`
//! support window.

/// Cluster-wide wire format version. Stamped on every `NodeInfo` and
/// returned by `nodedb::version::WIRE_FORMAT_VERSION` (a re-export).
///
/// WARNING: pinned at 1 until 1.0. See the module docs above before changing.
pub const WIRE_FORMAT_VERSION: u16 = 1;

/// Minimum wire format version this build can read. Equal to
/// `WIRE_FORMAT_VERSION`: floor == ceiling, no backward compat window.
pub const MIN_WIRE_FORMAT_VERSION: u16 = WIRE_FORMAT_VERSION;

// Compile-time invariants — these constants must satisfy:
//   - MIN_WIRE_FORMAT_VERSION <= WIRE_FORMAT_VERSION
//   - WIRE_FORMAT_VERSION > 0
const _: () = assert!(MIN_WIRE_FORMAT_VERSION <= WIRE_FORMAT_VERSION);
const _: () = assert!(WIRE_FORMAT_VERSION > 0);

