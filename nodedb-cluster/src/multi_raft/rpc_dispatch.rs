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
