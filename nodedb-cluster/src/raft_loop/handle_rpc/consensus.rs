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
