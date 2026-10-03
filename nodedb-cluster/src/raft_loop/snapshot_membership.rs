// SPDX-License-Identifier: BUSL-1.1

//! A data group's membership, adopted from the final chunk of an
//! `InstallSnapshot`.
//!
//! A snapshot replaces the log through its index, conf changes included.
//! This node never applies the conf changes it covers, so its Raft and
//! routing view of the group's voters and learners would stay as they were
//! before the gap. The leader sends its membership on the final chunk, and
//! this node takes it before the install.
//!
//! The membership is taken only from a leader at or above this node's term
//! for the group. A leader's membership moves only by applying a committed
//! conf change, so it holds committed changes only, and taking it ahead of
//! the install is safe: a conf change above the snapshot index applies again
//! from the log, and applying a change twice is a no-op.
//!
//! The metadata group takes its membership from the routing table its
//! snapshot image carries (see [`crate::install_snapshot::finalize`]).

use nodedb_raft::InstallSnapshotRequest;
use tracing::debug;

use crate::error::{ClusterError, Result};
use crate::forward::PlanExecutor;
use crate::metadata_group::METADATA_GROUP_ID;

use super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Set a data group's voters and learners to the ones the final chunk
    /// `req` carries, in this node's routing view and Raft, and save the
    /// routing table.
    ///
    /// The save comes before the install moves the durable applied floor
    /// past the covered conf changes, so a restart mounts the group with this
    /// membership. It runs through the routing persister, off the async
    /// threads, and this handler awaits it. A failed save fails the install,
    /// and the leader sends the snapshot again.
    ///
    /// A no-op for the metadata group, for a chunk that carries no voters,
    /// for a group this node does not host, and for a sender below this
    /// node's term for the group.
    pub(super) async fn adopt_snapshot_membership(
        &self,
        req: &InstallSnapshotRequest,
    ) -> Result<()> {
        let group_id = req.group_id;
        if group_id == METADATA_GROUP_ID || req.voters.is_empty() {
            return Ok(());
        }
        {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if !mr.contains_group(group_id) {
                return Ok(());
            }
            let (_, term) = mr.group_leader_at_term(group_id);
            if req.term < term {
                return Ok(());
            }
            let routing = mr.routing();
            {
                let mut table = routing.write().unwrap_or_else(|p| p.into_inner());
                if table.group_info(group_id).is_none() {
                    return Ok(());
                }
                table.set_group_members(group_id, req.voters.clone());
                table.set_group_learners(group_id, req.learners.clone());
            }
            mr.sync_group_membership_from_routing(group_id)?;
        }
        debug!(
            group_id,
            voters = ?req.voters,
            learners = ?req.learners,
            "install snapshot: membership adopted from the leader"
        );
        let Some(persister) = self.routing_persister.as_ref() else {
            return Ok(());
        };
        if persister.wait(persister.request()).await {
            return Ok(());
        }
        Err(ClusterError::Storage {
            detail: format!(
                "could not save the routing table with group {group_id}'s snapshot membership; \
                 the leader sends the snapshot again"
            ),
        })
    }
}
