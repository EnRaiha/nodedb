// SPDX-License-Identifier: BUSL-1.1

//! `InstallSnapshot` request handler.

use tracing::info;

use crate::error::Result;
use crate::message::{InstallSnapshotRequest, InstallSnapshotResponse};
use crate::node::core::RaftNode;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Handle incoming InstallSnapshot RPC (Raft paper Figure 13).
    ///
    /// Called on followers (and learners) that are too far behind for
    /// log-based catch-up. The leader sends its snapshot; the receiver
    /// replaces its log and state.
    ///
    /// The CALLER MUST have restored the snapshot into the state machine
    /// before invoking this: installing the snapshot moves the durable applied
    /// floor to `last_included_index`, which asserts those effects are durable.
    pub fn handle_install_snapshot(
        &mut self,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        if req.term < self.hard_state.current_term {
            return Ok(InstallSnapshotResponse {
                term: self.hard_state.current_term,
            });
        }

        if req.term > self.hard_state.current_term {
            self.become_follower(req.term);
        }

        self.leader_id = req.leader_id;
        self.reset_election_timeout();

        if req.done && req.last_included_index > self.log.snapshot_index() {
            info!(
                node = self.config.node_id,
                group = self.config.group_id,
                snapshot_index = req.last_included_index,
                snapshot_term = req.last_included_term,
                "applying installed snapshot"
            );

            self.log
                .apply_snapshot(req.last_included_index, req.last_included_term);

            if self.volatile.commit_index < req.last_included_index {
                self.volatile.commit_index = req.last_included_index;
            }
            if self.volatile.last_applied < req.last_included_index {
                self.volatile.last_applied = req.last_included_index;
            }
            // Move the durable floor with the snapshot boundary. The entries
            // the snapshot subsumes are gone from the log, so a restart that
            // resumed from the pre-snapshot floor would replay from an index
            // the log can no longer serve.
            self.save_durable_applied_index(req.last_included_index)?;
        }

        Ok(InstallSnapshotResponse {
            term: self.hard_state.current_term,
        })
    }
}
