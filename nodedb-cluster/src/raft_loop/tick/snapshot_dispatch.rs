// SPDX-License-Identifier: BUSL-1.1

//! Install-snapshot dispatch for peers that have fallen behind the leader's
//! snapshot boundary (`group_ready.snapshots_needed`).

use tracing::{debug, warn};

use crate::forward::PlanExecutor;

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Dispatch `InstallSnapshot` RPCs for every peer this group's `Ready`
    /// output flagged as needing one. Called only when
    /// `!group_ready.snapshots_needed.is_empty()`.
    pub(super) fn dispatch_group_snapshots(&self, group_id: u64, snapshots_needed: Vec<u64>) {
        let (snapshot_meta, in_flight_snapshots) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            (
                mr.snapshot_metadata(group_id).ok(),
                mr.in_flight_snapshots(),
            )
        };

        if let Some((term, snap_index, snap_term)) = snapshot_meta {
            for peer in snapshots_needed {
                let transport = self.transport.clone();
                let mr = self.multi_raft.clone();
                let mut shutdown_rx = self.shutdown_watch.subscribe();
                let node_id = self.node_id;
                let chunk_bytes = self.snapshot_chunk_bytes;
                let snapshot_builder = self.snapshot_builder.clone();
                let inflight = in_flight_snapshots.clone();
                tokio::spawn(async move {
                    if *shutdown_rx.borrow() {
                        return;
                    }
                    // Mark this group as having a snapshot transfer in
                    // flight for the whole task (build + send). Held
                    // until the task ends — including error/shutdown
                    // paths — so compaction cannot advance the snapshot
                    // boundary mid-transfer.
                    let _inflight_guard = inflight.begin(group_id);
                    // Build the real per-group snapshot payload on the
                    // leader before framing the chunked RPC. A `None`
                    // builder (cluster-only tests) or a build failure
                    // falls back to the stub (empty) chunk, which the
                    // sender already handles.
                    let snapshot_bytes: Vec<u8> = match &snapshot_builder {
                        Some(b) => b
                            .build_group_snapshot(group_id, snap_index, snap_term)
                            .await
                            .unwrap_or_else(|e| {
                                warn!(
                                    group_id, peer, error = %e,
                                    "snapshot build failed; sending stub"
                                );
                                Vec::new()
                            }),
                        None => Vec::new(),
                    };
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => {}
                        result = crate::install_snapshot::sender::send_chunked(
                            &transport,
                            crate::install_snapshot::sender::SendChunkedParams {
                                peer,
                                group_id,
                                term,
                                leader_id: node_id,
                                last_included_index: snap_index,
                                last_included_term: snap_term,
                                snapshot_bytes: &snapshot_bytes,
                                chunk_bytes,
                            },
                        ) => {
                            match result {
                                Ok(resp_term) => {
                                    if resp_term > term {
                                        let mut mr =
                                            mr.lock().unwrap_or_else(|p| p.into_inner());
                                        // Higher term — let the tick loop handle step-down.
                                        let _ = mr.handle_append_entries_response(
                                            group_id,
                                            peer,
                                            &nodedb_raft::AppendEntriesResponse {
                                                term: resp_term,
                                                success: false,
                                                last_log_index: 0,
                                            },
                                        );
                                        // Persist the term bump durably.
                                        if let Err(e) =
                                            mr.persist_group_hard_state(group_id)
                                        {
                                            tracing::error!(group_id, peer, error = %e, "persist hard state after snapshot step-down");
                                        }
                                    }
                                    debug!(group_id, peer, "install_snapshot sent");
                                }
                                Err(e) => {
                                    warn!(
                                        group_id, peer, error = %e,
                                        "install_snapshot RPC failed"
                                    );
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}
