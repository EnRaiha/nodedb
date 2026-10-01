// SPDX-License-Identifier: BUSL-1.1

//! Install-snapshot dispatch for peers that have fallen behind the leader's
//! snapshot boundary (`group_ready.snapshots_needed`).
//!
//! A data group's build is asked for the log's snapshot boundary. The host
//! captures a cut at or above it and names the cut, and the snapshot ships
//! labelled with the cut and the term of its entry. Metadata
//! group 0 and the Calvin sequencer group ship their state machine at the
//! applied index instead: the host captures it on the tick thread, between
//! apply batches. Group 0's capture is serialized off the tick.

use std::sync::{Arc, Mutex};

use tracing::{debug, warn};

use crate::calvin::SEQUENCER_GROUP_ID;
use crate::forward::PlanExecutor;
use crate::metadata_group::METADATA_GROUP_ID;
use crate::multi_raft::MultiRaft;
use crate::raft_loop::MetadataSnapshotCapture;
use crate::transport::NexarTransport;

use super::super::loop_core::{CommitApplier, RaftLoop};

/// A state machine capture taken on the tick thread.
enum AppliedCapture {
    /// Group 0's capture, serialized off the tick.
    Metadata(Box<dyn MetadataSnapshotCapture>),
    /// A payload encoded at capture.
    Encoded(Vec<u8>),
}

/// Everything one `InstallSnapshot` transfer to one peer needs.
struct SnapshotSend {
    transport: Arc<NexarTransport>,
    multi_raft: Arc<Mutex<MultiRaft>>,
    peer: u64,
    group_id: u64,
    term: u64,
    leader_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    chunk_bytes: u64,
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Dispatch `InstallSnapshot` RPCs for every peer this group's `Ready`
    /// output flagged as needing one. Called only when
    /// `!group_ready.snapshots_needed.is_empty()`.
    pub(super) fn dispatch_group_snapshots(&self, group_id: u64, snapshots_needed: Vec<u64>) {
        if (group_id == METADATA_GROUP_ID || group_id == SEQUENCER_GROUP_ID)
            && self.snapshot_builder.is_some()
        {
            self.dispatch_applied_snapshots(group_id, snapshots_needed);
            return;
        }
        let (snapshot_meta, in_flight_snapshots) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            (
                mr.snapshot_metadata(group_id).ok(),
                mr.in_flight_snapshots(),
            )
        };
        let Some((term, snap_index, snap_term)) = snapshot_meta else {
            return;
        };
        // A lagging peer is flagged on every heartbeat until its transfer
        // lands. One transfer per group runs at a time; the next heartbeat
        // after it ends flags any peer still behind.
        if in_flight_snapshots.is_active(group_id) {
            return;
        }
        for peer in snapshots_needed {
            let mut send = self.snapshot_send(peer, group_id, term, snap_index, snap_term);
            let mut shutdown_rx = self.shutdown_watch.subscribe();
            let snapshot_builder = self.snapshot_builder.clone();
            // Taken here, before the spawn, so the next tick sees the
            // transfer. Held for the whole task (build + send), including
            // the error and shutdown paths. Every compaction path
            // (`MultiRaft::maybe_compact_group`) defers while it is held, so
            // the log keeps every entry above the boundary, the cut's term
            // included, until the transfer ends.
            let inflight_guard = in_flight_snapshots.begin(group_id);
            tokio::spawn(async move {
                let _inflight_guard = inflight_guard;
                if *shutdown_rx.borrow() {
                    return;
                }
                // A `None` builder (cluster-only tests) sends the stub
                // (empty) chunk at the log boundary. A failed build sends
                // nothing: an empty chunk would move the peer's boundary
                // without its state. The next tick flags the peer again.
                let snapshot = match &snapshot_builder {
                    Some(b) => match b
                        .build_group_snapshot(group_id, snap_index, snap_term)
                        .await
                    {
                        Ok(snapshot) => snapshot,
                        Err(e) => {
                            warn!(group_id, peer, error = %e, "snapshot build failed; nothing sent");
                            return;
                        }
                    },
                    None => crate::raft_loop::BuiltGroupSnapshot {
                        bytes: Vec::new(),
                        cut_index: snap_index,
                    },
                };
                // The snapshot is labelled with its cut: the state it holds
                // ends there, so the peer resumes the log right after it.
                let Some(cut_term) = cut_term(&send.multi_raft, group_id, snap_index, &snapshot)
                else {
                    return;
                };
                send.last_included_index = snapshot.cut_index;
                send.last_included_term = cut_term;
                send_snapshot(send, &snapshot.bytes, &mut shutdown_rx).await;
            });
        }
    }

    /// Capture `group_id`'s state machine at its applied index and ship it to
    /// every peer in `peers`. Metadata group 0 and the Calvin sequencer group
    /// hold their state in a state machine the tick thread applies, so the
    /// capture holds exactly the entries through the applied index.
    fn dispatch_applied_snapshots(&self, group_id: u64, peers: Vec<u64>) {
        let Some(builder) = self.snapshot_builder.clone() else {
            return;
        };
        let (term, applied, applied_term, inflight) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let Ok((term, _, _)) = mr.snapshot_metadata(group_id) else {
                return;
            };
            let applied = mr.last_applied(group_id).unwrap_or(0);
            (
                term,
                applied,
                mr.log_term_at(group_id, applied),
                mr.in_flight_snapshots(),
            )
        };
        // One capture at a time: a peer is flagged on every heartbeat until
        // its transfer lands.
        if inflight.is_active(group_id) {
            return;
        }
        let Some(applied_term) = applied_term else {
            warn!(
                group_id,
                applied,
                "state machine snapshot: the log no longer holds the applied entry's term; \
                 nothing sent"
            );
            return;
        };
        // Taken before the capture so no compaction passes the captured
        // index while the image is serialized and sent.
        let inflight_guard = inflight.begin(group_id);
        let capture = if group_id == SEQUENCER_GROUP_ID {
            builder
                .capture_sequencer(applied)
                .map(AppliedCapture::Encoded)
        } else {
            builder
                .capture_metadata(applied, applied_term)
                .map(AppliedCapture::Metadata)
        };
        let capture = match capture {
            Ok(capture) => capture,
            Err(e) => {
                warn!(group_id, applied, error = %e, "state machine snapshot capture failed; nothing sent");
                return;
            }
        };
        let sends: Vec<SnapshotSend> = peers
            .into_iter()
            .map(|peer| self.snapshot_send(peer, group_id, term, applied, applied_term))
            .collect();
        let mut shutdown_rx = self.shutdown_watch.subscribe();
        tokio::spawn(async move {
            let _inflight_guard = inflight_guard;
            let bytes = match capture {
                AppliedCapture::Encoded(bytes) => bytes,
                AppliedCapture::Metadata(capture) => {
                    match tokio::task::spawn_blocking(move || capture.serialize()).await {
                        Ok(Ok(bytes)) => bytes,
                        Ok(Err(e)) => {
                            warn!(applied, error = %e, "group 0 snapshot serialize failed; nothing sent");
                            return;
                        }
                        Err(e) => {
                            warn!(applied, error = %e, "group 0 snapshot serialize task failed");
                            return;
                        }
                    }
                }
            };
            for send in sends {
                if *shutdown_rx.borrow() {
                    return;
                }
                send_snapshot(send, &bytes, &mut shutdown_rx).await;
            }
        });
    }

    fn snapshot_send(
        &self,
        peer: u64,
        group_id: u64,
        term: u64,
        last_included_index: u64,
        last_included_term: u64,
    ) -> SnapshotSend {
        SnapshotSend {
            transport: self.transport.clone(),
            multi_raft: self.multi_raft.clone(),
            peer,
            group_id,
            term,
            leader_id: self.node_id,
            last_included_index,
            last_included_term,
            chunk_bytes: self.snapshot_chunk_bytes,
        }
    }
}

/// The term of the entry at `snapshot`'s cut, from the leader's log. `None`,
/// with a warning, when the cut is below the log boundary `snap_index` or the
/// log no longer holds its term: nothing is sent, and the next tick flags the
/// peer again.
fn cut_term(
    multi_raft: &Mutex<MultiRaft>,
    group_id: u64,
    snap_index: u64,
    snapshot: &crate::raft_loop::BuiltGroupSnapshot,
) -> Option<u64> {
    let cut = snapshot.cut_index;
    if cut < snap_index {
        warn!(
            group_id,
            cut, snap_index, "snapshot build: the cut is below the log boundary; nothing sent"
        );
        return None;
    }
    let term = multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .log_term_at(group_id, cut);
    if term.is_none() {
        warn!(
            group_id,
            cut, "snapshot build: the log no longer holds the cut's term; nothing sent"
        );
    }
    term
}

/// Send `snapshot_bytes` to one peer in chunks, stepping down on a higher
/// term in the reply.
async fn send_snapshot(
    send: SnapshotSend,
    snapshot_bytes: &[u8],
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) {
    let SnapshotSend {
        transport,
        multi_raft,
        peer,
        group_id,
        term,
        leader_id,
        last_included_index,
        last_included_term,
        chunk_bytes,
    } = send;
    // The membership holds every conf change applied through the snapshot
    // index. A conf change applied since sits above that index, and the
    // peer applies it from the log after the install.
    let (voters, learners) = multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .group_membership(group_id)
        .map(|m| (m.voters, m.learners))
        .unwrap_or_default();
    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => {}
        result = crate::install_snapshot::sender::send_chunked(
            &transport,
            crate::install_snapshot::sender::SendChunkedParams {
                peer,
                group_id,
                term,
                leader_id,
                last_included_index,
                last_included_term,
                snapshot_bytes,
                chunk_bytes,
                voters: &voters,
                learners: &learners,
            },
        ) => {
            match result {
                Ok(resp_term) if resp_term <= term => {
                    // The peer holds the state through the snapshot index, so
                    // replication to it resumes after that index.
                    multi_raft
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .record_snapshot_installed(group_id, peer, last_included_index);
                    debug!(group_id, peer, "install_snapshot sent");
                }
                Ok(resp_term) => {
                    {
                        let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                        // Higher term: the tick loop handles the step-down.
                        if let Err(e) = mr.handle_append_entries_response(
                            group_id,
                            peer,
                            &nodedb_raft::AppendEntriesResponse {
                                term: resp_term,
                                success: false,
                                last_log_index: 0,
                                round: nodedb_raft::node::leader_lease::UNTRACKED_ROUND,
                                needs_snapshot: false,
                            },
                        ) {
                            tracing::error!(group_id, peer, resp_term, error = %e, "apply higher term from install_snapshot response");
                        }
                        if let Err(e) = mr.persist_group_hard_state(group_id) {
                            tracing::error!(group_id, peer, error = %e, "persist hard state after snapshot step-down");
                        }
                    }
                    debug!(group_id, peer, resp_term, "install_snapshot answered with a higher term");
                }
                Err(e) => {
                    warn!(group_id, peer, error = %e, "install_snapshot RPC failed");
                }
            }
        }
    }
}
