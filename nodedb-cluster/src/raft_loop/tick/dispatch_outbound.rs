// SPDX-License-Identifier: BUSL-1.1

//! Batch and dispatch outbound Raft messages produced by a tick's `Ready`
//! output: AppendEntries, RequestVote, and TimeoutNow. One background task
//! per destination peer; batches all messages targeting the same peer
//! across every group.

use std::collections::HashMap as BatchMap;

use tracing::{debug, error, warn};

use nodedb_raft::transport::RaftTransport;

use crate::forward::PlanExecutor;

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Batch and dispatch AppendEntries / RequestVote / TimeoutNow messages
    /// from this tick's `Ready` groups. Called only when `!ready.is_empty()`.
    pub(super) fn dispatch_outbound_messages(&self, groups: &[(u64, nodedb_raft::Ready)]) {
        let mut ae_batches: BatchMap<u64, Vec<(u64, nodedb_raft::AppendEntriesRequest)>> =
            BatchMap::new();
        let mut vote_batches: BatchMap<u64, Vec<(u64, nodedb_raft::RequestVoteRequest)>> =
            BatchMap::new();
        let mut timeout_now_msgs: Vec<(u64, nodedb_raft::TimeoutNowRequest)> = Vec::new();

        for (group_id, group_ready) in groups {
            for (peer, req) in &group_ready.messages {
                ae_batches
                    .entry(*peer)
                    .or_default()
                    .push((*group_id, req.clone()));
            }
            for (peer, req) in &group_ready.vote_requests {
                vote_batches
                    .entry(*peer)
                    .or_default()
                    .push((*group_id, req.clone()));
            }
            for (dest, req) in &group_ready.timeout_now {
                timeout_now_msgs.push((*dest, req.clone()));
            }
        }

        // Dispatch batched AppendEntries — one task per peer.
        //
        // Each detached task subscribes to the shutdown watch
        // and wraps its RPC awaits in `tokio::select!` so a
        // `RaftLoop::begin_shutdown` signal (or the `run` loop
        // propagating an external shutdown) cancels the
        // in-flight QUIC call at the next await point. This
        // is what lets graceful shutdown drop the
        // `Arc<Mutex<MultiRaft>>` clone promptly and release
        // per-group redb locks for an in-process restart.
        for (peer, messages) in ae_batches {
            let transport = self.transport.clone();
            let mr = self.multi_raft.clone();
            let mut shutdown_rx = self.shutdown_watch.subscribe();
            tokio::spawn(async move {
                if *shutdown_rx.borrow() {
                    return;
                }
                for (group_id, req) in messages {
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => return,
                        rpc = transport.append_entries(peer, req) => {
                            match rpc {
                                Ok(resp) => {
                                    let mut mr =
                                        mr.lock().unwrap_or_else(|p| p.into_inner());
                                    if let Err(e) = mr
                                        .handle_append_entries_response(group_id, peer, &resp)
                                    {
                                        debug!(group_id, peer, error = %e, "handle ae response");
                                    }
                                    // A response can bump the term (step
                                    // down to follower); persist it durably.
                                    if let Err(e) = mr.persist_group_hard_state(group_id) {
                                        error!(group_id, peer, error = %e, "persist hard state after ae response");
                                    }
                                }
                                Err(e) => {
                                    warn!(group_id, peer, error = %e, "append_entries RPC failed");
                                    break; // Peer is down — skip remaining groups.
                                }
                            }
                        }
                    }
                }
            });
        }

        // Dispatch batched RequestVote — one task per peer.
        for (peer, votes) in vote_batches {
            let transport = self.transport.clone();
            let mr = self.multi_raft.clone();
            let mut shutdown_rx = self.shutdown_watch.subscribe();
            tokio::spawn(async move {
                if *shutdown_rx.borrow() {
                    return;
                }
                for (group_id, req) in votes {
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => return,
                        rpc = transport.request_vote(peer, req) => {
                            match rpc {
                                Ok(resp) => {
                                    let mut mr =
                                        mr.lock().unwrap_or_else(|p| p.into_inner());
                                    if let Err(e) = mr
                                        .handle_request_vote_response(group_id, peer, &resp)
                                    {
                                        debug!(group_id, peer, error = %e, "handle vote response");
                                    }
                                    // A higher-term response steps this
                                    // candidate down to follower; persist
                                    // that term bump durably.
                                    if let Err(e) = mr.persist_group_hard_state(group_id) {
                                        error!(group_id, peer, error = %e, "persist hard state after vote response");
                                    }
                                }
                                Err(e) => {
                                    warn!(group_id, peer, error = %e, "request_vote RPC failed");
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        }

        // Dispatch TimeoutNow — one task per message (one-way, no response).
        for (dest, req) in timeout_now_msgs {
            let transport = self.transport.clone();
            let mut shutdown_rx = self.shutdown_watch.subscribe();
            tokio::spawn(async move {
                if *shutdown_rx.borrow() {
                    return;
                }
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => {}
                    result = transport.timeout_now(dest, req) => {
                        if let Err(e) = result {
                            warn!(dest, error = %e, "timeout_now RPC failed");
                        }
                    }
                }
            });
        }
    }
}
