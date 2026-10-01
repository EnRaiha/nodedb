// SPDX-License-Identifier: BUSL-1.1

//! RequestVote dispatch for one group, gated on the group's disk.
//!
//! A candidate's term bump and self-vote are staged on the group's disk by
//! the tick. Its vote requests leave only once those writes are durable. The
//! wait runs on the group's own task, without the `MultiRaft` lock, so no
//! other group's heartbeats or elections wait on this group's disk.

use nodedb_raft::transport::RaftTransport;
use tracing::{debug, error, warn};

use crate::forward::PlanExecutor;
use crate::group_disk::DurabilityTicket;

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Send `group_id`'s vote requests, one task per peer, once `ticket` is
    /// durable. A ticket that fails, because the group was unmounted, sends
    /// nothing.
    pub(super) fn dispatch_group_votes(
        &self,
        group_id: u64,
        ticket: Option<DurabilityTicket>,
        votes: Vec<(u64, nodedb_raft::RequestVoteRequest)>,
    ) {
        let transport = self.transport.clone();
        let multi_raft = self.multi_raft.clone();
        let mut shutdown_rx = self.shutdown_watch.subscribe();
        // One shutdown receiver per request, taken here: the watch sender
        // stays with the loop.
        let votes: Vec<_> = votes
            .into_iter()
            .map(|vote| (vote, self.shutdown_watch.subscribe()))
            .collect();
        tokio::spawn(async move {
            if let Some(ticket) = ticket {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => return,
                    durable = ticket.durable() => {
                        if let Err(e) = durable {
                            warn!(group_id, error = %e, "vote requests not sent: the term is not durable");
                            return;
                        }
                    }
                }
            }
            for ((peer, req), mut shutdown_rx) in votes {
                let transport = transport.clone();
                let multi_raft = multi_raft.clone();
                tokio::spawn(async move {
                    if *shutdown_rx.borrow() {
                        return;
                    }
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => {}
                        rpc = transport.request_vote(peer, req) => match rpc {
                            Ok(resp) => {
                                let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                                if let Err(e) = mr.handle_request_vote_response(group_id, peer, &resp) {
                                    debug!(group_id, peer, error = %e, "handle vote response");
                                }
                                // A higher-term response steps this candidate
                                // down to follower. The term bump is staged,
                                // and no message depends on it.
                                if let Err(e) = mr.persist_group_hard_state(group_id) {
                                    error!(group_id, peer, error = %e, "stage hard state after vote response");
                                }
                            }
                            Err(e) => warn!(group_id, peer, error = %e, "request_vote RPC failed"),
                        },
                    }
                });
            }
        });
    }
}
