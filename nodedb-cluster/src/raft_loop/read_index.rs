// SPDX-License-Identifier: BUSL-1.1

//! Read index for any node: confirmed locally on the leader, asked of the
//! leader over the transport on every other node.
//!
//! A read index is the leader's commit index at a moment a quorum confirmed
//! its leadership. A node whose state machine has applied a group through
//! that index observes every entry committed before the read index was taken.
//!
//! The leader status a node answers for another node's routing hint is also
//! here. It reads this node's own Raft state and runs no quorum round.

use std::time::Duration;

use crate::error::{ClusterError, Result};
use crate::forward::PlanExecutor;
use crate::read_index_wait::confirm_read_index;
use crate::rpc_codec::{
    LeaderMembership, LeaderStatusResponse, RaftRpc, ReadIndexOutcome, ReadIndexRequest,
    ReadIndexResponse,
};

use super::loop_core::{CommitApplier, RaftLoop};

/// Upper bound on the quorum wait a remote node may ask for.
const MAX_REMOTE_TIMEOUT: Duration = Duration::from_secs(10);

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Obtain a read index for `group_id`.
    ///
    /// On the group leader, confirms leadership against a quorum. On any other
    /// node, asks the known leader: the leader this node's Raft knows when it
    /// hosts a replica of the group, else the leader its routing table names.
    /// A refusal that names a leader at a newer term moves the routing hint
    /// to that leader, and so does a confirmation from the asked leader.
    /// Fails with [`ClusterError::ReadIndexNotLeader`] when
    /// no leader is known or the asked node no longer leads, and with
    /// [`ClusterError::ReadIndexTimeout`] when no quorum answered within
    /// `timeout`.
    pub async fn read_index_via_leader(&self, group_id: u64, timeout: Duration) -> Result<u64> {
        let leader = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if mr.is_group_leader(group_id) {
                None
            } else if mr.contains_group(group_id) {
                Some(mr.group_leader(group_id))
            } else {
                let routing = mr.routing();
                let table = routing.read().unwrap_or_else(|p| p.into_inner());
                Some(table.group_info(group_id).map_or(0, |info| info.leader))
            }
        };
        let leader_id = match leader {
            None => return confirm_read_index(&self.multi_raft, group_id, timeout).await,
            Some(id) if id == 0 || id == self.node_id => {
                return Err(ClusterError::ReadIndexNotLeader { group_id });
            }
            Some(id) => id,
        };
        self.register_peer_addr(leader_id)?;
        let request = RaftRpc::ReadIndexRequest(ReadIndexRequest {
            group_id,
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        });
        match self.transport.send_rpc(leader_id, request).await? {
            RaftRpc::ReadIndexResponse(ReadIndexResponse { outcome }) => match outcome {
                ReadIndexOutcome::Confirmed { read_index, term } => {
                    self.observe_redirect(group_id, Some(leader_id), term);
                    Ok(read_index)
                }
                ReadIndexOutcome::NotLeader { leader_hint, term } => {
                    self.observe_redirect(group_id, leader_hint, term);
                    Err(ClusterError::ReadIndexNotLeader { group_id })
                }
                ReadIndexOutcome::Timeout { waited_ms } => Err(ClusterError::ReadIndexTimeout {
                    group_id,
                    waited_ms,
                }),
            },
            other => Err(ClusterError::Transport {
                detail: format!("read index: unexpected response variant {other:?}"),
            }),
        }
    }

    /// Answer a remote node's [`ReadIndexRequest`] for a group this node may
    /// lead.
    pub(super) async fn handle_read_index_rpc(&self, req: ReadIndexRequest) -> Result<RaftRpc> {
        let timeout = Duration::from_millis(req.timeout_ms).min(MAX_REMOTE_TIMEOUT);
        let outcome = match confirm_read_index(&self.multi_raft, req.group_id, timeout).await {
            Ok(read_index) => {
                // The term this node leads at. A node that stepped down since
                // the quorum confirmed it reports term 0: it names no term.
                let (leader, term) = self
                    .multi_raft
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .group_leader_at_term(req.group_id);
                let term = if leader == self.node_id { term } else { 0 };
                ReadIndexOutcome::Confirmed { read_index, term }
            }
            Err(ClusterError::ReadIndexTimeout { waited_ms, .. }) => {
                ReadIndexOutcome::Timeout { waited_ms }
            }
            Err(_) => {
                let (hint, term) = self
                    .multi_raft
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .group_leader_at_term(req.group_id);
                ReadIndexOutcome::NotLeader {
                    leader_hint: (hint != 0).then_some(hint),
                    term,
                }
            }
        };
        Ok(RaftRpc::ReadIndexResponse(ReadIndexResponse { outcome }))
    }

    /// The leader this node knows for `group_id` and the term it leads,
    /// from this node's own Raft state with no quorum round. `(0, 0)` when
    /// this node knows no leader or hosts no replica of the group. A node
    /// that leads the group also answers its Raft voters and learners.
    ///
    /// Raft clears a node's leader whenever its term moves, so a named
    /// leader always leads the reported term.
    pub(super) fn leader_status(&self, group_id: u64) -> LeaderStatusResponse {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let (leader, term) = mr.group_leader_at_term(group_id);
        if leader == 0 {
            return LeaderStatusResponse {
                leader: 0,
                term: 0,
                membership: None,
            };
        }
        let membership = if leader == self.node_id {
            mr.group_membership(group_id).map(|m| LeaderMembership {
                voters: m.voters,
                learners: m.learners,
            })
        } else {
            None
        };
        LeaderStatusResponse {
            leader,
            term,
            membership,
        }
    }

    /// Register `node_id`'s listen address with the transport, from the local
    /// topology.
    fn register_peer_addr(&self, node_id: u64) -> Result<()> {
        let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
        let node = topo
            .get_node(node_id)
            .ok_or_else(|| ClusterError::Transport {
                detail: format!("read index: leader {node_id} not in local topology"),
            })?;
        let addr = node.socket_addr().ok_or_else(|| ClusterError::Transport {
            detail: format!(
                "read index: leader {node_id} has unparseable addr {:?}",
                node.addr
            ),
        })?;
        self.transport.register_peer(node_id, addr);
        Ok(())
    }
}
