// SPDX-License-Identifier: BUSL-1.1

//! `NexarArrayDispatch` bridges `ShardRpcDispatch` ↔ `NexarTransport`,
//! tunnelling `VShardEnvelope` bytes through `RaftRpc::VShardEnvelope`.
//!
//! When the target shard is owned by the local node, `NexarArrayDispatch`
//! short-circuits the QUIC transport and dispatches directly to the local
//! Data Plane via `handle_array_shard_rpc`. This avoids the `NodeUnreachable`
//! error that arises when a node tries to `send_rpc` to itself (the transport
//! peer table does not contain an entry for the local node's own ID).

use std::sync::Arc;

use async_trait::async_trait;
use nodedb_cluster::distributed_array::ArrayLocalExecutor;
use nodedb_cluster::distributed_array::handler::handle_array_shard_rpc;
use nodedb_cluster::distributed_array::rpc::ShardRpcDispatch;
use nodedb_cluster::error::{ClusterError, Result as ClusterResult};
use nodedb_cluster::rpc_codec::{RaftRpc, remaining_budget_ms};
use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType};
use nodedb_cluster::{NexarTransport, RoutingTable};

use crate::control::cluster::array_cluster_helpers::array_resp_msg_type;
use crate::control::cluster::array_executor::DataPlaneArrayExecutor;
use crate::control::state::SharedState;

/// Implements `ShardRpcDispatch` by either short-circuiting to the local Data
/// Plane (when the target shard leader is this node) or tunnelling through
/// `NexarTransport::send_rpc` via `RaftRpc::VShardEnvelope`.
///
/// The short-circuit path avoids the `NodeUnreachable` error that arises when a
/// coordinator tries to `send_rpc` to itself — the QUIC transport peer table
/// never contains an entry for the local node ID.
pub struct NexarArrayDispatch {
    transport: Arc<NexarTransport>,
    routing: Arc<std::sync::RwLock<RoutingTable>>,
    /// This node's own ID, used to detect local-shard requests.
    own_node_id: u64,
    /// Local Data Plane executor — used when the target shard is owned by
    /// this node, bypassing the QUIC transport entirely.
    local_executor: Arc<dyn ArrayLocalExecutor>,
}

impl NexarArrayDispatch {
    pub fn new(
        transport: Arc<NexarTransport>,
        routing: Arc<std::sync::RwLock<RoutingTable>>,
        own_node_id: u64,
        state: Arc<SharedState>,
    ) -> Self {
        let local_executor =
            Arc::new(DataPlaneArrayExecutor::new(state)) as Arc<dyn ArrayLocalExecutor>;
        Self {
            transport,
            routing,
            own_node_id,
            local_executor,
        }
    }
}

/// The sends one shard call makes at most: the first, and one resend.
const CALL_ATTEMPTS: usize = 2;

/// The pause before a resend, so the metadata applier can heal a routing
/// entry that named a stale or fake leader.
const RESEND_PAUSE: std::time::Duration = std::time::Duration::from_millis(50);

#[async_trait]
impl ShardRpcDispatch for NexarArrayDispatch {
    /// Every send gets what is left of the `timeout_ms` budget. A live shard
    /// can use all of it. A dead shard fails the send at the QUIC idle
    /// timeout, and the resend reaches the shard's current leader.
    async fn call(&self, req: VShardEnvelope, timeout_ms: u64) -> ClusterResult<VShardEnvelope> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        let resend_safe = is_resend_safe(req.msg_type);
        let mut attempts_left = CALL_ATTEMPTS;
        loop {
            match self.call_once(&req, remaining_budget_ms(deadline)).await {
                Ok(resp) => return Ok(resp),
                // A resend starts only while the budget has time left.
                Err(e)
                    if attempts_left > 1
                        && remaining_budget_ms(deadline) > 0
                        && may_resend(&e, resend_safe) =>
                {
                    // `call_once` re-reads routing, so the resend reaches the
                    // shard's current leader.
                    attempts_left -= 1;
                    tokio::time::sleep(RESEND_PAUSE).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Whether a second run of the request leaves the same state and answer. A
/// slice, an aggregate, an elementwise op and a surrogate bitmap only read
/// the shard. A second flush or compaction finds nothing left to do. A put
/// or a delete can have applied when its answer was lost.
fn is_resend_safe(msg_type: VShardMessageType) -> bool {
    matches!(
        msg_type,
        VShardMessageType::ArrayShardSliceReq
            | VShardMessageType::ArrayShardAggReq
            | VShardMessageType::ArrayShardElementwiseReq
            | VShardMessageType::ArrayShardSurrogateBitmapReq
            | VShardMessageType::ArrayShardMaintReq
    )
}

/// Whether a failed send can go again. A request that never reached the
/// shard always can. A lost answer can only for a resend-safe request:
/// any other one reports its unknown outcome.
fn may_resend(e: &ClusterError, resend_safe: bool) -> bool {
    e.is_unsent() || (resend_safe && e.is_lost_answer())
}

impl NexarArrayDispatch {
    async fn call_once(
        &self,
        req: &VShardEnvelope,
        timeout_ms: u64,
    ) -> ClusterResult<VShardEnvelope> {
        // Resolve the shard's leader node from the routing table.
        let node_id = {
            let rt = self.routing.read().map_err(|_| {
                nodedb_cluster::error::ClusterError::Transport {
                    detail: "routing table lock poisoned".into(),
                }
            })?;
            rt.leader_for_vshard(req.vshard_id)?
        };

        // Short-circuit: if the target shard is owned by this node, dispatch
        // directly to the local Data Plane instead of looping through QUIC.
        if node_id == self.own_node_id {
            let req_opcode = req.msg_type as u32;
            let resp_opcode = req_opcode + 1;
            let resp_msg_type = array_resp_msg_type(resp_opcode).ok_or_else(|| {
                nodedb_cluster::error::ClusterError::Codec {
                    detail: format!("local dispatch: unknown response opcode {resp_opcode}"),
                }
            })?;

            let resp_payload = handle_array_shard_rpc(
                req_opcode,
                req.vshard_id,
                &req.payload,
                &self.local_executor,
            )
            .await?;

            return Ok(VShardEnvelope::new(
                resp_msg_type,
                self.own_node_id,
                req.source_node,
                req.vshard_id,
                resp_payload,
            ));
        }

        // Remote path: encode the VShardEnvelope to bytes for the RaftRpc tunnel.
        let envelope_bytes = req.to_bytes();

        // Wrap in RaftRpc and send. The remote node's handler decodes the
        // envelope bytes and dispatches to the appropriate shard handler.
        let rpc = RaftRpc::VShardEnvelope(envelope_bytes);
        // The call's remaining budget bounds the reply wait, never the
        // transport's fixed RPC timeout.
        let reply_wait = std::time::Duration::from_millis(timeout_ms);
        let resp_rpc = tokio::time::timeout(
            reply_wait,
            self.transport
                .send_rpc_with_read_timeout(node_id, rpc, reply_wait),
        )
        .await
        // The request can have reached the shard. The budget is spent, so
        // `call` starts no resend.
        .map_err(|_| nodedb_cluster::error::ClusterError::ShardTimeout {
            vshard_id: req.vshard_id,
            elapsed_ms: timeout_ms,
        })??;

        // The shard answered, so an answer this node cannot read is a codec
        // error: a resend can run a put or a delete twice.
        match resp_rpc {
            RaftRpc::VShardEnvelope(bytes) => VShardEnvelope::from_bytes(&bytes).ok_or_else(|| {
                nodedb_cluster::error::ClusterError::Codec {
                    detail: "array shard response: failed to decode VShardEnvelope".into(),
                }
            }),
            // The shard's handler failed with a typed error. It is rebuilt as
            // the error the local short-circuit returns, so `WrongOwner`
            // reaches the fan-out's reroute retry.
            RaftRpc::VShardRefusal(refusal) => {
                Err(nodedb_cluster::error::ClusterError::from(refusal.error))
            }
            other => Err(nodedb_cluster::error::ClusterError::Codec {
                detail: format!(
                    "array shard RPC: unexpected response type {:?}",
                    std::mem::discriminant(&other)
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timed_out() -> ClusterError {
        ClusterError::ShardTimeout {
            vshard_id: 3,
            elapsed_ms: 1_000,
        }
    }

    /// A slice, an aggregate, an elementwise op and a surrogate bitmap only
    /// read the shard. A repeated flush or compaction finds nothing to do.
    #[test]
    fn only_array_reads_and_maintenance_are_resend_safe() {
        assert!(is_resend_safe(VShardMessageType::ArrayShardMaintReq));
        assert!(is_resend_safe(VShardMessageType::ArrayShardSliceReq));
        assert!(is_resend_safe(VShardMessageType::ArrayShardAggReq));
        assert!(is_resend_safe(VShardMessageType::ArrayShardElementwiseReq));
        assert!(is_resend_safe(
            VShardMessageType::ArrayShardSurrogateBitmapReq
        ));
        assert!(!is_resend_safe(VShardMessageType::ArrayShardPutReq));
        assert!(!is_resend_safe(VShardMessageType::ArrayShardDeleteReq));
    }

    /// A read whose answer was lost goes again.
    #[test]
    fn a_timed_out_read_resends() {
        assert!(may_resend(&timed_out(), true));
        let unanswered = ClusterError::Unanswered {
            node_id: 2,
            detail: "stream reset".into(),
        };
        assert!(may_resend(&unanswered, true));
    }

    /// A put or a delete whose answer was lost reports its unknown outcome.
    #[test]
    fn a_timed_out_write_is_never_resent() {
        assert!(!may_resend(&timed_out(), false));
    }

    /// A request that never reached the shard goes again, read or write.
    #[test]
    fn an_unsent_request_resends() {
        let unsent = ClusterError::Transport {
            detail: "connect refused".into(),
        };
        assert!(may_resend(&unsent, false));
        assert!(may_resend(&unsent, true));
    }

    /// An answer the shard gave never goes again.
    #[test]
    fn an_answer_is_never_resent() {
        let unreadable = ClusterError::Codec {
            detail: "array shard response: failed to decode VShardEnvelope".into(),
        };
        assert!(!may_resend(&unreadable, true));
    }
}
