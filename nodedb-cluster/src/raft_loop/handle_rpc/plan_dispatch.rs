// SPDX-License-Identifier: BUSL-1.1

//! Physical-plan execution (C-β), metadata/data propose forwarding, and
//! VShardEnvelope routing RPC bodies.

use crate::error::{ClusterError, Result};
use crate::forward::{ChunkSink, PlanExecutor};
use crate::rpc_codec::{
    DataProposeRequest, ExecuteRequest, MetadataProposeRequest, RaftRpc, TypedClusterError,
};

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    // Physical-plan execution (C-β) — execute locally via the PlanExecutor,
    // skipping SQL re-planning entirely.
    pub(super) async fn handle_execute_rpc(&self, req: ExecuteRequest) -> Result<RaftRpc> {
        let resp = self.plan_executor.execute_plan(req).await;
        Ok(RaftRpc::ExecuteResponse(resp))
    }

    // Metadata-group proposal forwarding — apply locally if
    // we're the metadata leader, otherwise return a
    // NotLeader response with a leader hint so the
    // forwarder can chase the redirect.
    pub(super) fn handle_metadata_propose_rpc(
        &self,
        req: MetadataProposeRequest,
    ) -> Result<RaftRpc> {
        let resp = match self.propose_to_metadata_group(req.bytes) {
            Ok(log_index) => crate::rpc_codec::MetadataProposeResponse::ok(log_index),
            Err(crate::error::ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint,
            })) => crate::rpc_codec::MetadataProposeResponse::err("not leader", leader_hint),
            Err(e) => crate::rpc_codec::MetadataProposeResponse::err(e.to_string(), None),
        };
        Ok(RaftRpc::MetadataProposeResponse(resp))
    }

    // Data-group proposal forwarding — apply locally if we are the
    // data-group leader for the given vshard, otherwise return
    // NotLeader with a hint so the forwarder can chase the redirect.
    pub(super) fn handle_data_propose_rpc(&self, req: DataProposeRequest) -> Result<RaftRpc> {
        let resp = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            match mr.propose(req.vshard_id, req.bytes) {
                Ok((group_id, log_index)) => {
                    crate::rpc_codec::DataProposeResponse::ok(group_id, log_index)
                }
                Err(crate::error::ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                    leader_hint,
                })) => crate::rpc_codec::DataProposeResponse::err("not leader", leader_hint),
                Err(e) => crate::rpc_codec::DataProposeResponse::err(e.to_string(), None),
            }
        };
        Ok(RaftRpc::DataProposeResponse(resp))
    }

    // VShardEnvelope — dispatch to registered handler (Event Plane, etc.).
    pub(super) async fn handle_vshard_envelope_rpc(&self, bytes: Vec<u8>) -> Result<RaftRpc> {
        if let Some(ref handler) = self.vshard_handler {
            let response_bytes = handler(bytes).await?;
            Ok(RaftRpc::VShardEnvelope(response_bytes))
        } else {
            Err(ClusterError::Transport {
                detail: "VShardEnvelope handler not configured".into(),
            })
        }
    }

    // Streaming physical-plan execution (L4) — delegate to the PlanExecutor's
    // streaming path. The transport drives the multi-frame chunk/end envelope
    // writes; this just runs the plan and feeds `sink`.
    pub(super) async fn handle_rpc_streaming_impl(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        self.plan_executor.execute_plan_streaming(req, sink).await
    }
}
