// SPDX-License-Identifier: BUSL-1.1

//! The single `impl RaftRpcHandler for RaftLoop` block. Thin dispatch only —
//! each method delegates to a helper defined in a sibling module
//! ([`super::consensus`], [`super::membership`], [`super::plan_dispatch`],
//! [`super::shuffle_calvin`]).

use crate::error::{ClusterError, Result};
use crate::forward::{ChunkSink, PlanExecutor};
use crate::rpc_codec::{
    AssignSurrogateRequest, AssignSurrogateResponse, ExecuteRequest, RaftRpc,
    ReleaseReservationRequest, ReleaseReservationResponse, ReserveReadRequest, ReserveReadResponse,
    ShuffleAggregateConsumeRequest, ShuffleAggregateConsumeResponse, ShuffleConsumeRequest,
    ShuffleConsumeResponse, ShuffleProduceRequest, ShuffleProduceResponse, ShufflePushRequest,
    SubmitCalvinInboxRequest, SubmitCalvinInboxResponse, SubmitCalvinTxnRequest,
    SubmitCalvinTxnResponse, TypedClusterError,
};
use crate::transport::RaftRpcHandler;
use nodedb_raft::message::TimeoutNowRequest;

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftRpcHandler for RaftLoop<A, P> {
    async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
        match rpc {
            // Raft consensus RPCs — lock MultiRaft (sync, never across await).
            RaftRpc::AppendEntriesRequest(req) => self.handle_append_entries_rpc(req),
            RaftRpc::RequestVoteRequest(req) => self.handle_request_vote_rpc(req),
            RaftRpc::InstallSnapshotRequest(req) => self.handle_install_snapshot_rpc(req).await,
            // Cluster join — full orchestration in `super::join`.
            RaftRpc::JoinRequest(req) => Ok(RaftRpc::JoinResponse(self.join_flow(req).await)),
            // Health check.
            RaftRpc::Ping(req) => self.handle_ping_rpc(req),
            // Topology broadcast.
            RaftRpc::TopologyUpdate(update) => self.handle_topology_update_rpc(update),
            // Physical-plan execution (C-β) — execute locally via the PlanExecutor,
            // skipping SQL re-planning entirely.
            RaftRpc::ExecuteRequest(req) => self.handle_execute_rpc(req).await,
            // Metadata-group proposal forwarding.
            RaftRpc::MetadataProposeRequest(req) => self.handle_metadata_propose_rpc(req),
            // Data-group proposal forwarding.
            RaftRpc::DataProposeRequest(req) => self.handle_data_propose_rpc(req),
            // VShardEnvelope — dispatch to registered handler (Event Plane, etc.).
            RaftRpc::VShardEnvelope(bytes) => self.handle_vshard_envelope_rpc(bytes).await,
            other => Err(ClusterError::Transport {
                detail: format!("unexpected request type in RPC handler: {other:?}"),
            }),
        }
    }

    // Streaming physical-plan execution (L4) — delegate to the PlanExecutor's
    // streaming path. The transport drives the multi-frame chunk/end envelope
    // writes; this just runs the plan and feeds `sink`.
    async fn handle_rpc_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        self.handle_rpc_streaming_impl(req, sink).await
    }

    async fn on_shuffle_request(&self, req: ShufflePushRequest) {
        self.on_shuffle_request_impl(req).await
    }

    async fn on_shuffle_chunk(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> Result<()> {
        self.on_shuffle_chunk_impl(shuffle_id, part, side, payload)
            .await
    }

    async fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<TypedClusterError>,
    ) {
        self.on_shuffle_end_impl(shuffle_id, part, side, error)
            .await
    }

    async fn on_shuffle_produce(&self, req: ShuffleProduceRequest) -> ShuffleProduceResponse {
        self.on_shuffle_produce_impl(req).await
    }

    async fn on_shuffle_consume(&self, req: ShuffleConsumeRequest) -> ShuffleConsumeResponse {
        self.on_shuffle_consume_impl(req).await
    }

    async fn on_shuffle_aggregate(
        &self,
        req: ShuffleAggregateConsumeRequest,
    ) -> ShuffleAggregateConsumeResponse {
        self.on_shuffle_aggregate_impl(req).await
    }

    async fn on_assign_surrogate(&self, req: AssignSurrogateRequest) -> AssignSurrogateResponse {
        self.on_assign_surrogate_impl(req).await
    }

    async fn on_submit_calvin_txn(&self, req: SubmitCalvinTxnRequest) -> SubmitCalvinTxnResponse {
        self.on_submit_calvin_txn_impl(req).await
    }

    async fn on_submit_calvin_inbox(
        &self,
        req: SubmitCalvinInboxRequest,
    ) -> SubmitCalvinInboxResponse {
        self.on_submit_calvin_inbox_impl(req).await
    }

    async fn on_reserve_read(&self, req: ReserveReadRequest) -> ReserveReadResponse {
        self.on_reserve_read_impl(req).await
    }

    async fn on_release_reservation(
        &self,
        req: ReleaseReservationRequest,
    ) -> ReleaseReservationResponse {
        self.on_release_reservation_impl(req).await
    }

    async fn on_timeout_now(&self, req: TimeoutNowRequest) {
        self.on_timeout_now_impl(req).await
    }
}
