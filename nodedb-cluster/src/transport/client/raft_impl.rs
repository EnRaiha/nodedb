// SPDX-License-Identifier: BUSL-1.1

//! `nodedb_raft::RaftTransport` trait impl — dispatches Raft RPCs through the
//! outbound [`send_rpc`] path and unpacks the typed response.
//!
//! [`send_rpc`]: super::transport::NexarTransport::send_rpc

use nodedb_raft::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest,
};
use nodedb_raft::transport::RaftTransport;

use crate::error::ClusterError;
use crate::rpc_codec::RaftRpc;

use super::transport::NexarTransport;

fn to_raft_err(e: ClusterError) -> nodedb_raft::RaftError {
    nodedb_raft::RaftError::Transport {
        detail: e.to_string(),
    }
}

impl RaftTransport for NexarTransport {
    async fn append_entries(
        &self,
        target: u64,
        req: AppendEntriesRequest,
    ) -> nodedb_raft::Result<AppendEntriesResponse> {
        let resp = self
            .send_rpc(target, RaftRpc::AppendEntriesRequest(req))
            .await
            .map_err(to_raft_err)?;
        match resp {
            RaftRpc::AppendEntriesResponse(r) => Ok(r),
            other => Err(nodedb_raft::RaftError::Transport {
                detail: format!("expected AppendEntriesResponse, got {other:?}"),
            }),
        }
    }

    async fn request_vote(
        &self,
        target: u64,
        req: RequestVoteRequest,
    ) -> nodedb_raft::Result<RequestVoteResponse> {
        let resp = self
            .send_rpc(target, RaftRpc::RequestVoteRequest(req))
            .await
            .map_err(to_raft_err)?;
        match resp {
            RaftRpc::RequestVoteResponse(r) => Ok(r),
            other => Err(nodedb_raft::RaftError::Transport {
                detail: format!("expected RequestVoteResponse, got {other:?}"),
            }),
        }
    }

    async fn install_snapshot(
        &self,
        target: u64,
        req: InstallSnapshotRequest,
    ) -> nodedb_raft::Result<InstallSnapshotResponse> {
        let resp = self
            .send_rpc(target, RaftRpc::InstallSnapshotRequest(req))
            .await
            .map_err(to_raft_err)?;
        match resp {
            RaftRpc::InstallSnapshotResponse(r) => Ok(r),
            other => Err(nodedb_raft::RaftError::Transport {
                detail: format!("expected InstallSnapshotResponse, got {other:?}"),
            }),
        }
    }

    async fn timeout_now(&self, target: u64, req: TimeoutNowRequest) -> nodedb_raft::Result<()> {
        // Fire-and-forget: the receiver sends no reply (it either campaigns or
        // ignores per its term/leader guard). Loss is tolerated — the transfer
        // aborts on its deadline and is retried by the next convergence pass.
        self.send_rpc_oneway(target, RaftRpc::TimeoutNowRequest(req))
            .await
            .map_err(to_raft_err)
    }
}
