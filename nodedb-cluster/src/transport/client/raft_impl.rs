// SPDX-License-Identifier: BUSL-1.1

//! `nodedb_raft::RaftTransport` trait impl — dispatches Raft RPCs through the
//! outbound [`send_rpc`] path and unpacks the typed response.
//!
//! [`send_rpc`]: super::transport::NexarTransport::send_rpc

use nodedb_raft::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    PreVoteRequest, PreVoteResponse, RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest,
};
use nodedb_raft::transport::RaftTransport;

use crate::error::ClusterError;
use crate::rpc_codec::RaftRpc;

use super::transport::NexarTransport;

/// Map a send error onto the Raft transport's error type.
///
/// A group the peer does not host stays typed. Every other error crosses as
/// a transport error with its message.
fn to_raft_err(e: ClusterError) -> nodedb_raft::RaftError {
    match e {
        ClusterError::Raft(e) => e,
        ClusterError::GroupNotFound { group_id } => {
            nodedb_raft::RaftError::GroupNotFound { group_id }
        }
        other => nodedb_raft::RaftError::Transport {
            detail: other.to_string(),
        },
    }
}

/// The error for a reply of the wrong type.
fn unexpected_reply(expected: &str, reply: RaftRpc) -> ClusterError {
    ClusterError::Codec {
        detail: format!("expected {expected}, got {reply:?}"),
    }
}

impl NexarTransport {
    /// Send AppendEntries and keep the typed cluster error.
    ///
    /// The tick loop tells a link failure from a refusal by this error.
    pub async fn send_append_entries(
        &self,
        target: u64,
        req: AppendEntriesRequest,
    ) -> crate::error::Result<AppendEntriesResponse> {
        match self
            .send_rpc(target, RaftRpc::AppendEntriesRequest(req))
            .await?
        {
            RaftRpc::AppendEntriesResponse(r) => Ok(r),
            other => Err(unexpected_reply("AppendEntriesResponse", other)),
        }
    }

    /// Send a PreVote probe and keep the typed cluster error.
    pub async fn send_pre_vote(
        &self,
        target: u64,
        req: PreVoteRequest,
    ) -> crate::error::Result<PreVoteResponse> {
        match self.send_rpc(target, RaftRpc::PreVoteRequest(req)).await? {
            RaftRpc::PreVoteResponse(r) => Ok(r),
            other => Err(unexpected_reply("PreVoteResponse", other)),
        }
    }
}

impl RaftTransport for NexarTransport {
    async fn append_entries(
        &self,
        target: u64,
        req: AppendEntriesRequest,
    ) -> nodedb_raft::Result<AppendEntriesResponse> {
        self.send_append_entries(target, req)
            .await
            .map_err(to_raft_err)
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

    async fn pre_vote(
        &self,
        target: u64,
        req: PreVoteRequest,
    ) -> nodedb_raft::Result<PreVoteResponse> {
        self.send_pre_vote(target, req).await.map_err(to_raft_err)
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
