// SPDX-License-Identifier: BUSL-1.1

use crate::error::Result;
use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest,
};

/// Trait for Raft network transport.
///
/// The `nodedb-cluster` crate implements this over nexar's QUIC/RDMA
/// transport. Tests use an in-memory channel-based implementation.
pub trait RaftTransport: Send + Sync {
    /// Send AppendEntries RPC to a peer and await response.
    fn append_entries(
        &self,
        target: u64,
        req: AppendEntriesRequest,
    ) -> impl std::future::Future<Output = Result<AppendEntriesResponse>> + Send;

    /// Send RequestVote RPC to a peer and await response.
    fn request_vote(
        &self,
        target: u64,
        req: RequestVoteRequest,
    ) -> impl std::future::Future<Output = Result<RequestVoteResponse>> + Send;

    /// Send InstallSnapshot RPC to a peer and await response.
    fn install_snapshot(
        &self,
        target: u64,
        req: InstallSnapshotRequest,
    ) -> impl std::future::Future<Output = Result<InstallSnapshotResponse>> + Send;

    /// Send a TimeoutNow RPC to a peer (one-way — no response).
    ///
    /// The recipient immediately starts an election, bypassing its election
    /// timeout. The sender does not wait for any reply.
    fn timeout_now(
        &self,
        target: u64,
        req: TimeoutNowRequest,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}
