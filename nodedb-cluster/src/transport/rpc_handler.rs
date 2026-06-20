// SPDX-License-Identifier: BUSL-1.1

//! [`RaftRpcHandler`] — the inbound-RPC dispatch trait the transport server
//! calls for each incoming bidi stream.

use crate::error::Result;
use crate::forward::ChunkSink;
use crate::rpc_codec::{
    ExecuteRequest, RaftRpc, ShuffleProduceRequest, ShufflePushRequest, TypedClusterError,
};

/// Trait for handling incoming Raft RPCs.
///
/// Implementors receive a request [`RaftRpc`] and return the corresponding
/// response variant. The transport calls this for each incoming bidi stream.
pub trait RaftRpcHandler: Send + Sync + 'static {
    fn handle_rpc(&self, rpc: RaftRpc)
    -> impl std::future::Future<Output = Result<RaftRpc>> + Send;

    /// Handle a streaming `ExecuteStreamRequest`: execute the plan and push
    /// each result frame to `sink`. Returns `None` on a clean EOF or
    /// `Some(err)` on a terminal failure. The transport writes one
    /// `RPC_EXECUTE_STREAM_END` envelope carrying this outcome after the call
    /// returns. See [`crate::forward::PlanExecutor::execute_plan_streaming`].
    fn handle_rpc_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> impl std::future::Future<Output = Option<TypedClusterError>> + Send;

    /// Cross-node streaming shuffle (E1) — opening frame of a `ShufflePush`
    /// stream. Lazily creates the receiver inbox for `(shuffle_id, part, side)`
    /// carrying `producer_count` and `num_parts`.
    fn on_shuffle_request(
        &self,
        req: ShufflePushRequest,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Deposit one shuffle chunk payload into the receiver inbox. Bounded —
    /// the implementation blocks while the inbox buffer is full so QUIC flow
    /// control back-pressures the producer.
    fn on_shuffle_chunk(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Terminal frame for one producer of a `ShufflePush` stream: record the
    /// `End` (advancing the per-part build barrier) and capture any terminal
    /// error.
    fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<TypedClusterError>,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Cross-node shuffle PRODUCER trigger (E4a). Execute the local scan
    /// fragment carried by `req`, hash-partition each output row on `req.keys`,
    /// and fan the rows out to the per-part owners as `ShufflePush` streams
    /// (looping back for parts this node owns). Returns `None` on a clean
    /// produce, or `Some(err)` if the scan failed (every part has already been
    /// `End`ed with the same error). The transport writes exactly one
    /// `ShuffleProduceResponse` carrying this outcome back to the coordinator.
    fn on_shuffle_produce(
        &self,
        req: ShuffleProduceRequest,
    ) -> impl std::future::Future<Output = Option<TypedClusterError>> + Send;
}
