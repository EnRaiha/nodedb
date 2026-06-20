// SPDX-License-Identifier: BUSL-1.1

//! Host-crate integration hooks for the Raft loop.
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so behaviour that
//! lives in the host crate (`nodedb`) — snapshot quarantine accounting and the
//! three cross-node shuffle stages — is reached through these `Send + Sync`
//! trait objects. The `RaftLoop` holds each as an optional field; cluster-only
//! tests leave them `None`.

use crate::error::Result;

/// Hook for quarantine integration on the Raft snapshot receive path.
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the host crate
/// (`nodedb`) supplies an implementation backed by its `QuarantineRegistry`.
/// Cluster-only tests leave the field `None`, which skips all quarantine
/// accounting.
///
/// All methods take `(group_id, last_included_index)` as the snapshot identity.
pub trait SnapshotQuarantineHook: Send + Sync + 'static {
    /// Returns `true` if the chunk identified by `(group_id, index)` is
    /// already in the quarantined state and should be rejected immediately
    /// without attempting to decode it.
    fn is_quarantined(&self, group_id: u64, last_included_index: u64) -> bool;

    /// Called after a successful decode — resets the strike counter so a
    /// single transient CRC error is not held against a healthy peer.
    fn record_success(&self, group_id: u64, last_included_index: u64);

    /// Called on a CRC-class decode failure.
    ///
    /// Returns `true` when the segment has just been quarantined (second
    /// consecutive failure), and `false` on the first strike (caller should
    /// surface the framing error and allow the peer to retry).
    fn record_failure(&self, group_id: u64, last_included_index: u64, error: &str) -> bool;
}

/// Hook for the cross-node streaming-shuffle receiver registry (E1).
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the receiver
/// registry — which is owned by `nodedb`'s `SharedState` and consumed by the
/// `!Send` Data Plane in a later unit — lives behind this `Send + Sync` hook.
/// The transport read-loop drives a `ShufflePush` stream and calls these
/// methods; the host crate's implementation deposits payloads into the
/// per-`(shuffle_id, part, side)` inbox and advances the per-part build
/// barrier.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `ShufflePush` stream
/// against a node with no receiver installed returns a typed error.
///
/// The hook is **async** because the host-crate implementation stages arriving
/// rows to a Control-Plane scratch file (E3b: receive-to-spill) and must NOT
/// block the transport reactor thread on a synchronous `std::fs` write. The
/// awaited `tokio::fs` write inside `on_shuffle_chunk` is what lets QUIC flow
/// control back-pressure the producer — the chunk is staged inline, never
/// detached into a spawned task.
#[async_trait::async_trait]
pub trait ShuffleReceiver: Send + Sync + 'static {
    /// First frame of a stream: lazily create the inbox for
    /// `(shuffle_id, part, side)` (carrying `producer_count` and `num_parts`)
    /// or reuse the existing one.
    async fn on_shuffle_request(&self, shuffle_id: u64, part: u32, side: u8, producer_count: u32);

    /// Stage one chunk payload to the inbox's scratch file (bounded — the
    /// awaited file write back-pressures the producer via QUIC flow control).
    /// Returns a typed error on a malformed chunk array or an I/O failure
    /// (never a silent drop).
    async fn on_shuffle_chunk(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> Result<()>;

    /// Terminal frame for one producer: record the `End` (advancing the
    /// barrier), flush + sync the staging file when the barrier completes, and
    /// capture any terminal error.
    async fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<crate::rpc_codec::TypedClusterError>,
    );
}

/// Hook for the cross-node shuffle PRODUCER (E4a).
///
/// Sibling of [`ShuffleReceiver`]: `nodedb-cluster` cannot depend on `nodedb`
/// (circular), so the produce logic — decode the local scan plan, run it through
/// the local streaming executor, hash-partition each output row, and fan the
/// rows out to the per-part owners (looping back into the local receiver
/// registry for self-owned parts) — lives in `nodedb` behind this `Send + Sync`
/// hook. The transport read-loop calls [`on_shuffle_produce`](Self::on_shuffle_produce)
/// when a `ShuffleProduceRequest` arrives and writes the returned outcome back as
/// a `ShuffleProduceResponse`.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `ShuffleProduce`
/// request against a node with no producer installed returns a typed
/// "not configured" error.
///
/// The hook is **async** because the produce path drives QUIC fan-out streams
/// and the local streaming executor on the Tokio transport reactor. QUIC is fine
/// here (Control Plane); the local scan itself is dispatched to the Data Plane
/// through the existing SPSC bridge by the host-crate implementation.
#[async_trait::async_trait]
pub trait ShuffleProducer: Send + Sync + 'static {
    /// Run the local scan fragment, hash-partition its rows, and fan them out to
    /// the part-owners. Returns `None` on a clean produce or `Some(err)` on a
    /// terminal scan failure (after every part has been `End`ed with the error).
    async fn on_shuffle_produce(
        &self,
        req: crate::rpc_codec::ShuffleProduceRequest,
    ) -> Option<crate::rpc_codec::TypedClusterError>;
}

/// Hook for the cross-node shuffle CONSUMER (E4b).
///
/// Sibling of [`ShuffleProducer`]: `nodedb-cluster` cannot depend on `nodedb`
/// (circular), so the consume logic — wait for both staged sides of the part to
/// finalize, resolve their local staged-file paths, run the node-local
/// grace-hash join through the Data Plane, and return the joined rows — lives in
/// `nodedb` behind this `Send + Sync` hook. The transport read-loop calls
/// [`on_shuffle_consume`](Self::on_shuffle_consume) when a `ShuffleConsumeRequest`
/// arrives and writes the returned [`ShuffleConsumeResponse`](crate::rpc_codec::ShuffleConsumeResponse)
/// back to the coordinator.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `ShuffleConsume`
/// request against a node with no consumer installed returns a typed
/// "not configured" error.
///
/// The hook is **async** because the consume path awaits the per-side finalize
/// signal (bounded by the request deadline) on the Tokio transport reactor
/// before dispatching the grace join. The grace join itself runs on the Data
/// Plane via the host crate's local executor / SPSC bridge; this hook never
/// touches storage or io_uring directly.
#[async_trait::async_trait]
pub trait ShuffleConsumer: Send + Sync + 'static {
    /// Complete one part of a distributed shuffle join: wait for both staged
    /// sides to finalize, run the node-local grace join, and return the joined
    /// rows (or a typed error on missing inbox / finalize timeout / producer
    /// terminal error / join failure). Never hangs — the finalize wait is
    /// deadline-bounded.
    async fn on_shuffle_consume(
        &self,
        req: crate::rpc_codec::ShuffleConsumeRequest,
    ) -> crate::rpc_codec::ShuffleConsumeResponse;
}

/// Hook for the cross-node distributed GROUP BY shuffle CONSUMER (E5b).
///
/// SINGLE-SIDED aggregate sibling of [`ShuffleConsumer`]: `nodedb-cluster` cannot
/// depend on `nodedb` (circular), so the aggregate-consume logic — wait for the
/// part's ONE staged producer side (side 0) to finalize, resolve its local
/// staged-file path, merge + finalize the partial `GroupState`s through the Data
/// Plane, and return the result rows — lives in `nodedb` behind this `Send +
/// Sync` hook. The transport read-loop calls
/// [`on_shuffle_aggregate`](Self::on_shuffle_aggregate) when a
/// `ShuffleAggregateConsumeRequest` arrives and writes the returned
/// [`ShuffleAggregateConsumeResponse`](crate::rpc_codec::ShuffleAggregateConsumeResponse)
/// back to the coordinator.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a
/// `ShuffleAggregateConsume` request against a node with no aggregator installed
/// returns a typed "not configured" error.
///
/// The hook is **async** because the consume path awaits the single-side finalize
/// signal (bounded by the request deadline) on the Tokio transport reactor before
/// dispatching the merge. The merge + finalize itself runs on the Data Plane via
/// the host crate's local executor / SPSC bridge; this hook never touches storage
/// or io_uring directly. Unlike [`ShuffleConsumer`] it waits for only the single
/// producer side (`0`) — there is no probe side.
#[async_trait::async_trait]
pub trait ShuffleAggregator: Send + Sync + 'static {
    /// Complete one part of a distributed GROUP BY shuffle: wait for the part's
    /// single staged producer side to finalize, merge + finalize the partial
    /// `GroupState`s, and return the aggregate rows (or a typed error on missing
    /// inbox / finalize timeout / producer terminal error / merge failure). Never
    /// hangs — the finalize wait is deadline-bounded.
    async fn on_shuffle_aggregate(
        &self,
        req: crate::rpc_codec::ShuffleAggregateConsumeRequest,
    ) -> crate::rpc_codec::ShuffleAggregateConsumeResponse;
}
