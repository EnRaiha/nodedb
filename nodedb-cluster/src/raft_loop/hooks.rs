// SPDX-License-Identifier: BUSL-1.1

//! Host-crate integration hooks for the Raft loop.
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so behaviour that
//! lives in the host crate (`nodedb`) — snapshot quarantine accounting and the
//! three cross-node shuffle stages — is reached through these `Send + Sync`
//! trait objects. The `RaftLoop` holds each as an optional field; cluster-only
//! tests leave them `None`.

use crate::error::Result;

/// Hook for building per-group snapshot payloads on the Raft snapshot SEND path.
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the snapshot
/// builder — which serializes the engine state for the vshards owned by a Raft
/// group so a lagging/new follower can be caught up — lives in the host crate
/// (`nodedb`) behind this `Send + Sync` hook. The tick loop's install-snapshot
/// dispatch (see [`super::tick`]) calls [`build_group_snapshot`](Self::build_group_snapshot)
/// before framing the chunked `InstallSnapshot` RPC.
///
/// Cluster-only tests leave the `RaftLoop` field `None`, which makes the sender
/// fall back to the stub (empty) chunk — exactly the pre-builder behaviour.
///
/// The hook is **async** because the host-crate implementation dispatches the
/// per-vshard snapshot build to the Data Plane through the existing SPSC bridge
/// (an awaited round-trip on the Tokio transport reactor); it never touches
/// io_uring or storage directly.
#[async_trait::async_trait]
pub trait SnapshotBuilder: Send + Sync + 'static {
    /// Build the per-group snapshot payload (serialized engine state for the
    /// group's vshards) to ship to a lagging/new follower. Empty Vec is a valid
    /// "nothing to send" result (caller falls back to the stub chunk).
    async fn build_group_snapshot(
        &self,
        group_id: u64,
        last_included_index: u64,
        last_included_term: u64,
    ) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Hook for applying a received per-group snapshot to local engine state on the
/// Raft snapshot RECEIVE path.
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the follower-side
/// apply — which deserializes the per-group `TenantDataSnapshot` bytes and
/// installs them into the local Data-Plane state machine through the existing
/// SPSC bridge — lives in the host crate (`nodedb`) behind this `Send + Sync`
/// hook. The install-snapshot finalize path (see
/// [`crate::install_snapshot::finalize::commit`]) calls
/// [`apply_snapshot`](Self::apply_snapshot) AFTER the atomic `.partial`→`.snap`
/// rename and BEFORE advancing Raft, so the data is visible on this node before
/// the Raft log boundary moves.
///
/// Cluster-only tests leave the `RaftLoop` field `None`, which makes the
/// follower advance Raft WITHOUT restoring engine state — the pre-applier
/// behaviour (correct for tests that ship only the empty bootstrap stub).
///
/// The hook is **async** because the host-crate implementation dispatches the
/// per-tenant restore to the Data Plane through the SPSC bridge (an awaited
/// round-trip on the Tokio transport reactor); it never touches io_uring or
/// storage directly.
#[async_trait::async_trait]
pub trait SnapshotApplier: Send + Sync + 'static {
    /// Apply a per-group snapshot to the local data-plane state machine.
    /// Called AFTER the atomic .partial→.snap rename, BEFORE handle_install_snapshot
    /// advances Raft. Err MUST prevent the raft advance (follower retries).
    /// group_id 0 (metadata) is a no-op (metadata restored inline).
    async fn apply_snapshot(
        &self,
        group_id: u64,
        snapshot_bytes: &[u8],
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

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
    /// the part-owners. Returns a [`ShuffleProduceResponse`] whose `error` is
    /// `None` on a clean produce or `Some(err)` on a terminal scan failure (after
    /// every part has been `End`ed with the error), and whose `read_version_lsn`
    /// carries the max per-collection read version the local scan observed (`0` on
    /// failure) for the coordinator's cross-shard OCC read validation.
    async fn on_shuffle_produce(
        &self,
        req: crate::rpc_codec::ShuffleProduceRequest,
    ) -> crate::rpc_codec::ShuffleProduceResponse;
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

/// Hook for routed-surrogate-exchange (F1b).
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the assign logic —
/// run a LOCAL `SurrogateAssigner::assign` for the `(collection, pk)` endpoint
/// key carried by the request — lives in `nodedb` behind this `Send + Sync` hook.
/// The transport read-loop calls [`on_assign_surrogate`](Self::on_assign_surrogate)
/// when an `AssignSurrogateRequest` arrives at the home vShard's LEADER and writes
/// the returned [`AssignSurrogateResponse`](crate::rpc_codec::AssignSurrogateResponse)
/// back to the coordinator.
///
/// Because the handler runs on the home node (the vShard leader), a LOCAL assign
/// yields the AUTHORITATIVE surrogate: the first call allocates it and every later
/// call for the same key returns the same value (idempotent, first-wins). The
/// coordinator routes here precisely so the value it carries is the one the home
/// node will store under.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; an `AssignSurrogate`
/// request against a node with no assigner installed returns a typed "not
/// configured" error.
///
/// The hook is **async** for signature symmetry with the other one-shot hooks;
/// the host-crate implementation performs a synchronous local assign (the
/// `SurrogateAssigner` is a sync `Send + Sync` facade) and never touches
/// io_uring or the Data Plane directly.
#[async_trait::async_trait]
pub trait AssignRemoteSurrogate: Send + Sync + 'static {
    /// Assign-or-return the authoritative surrogate for the `(collection, pk)`
    /// endpoint key carried by `req`. Returns an [`AssignSurrogateResponse`] with
    /// the surrogate on success or a typed error on failure (never a silent
    /// drop).
    async fn on_assign_surrogate(
        &self,
        req: crate::rpc_codec::AssignSurrogateRequest,
    ) -> crate::rpc_codec::AssignSurrogateResponse;
}

/// Hook for routed Calvin-submit (Cv1).
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the submit logic —
/// decode the `TxClass`, submit it to THIS node's Calvin sequencer inbox, and
/// await assignment + completion through the node-local `CalvinCompletionRegistry`
/// — lives in `nodedb` behind this `Send + Sync` hook. The transport read-loop
/// calls [`on_submit_calvin_txn`](Self::on_submit_calvin_txn) when a
/// `SubmitCalvinTxnRequest` arrives at the SEQUENCER-GROUP leader and writes the
/// returned [`SubmitCalvinTxnResponse`](crate::rpc_codec::SubmitCalvinTxnResponse)
/// back to the coordinator.
///
/// Because the handler runs on the sequencer-group leader, the submit-and-await
/// is correct: only the leader's sequencer service assigns transactions
/// (`note_assigned`), and only the leader's registry receives BOTH the
/// assignment and the replicated completion ack. The coordinator routes here
/// precisely so the submit lands where it will actually be sequenced and acked.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `SubmitCalvinTxn`
/// request against a node with no Calvin-submit hook installed returns a typed
/// "not configured" error.
///
/// The hook is **async** because the submit-and-await blocks on the assignment
/// and completion oneshot channels (bounded by the request deadline) on the
/// Tokio transport reactor. The actual transaction execution happens on the Data
/// Plane via the sequencer service / per-vshard schedulers; this hook never
/// touches io_uring or storage directly.
#[async_trait::async_trait]
pub trait CalvinSubmit: Send + Sync + 'static {
    /// Submit the `TxClass` carried by `req` (msgpack-encoded) to this node's
    /// Calvin sequencer inbox and await its completion. Returns a
    /// [`SubmitCalvinTxnResponse`](crate::rpc_codec::SubmitCalvinTxnResponse)
    /// with `error: None` on commit or a typed error on failure (never a silent
    /// drop).
    async fn on_submit_calvin_txn(
        &self,
        req: crate::rpc_codec::SubmitCalvinTxnRequest,
    ) -> crate::rpc_codec::SubmitCalvinTxnResponse;
}

/// Hook for routed Calvin-INBOX submit (Cv1).
///
/// OLLP dependent sibling of [`CalvinSubmit`]: `nodedb-cluster` cannot depend on
/// `nodedb` (circular), so the submit logic — decode the `TxClass`, submit it to
/// THIS node's Calvin sequencer inbox, and await only the ASSIGNMENT (NOT
/// completion) through the node-local `CalvinCompletionRegistry` — lives in
/// `nodedb` behind this `Send + Sync` hook. The transport read-loop calls
/// [`on_submit_calvin_inbox`](Self::on_submit_calvin_inbox) when a
/// `SubmitCalvinInboxRequest` arrives at the SEQUENCER-GROUP leader and writes the
/// returned [`SubmitCalvinInboxResponse`](crate::rpc_codec::SubmitCalvinInboxResponse)
/// back to the coordinator.
///
/// Because the handler runs on the sequencer-group leader, the submit-and-assign
/// is correct: only the leader's sequencer service assigns transactions
/// (`note_assigned`). Unlike [`CalvinSubmit`] it returns AS SOON AS the
/// assignment is observed — the OLLP coordinator loop drives the dependent
/// transaction to completion itself in a later unit, so this hook must NOT block
/// until completion.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `SubmitCalvinInbox`
/// request against a node with no Calvin-inbox hook installed returns a typed
/// "not configured" error.
///
/// The hook is **async** because the submit-and-assign blocks on the assignment
/// oneshot channel (bounded by the request deadline) on the Tokio transport
/// reactor. The actual transaction execution happens on the Data Plane via the
/// sequencer service / per-vshard schedulers; this hook never touches io_uring or
/// storage directly.
#[async_trait::async_trait]
pub trait CalvinSubmitInbox: Send + Sync + 'static {
    /// Submit the `TxClass` carried by `req` (msgpack-encoded) to this node's
    /// Calvin sequencer inbox and await its ASSIGNMENT (not completion). Returns
    /// a [`SubmitCalvinInboxResponse`](crate::rpc_codec::SubmitCalvinInboxResponse)
    /// with `error: None` carrying the assignment on success or a typed error on
    /// failure (never a silent drop).
    async fn on_submit_calvin_inbox(
        &self,
        req: crate::rpc_codec::SubmitCalvinInboxRequest,
    ) -> crate::rpc_codec::SubmitCalvinInboxResponse;
}

/// Hook for routed reserve-read (Calvin OLLP).
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the reserve
/// logic — decode the `LockKey` and assign-only reserve the read lock through
/// THIS node's Calvin sequencer scheduler — lives in `nodedb` behind this
/// `Send + Sync` hook. The transport read-loop calls
/// [`on_reserve_read`](Self::on_reserve_read) when a `ReserveReadRequest`
/// arrives at the SEQUENCER-GROUP leader and writes the returned
/// [`ReserveReadResponse`](crate::rpc_codec::ReserveReadResponse) back to the
/// coordinator.
///
/// Because the handler runs on the sequencer-group leader, the reserve is
/// correct: only the leader's scheduler holds the authoritative lock table for
/// its local sequencer inbox. The coordinator routes here precisely so the
/// reservation lands where it will actually be enforced.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `ReserveRead`
/// request against a node with no reserve-read hook installed returns a typed
/// "not configured" error.
///
/// The hook is **async** for signature symmetry with the other one-shot
/// hooks; the reserve itself is bounded by the request deadline on the Tokio
/// transport reactor. It never touches io_uring or storage directly.
#[async_trait::async_trait]
pub trait ReserveRead: Send + Sync + 'static {
    /// Assign-only reserve the read lock for the `LockKey` carried by `req`.
    /// Returns a [`ReserveReadResponse`](crate::rpc_codec::ReserveReadResponse)
    /// with the minted (or confirmed) owner on success or a typed error on
    /// failure (never a silent drop).
    async fn on_reserve_read(
        &self,
        req: crate::rpc_codec::ReserveReadRequest,
    ) -> crate::rpc_codec::ReserveReadResponse;
}

/// Hook for routed release-reservation (Calvin OLLP).
///
/// Ack-only sibling of [`ReserveRead`]: `nodedb-cluster` cannot depend on
/// `nodedb` (circular), so the release logic — decode the owner and release
/// reason, and release the reservation through THIS node's Calvin sequencer
/// scheduler — lives in `nodedb` behind this `Send + Sync` hook. The transport
/// read-loop calls [`on_release_reservation`](Self::on_release_reservation)
/// when a `ReleaseReservationRequest` arrives at the SEQUENCER-GROUP leader
/// and writes the returned
/// [`ReleaseReservationResponse`](crate::rpc_codec::ReleaseReservationResponse)
/// back to the coordinator.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a
/// `ReleaseReservation` request against a node with no release-reservation
/// hook installed returns a typed "not configured" error.
///
/// The hook is **async** for signature symmetry with the other one-shot
/// hooks; the release itself is bounded by the request deadline on the Tokio
/// transport reactor. It never touches io_uring or storage directly.
#[async_trait::async_trait]
pub trait ReleaseReservation: Send + Sync + 'static {
    /// Release the reservation held by the owner carried by `req`. Returns a
    /// [`ReleaseReservationResponse`](crate::rpc_codec::ReleaseReservationResponse)
    /// with `error: None` on success (ack) or a typed error on failure (never
    /// a silent drop).
    async fn on_release_reservation(
        &self,
        req: crate::rpc_codec::ReleaseReservationRequest,
    ) -> crate::rpc_codec::ReleaseReservationResponse;
}
