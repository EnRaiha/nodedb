// SPDX-License-Identifier: BUSL-1.1

//! Small wire-format types embedded inside [`super::ReplicatedWrite`].

// ── Replicated write envelope ───────────────────────────────────────

/// One edge of an `EdgePutBatch` / `EdgeDeleteBatch` in the cross-node wire
/// shape. Mirrors `nodedb_physical::physical_plan::BatchEdge` but carries the
/// endpoint surrogates as `u32` (not the `Surrogate` newtype) so the payload
/// uses only trivially serializable types, exactly like the single `EdgePut`
/// variant. Followers bind both surrogates verbatim on apply (never
/// re-allocate), so the same `src_id`/`dst_id` resolves to the same identity
/// on every replica.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReplicatedBatchEdge {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    /// Leader-assigned global surrogate for the source node (binding key =
    /// `src_id.as_bytes()`).
    pub src_surrogate: u32,
    /// Leader-assigned global surrogate for the destination node (binding key =
    /// `dst_id.as_bytes()`).
    pub dst_surrogate: u32,
}

/// One entry of a write's materialized-sum resolution, in the cross-node wire
/// shape: which target row the binding's `(target collection, join value)` pair
/// names.
///
/// The surrogate travels as a bare `u32` rather than the `Surrogate` newtype,
/// like every other identity on this wire.
///
/// This shape supersedes the `(join_value, surrogate)` pairs the `*_sum_targets`
/// slots carry. Those pairs cannot express a source that drives two bindings
/// sharing a join column into different targets: the applier looks a value up,
/// finds the FIRST binding's target row, and folds the second binding's balance
/// into it. Both records travel — see
/// `ReplicatedWrite::PointPut::resolved_sum_target_bindings`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReplicatedSumTarget {
    /// TARGET collection of the binding this entry was resolved for, as the
    /// catalog names it.
    pub target_collection: String,
    /// Join-key value naming the target row.
    pub join_value: String,
    /// The target row's surrogate.
    pub surrogate: u32,
}

/// One row of a `ColumnarBulkDmlResolved` write: the Control Plane already
/// resolved this row from the predicate and decided the write policy against
/// its exact image, so the wire shape carries the image itself rather than
/// anything a follower would need to re-derive.
///
/// `new_row_msgpack` is empty for a delete row — a delete needs only the
/// primary key to remove the row.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ColumnarResolvedRow {
    /// MessagePack-encoded primary key value.
    pub pk_msgpack: Vec<u8>,
    /// MessagePack-encoded full post-image row. Empty for a delete.
    pub new_row_msgpack: Vec<u8>,
}

/// One mutation of a `KvResolvedWrite` in the cross-node wire shape.
///
/// Mirrors `nodedb_physical::physical_plan::KvResolvedMutation`, with the
/// surrogate as a bare `u32` rather than the `Surrogate` newtype — the same
/// convention every other identity on this wire uses. Decode binds it through
/// the surrogate assigner, so a follower addresses the row the leader did.
///
/// `precondition` is the drift check, not a CAS condition: `None` means the
/// key must be ABSENT at apply time, `Some(bytes)` that it must hold exactly
/// those raw stored bytes.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum KvResolvedMutationWire {
    Put {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Absolute expiry instant resolved by the proposing node, `0` for
        /// none. Carried for the same reason `KvPut::resolved_now_ms` is: no
        /// applying node may re-derive it from its own clock.
        expire_at_ms: u64,
        surrogate: u32,
        precondition: Option<Vec<u8>>,
    },
    Delete {
        collection: String,
        key: Vec<u8>,
        precondition: Option<Vec<u8>>,
    },
    Expire {
        collection: String,
        key: Vec<u8>,
        ttl_ms: u64,
        /// See `ReplicatedWrite::KvExpire::resolved_now_ms`. Per-mutation here
        /// because one resolved write can carry several.
        resolved_now_ms: u64,
        precondition: Option<Vec<u8>>,
    },
    Persist {
        collection: String,
        key: Vec<u8>,
        precondition: Option<Vec<u8>>,
    },
}

/// One mutation of a `DocumentResolvedWrite` in the cross-node wire shape.
///
/// Mirrors `nodedb_physical::physical_plan::DocumentResolvedMutation`, with the
/// surrogate as a bare `u32` rather than the `Surrogate` newtype — the same
/// convention every other identity on this wire uses. Decode binds it through
/// the surrogate assigner, so a follower addresses the row the leader did.
///
/// `value` is the PRE-ENCODE MessagePack body for both storage modes; the
/// applying node encodes a strict collection's Binary Tuple on the way to disk.
/// `precondition` is RAW STORED bytes and is the drift check, not business
/// logic: `None` means the row must be ABSENT at apply time, `Some(bytes)` that
/// it must hold exactly those bytes.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DocumentResolvedMutationWire {
    Put {
        collection: String,
        document_id: String,
        surrogate: u32,
        value: Vec<u8>,
        precondition: Option<Vec<u8>>,
        /// `(target collection, join value)` -> target surrogate, resolved by
        /// the proposing node. Carried for the same reason every other document
        /// record carries it: no applying node can resolve it locally.
        resolved_sum_targets: Vec<ReplicatedSumTarget>,
    },
    Delete {
        collection: String,
        document_id: String,
        surrogate: u32,
        precondition: Option<Vec<u8>>,
        /// See `Put::resolved_sum_targets`.
        resolved_sum_targets: Vec<ReplicatedSumTarget>,
    },
}

/// Whether a `ConstraintChange` installs (`Set`) or removes (`Drop`) a
/// collection's constraint set on every replica.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ConstraintChangeOp {
    Set,
    Drop,
}
