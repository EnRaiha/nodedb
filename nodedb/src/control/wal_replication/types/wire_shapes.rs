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
