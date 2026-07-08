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
