// SPDX-License-Identifier: Apache-2.0

//! `BatchEdge`: one edge in an `EdgePutBatch` / `EdgeDeleteBatch`.

use nodedb_types::{QualifiedCollection, Surrogate};

/// One edge in an `EdgePutBatch` / `EdgeDeleteBatch`.
///
/// `src_surrogate` / `dst_surrogate` carry the global row identity for the
/// edge endpoints (resolved at construction time via the surrogate assigner).
/// Neither is ever `Surrogate::ZERO`: an edge that carries it is refused.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BatchEdge {
    pub collection: QualifiedCollection,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub src_surrogate: Surrogate,
    pub dst_surrogate: Surrogate,
}
