// SPDX-License-Identifier: Apache-2.0

//! `BatchEdge`: one edge in an `EdgePutBatch` / `EdgeDeleteBatch`.

use nodedb_types::Surrogate;

/// One edge in an `EdgePutBatch` / `EdgeDeleteBatch`.
///
/// `src_surrogate` / `dst_surrogate` carry the global row identity for the
/// edge endpoints (resolved at construction time via the surrogate assigner).
/// `Surrogate::ZERO` is used in test fixtures and on in-memory paths where
/// no catalog is wired; production paths always populate real surrogates.
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
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub src_surrogate: Surrogate,
    pub dst_surrogate: Surrogate,
}
