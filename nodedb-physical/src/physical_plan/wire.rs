// SPDX-License-Identifier: Apache-2.0

//! Wire-format encode/decode helpers for PhysicalPlan.
//!
//! MessagePack encoding via zerompk. Used by the cluster layer to ship
//! physical plans over the wire as part of `ExecuteRequest` RPC.

use super::{PhysicalPlan, QueryOp};

/// Errors produced by the wire encode/decode helpers. Self-contained so this
/// module can move into the shared `nodedb-physical` crate without dragging
/// Origin's `Error` type along.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("{0}")]
    InvalidPlan(&'static str),
    #[error("plan codec: {0}")]
    Codec(String),
}

/// Encode a `PhysicalPlan` to MessagePack bytes.
///
/// Returns an error for plans that are node-local by construction and must
/// never be shipped over the QUIC wire:
/// - `ClusterArray` variants (handled on the Control Plane); and
/// - `QueryOp::ShuffleJoinConsume` and `QueryOp::ShuffleAggregateConsume`
///   (carry node-local staged-file paths; built locally by the part-owner's
///   consume hook and dispatched only to that same node's Data Plane).
///
/// `QueryOp::PartialAggregateState` is wire-SHIPPABLE — it carries a collection
/// name plus an optional `input` sub-plan (a wire-shippable `ProviderScan`) and
/// is dispatched to a remote producer's Data Plane — so it is intentionally NOT
/// rejected here.
pub fn encode(plan: &PhysicalPlan) -> Result<Vec<u8>, WireError> {
    if matches!(plan, PhysicalPlan::ClusterArray(_)) {
        return Err(WireError::InvalidPlan(
            "ClusterArray plans must not be sent over the wire",
        ));
    }
    if matches!(
        plan,
        PhysicalPlan::Query(QueryOp::ShuffleJoinConsume { .. })
    ) {
        return Err(WireError::InvalidPlan(
            "ShuffleJoinConsume plans carry node-local paths and must not be sent over the wire",
        ));
    }
    if matches!(
        plan,
        PhysicalPlan::Query(QueryOp::ShuffleAggregateConsume { .. })
    ) {
        return Err(WireError::InvalidPlan(
            "ShuffleAggregateConsume plans carry node-local paths and must not be sent over the wire",
        ));
    }
    zerompk::to_msgpack_vec(plan).map_err(|e| WireError::Codec(format!("encode: {e}")))
}

/// Decode a `PhysicalPlan` from MessagePack bytes.
pub fn decode(bytes: &[u8]) -> Result<PhysicalPlan, WireError> {
    zerompk::from_msgpack(bytes).map_err(|e| WireError::Codec(format!("decode: {e}")))
}

/// Encode a `Vec<PhysicalPlan>` to MessagePack bytes.
///
/// Used by the Calvin scheduler when building `TxClass::plans` bytes for a
/// cross-shard transaction that will be shipped through the sequencer.
pub fn encode_batch(plans: &Vec<PhysicalPlan>) -> Result<Vec<u8>, WireError> {
    for plan in plans {
        if matches!(plan, PhysicalPlan::ClusterArray(_)) {
            return Err(WireError::InvalidPlan(
                "ClusterArray plans must not be shipped via the sequencer",
            ));
        }
        if matches!(
            plan,
            PhysicalPlan::Query(QueryOp::ShuffleJoinConsume { .. })
        ) {
            return Err(WireError::InvalidPlan(
                "ShuffleJoinConsume plans carry node-local paths and must not be shipped via the sequencer",
            ));
        }
        if matches!(
            plan,
            PhysicalPlan::Query(QueryOp::ShuffleAggregateConsume { .. })
        ) {
            return Err(WireError::InvalidPlan(
                "ShuffleAggregateConsume plans carry node-local paths and must not be shipped via the sequencer",
            ));
        }
    }
    zerompk::to_msgpack_vec(plans).map_err(|e| WireError::Codec(format!("batch encode: {e}")))
}

/// Decode a `Vec<PhysicalPlan>` from MessagePack bytes.
///
/// Used by the Calvin scheduler to decode the opaque `TxClass::plans` blob
/// into executable plans for dispatch via `MetaOp::CalvinExecute`.
pub fn decode_batch(bytes: &[u8]) -> Result<Vec<PhysicalPlan>, WireError> {
    zerompk::from_msgpack(bytes).map_err(|e| WireError::Codec(format!("batch decode: {e}")))
}
