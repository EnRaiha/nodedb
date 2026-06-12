// SPDX-License-Identifier: Apache-2.0

//! `SyncAckResult` — the bridge response payload for an idempotent ingest.
//!
//! The Data-Plane handler serializes this into `Response.payload` after
//! running the idempotency check; the Control-Plane handler decodes it to
//! build the per-engine wire ack (e.g. `FtsIndexAckMsg`).

use serde::{Deserialize, Serialize};

use crate::sync::wire::ack_status::AckStatus;

/// Outcome of one idempotent ingest operation returned from the Data Plane.
///
/// Serialized via zerompk into `Response.payload`; the Control Plane decodes
/// this to populate the engine-specific wire ack message.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SyncAckResult {
    /// Idempotency outcome of the acknowledged ingest.
    pub status: AckStatus,
    /// Highest sequence number from this producer that has been durably applied
    /// on this stream, after processing the current message.
    pub applied_seq: u64,
}
