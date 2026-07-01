// SPDX-License-Identifier: Apache-2.0

//! `SyncAckResult` — the bridge response payload for an idempotent ingest.
//!
//! The Data-Plane handler serializes this into `Response.payload` after
//! running the idempotency check; the Control-Plane handler decodes it to
//! build the per-engine wire ack (e.g. `FtsIndexAckMsg`).

use serde::{Deserialize, Serialize};

use crate::sync::violation::ViolationType;
use crate::sync::wire::ack_status::AckStatus;

/// Outcome of one idempotent ingest operation returned from the Data Plane.
///
/// Serialized via zerompk into `Response.payload`; the Control Plane decodes
/// this to populate the engine-specific wire ack message.
///
/// Map-encoded so fields can be added with `#[msgpack(default)]` and older
/// payloads still decode without a migration.
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
#[msgpack(map)]
pub struct SyncAckResult {
    /// Idempotency outcome of the acknowledged ingest.
    pub status: AckStatus,
    /// Highest sequence number from this producer that has been durably applied
    /// on this stream, after processing the current message.
    pub applied_seq: u64,
    /// When set, the ingest was durably applied but a constraint was violated;
    /// carries the deterministic reason. `None` for a clean apply and for the
    /// engines that don't run apply-time validation. Only the deterministic
    /// `ViolationType` is carried — never any node-local DLQ id or timestamp.
    ///
    /// `#[msgpack(default)]`: payloads encoded before this field decode as
    /// `None`.
    #[msgpack(default)]
    pub reject: Option<ViolationType>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_with_reject_some() {
        let ack = SyncAckResult {
            status: AckStatus::Applied,
            applied_seq: 7,
            reject: Some(ViolationType::UniqueViolation {
                field: "email".into(),
                value: "x@y.com".into(),
            }),
        };
        let bytes = zerompk::to_msgpack_vec(&ack).unwrap();
        let decoded: SyncAckResult = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn round_trips_with_reject_none() {
        let ack = SyncAckResult {
            status: AckStatus::Applied,
            applied_seq: 3,
            reject: None,
        };
        let bytes = zerompk::to_msgpack_vec(&ack).unwrap();
        let decoded: SyncAckResult = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded, ack);
        assert_eq!(decoded.reject, None);
    }
}
