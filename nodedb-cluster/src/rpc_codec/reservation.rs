// SPDX-License-Identifier: BUSL-1.1

//! Routed reserve-read / release-reservation one-shot RPCs (Calvin OLLP).
//!
//! Assign-only reserve and ack-only release for the Calvin OLLP dependent-read
//! path: a coordinator that wants to reserve or release a read lock on the
//! SEQUENCER-GROUP LEADER sends one of these requests; the leader mutates its
//! local scheduler state and replies with exactly one response frame. Both are
//! one-shot request/response — no streaming.
//!
//! The domain payloads (`LockKeyWire`, `TxnIdWire`, `ReleaseReason` — see
//! [`crate::calvin::types::lock_wire`]) are msgpack-only, not rkyv. They ride
//! these rkyv envelope structs as opaque pre-encoded bytes (mirroring
//! `tx_class_bytes` in [`super::calvin_submit`]) and are decoded at the
//! hook boundary in the host crate.
//!
//! Discriminants 41/42 (`ReserveRead`) and 43/44 (`ReleaseReservation`) are
//! permanently assigned to these variants.

use super::discriminants::*;
use super::execute::TypedClusterError;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

// ── Wire types ──────────────────────────────────────────────────────────────

/// Coordinator → sequencer-leader routed reserve-read request.
///
/// Carries the `LockKey` as opaque msgpack bytes (`lock_key_bytes`); the
/// leader decodes it and assign-only reserves the read lock, minting an owner
/// if `owner_bytes` is `None`. The `deadline_remaining_ms` / `trace_id` fields
/// mirror [`SubmitCalvinInboxRequest`](super::calvin_submit::SubmitCalvinInboxRequest)
/// so the leader-side handler shares the same deadline / tracing prologue as
/// the other one-shot RPCs; the leader bounds the reserve by
/// `deadline_remaining_ms`.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ReserveReadRequest {
    /// The `LockKey` to reserve, encoded with `zerompk::to_msgpack_vec`.
    pub lock_key_bytes: Vec<u8>,
    pub vshard: u32,
    /// Pre-assigned owner (`TxnIdWire`, msgpack-encoded), when the reservation
    /// is being made on behalf of an already-known owner. `None` means the
    /// leader mints a new owner.
    pub owner_bytes: Option<Vec<u8>>,
    /// Deadline budget remaining for the reserve on the leader (ms).
    pub deadline_remaining_ms: u64,
    pub trace_id: [u8; 16],
}

/// Terminal reply to a [`ReserveReadRequest`].
///
/// `error: None` means the leader reserved the read lock; `owner_bytes`
/// carries the minted (or confirmed) owner (`TxnIdWire`, msgpack-encoded).
/// `error: Some(e)` means the reserve failed (lock conflict, the `LockKey`
/// failed to decode, or no reserve-read hook is configured); `owner_bytes` is
/// `None` in that case.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ReserveReadResponse {
    pub owner_bytes: Option<Vec<u8>>,
    pub error: Option<TypedClusterError>,
}

/// Coordinator → sequencer-leader routed release-reservation request.
///
/// Carries the owner (`TxnIdWire`) and release reason (`ReleaseReason`) as
/// opaque msgpack bytes; the leader decodes both and releases the
/// reservation. Ack-only — there is no data payload to return beyond success
/// or a typed error.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ReleaseReservationRequest {
    /// The owner (`TxnIdWire`) releasing the reservation, encoded with
    /// `zerompk::to_msgpack_vec`.
    pub owner_bytes: Vec<u8>,
    pub vshard: u32,
    /// The release reason (`ReleaseReason`), encoded with
    /// `zerompk::to_msgpack_vec`.
    pub reason_bytes: Vec<u8>,
    /// Deadline budget remaining for the release on the leader (ms).
    pub deadline_remaining_ms: u64,
    pub trace_id: [u8; 16],
}

/// Terminal reply to a [`ReleaseReservationRequest`].
///
/// `error: None` means the leader released the reservation (ack). `error:
/// Some(e)` means the release failed (unknown owner, either payload failed to
/// decode, or no release-reservation hook is configured).
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ReleaseReservationResponse {
    pub error: Option<TypedClusterError>,
}

// ── Codec ────────────────────────────────────────────────────────────────────

macro_rules! to_bytes {
    ($msg:expr) => {
        rkyv::to_bytes::<rkyv::rancor::Error>($msg)
            .map(|b| b.to_vec())
            .map_err(|e| ClusterError::Codec {
                detail: format!("rkyv serialize: {e}"),
            })
    };
}

macro_rules! from_bytes {
    ($payload:expr, $T:ty, $name:expr) => {{
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity($payload.len());
        aligned.extend_from_slice($payload);
        rkyv::from_bytes::<$T, rkyv::rancor::Error>(&aligned).map_err(|e| ClusterError::Codec {
            detail: format!("rkyv deserialize {}: {e}", $name),
        })
    }};
}

pub(super) fn encode_reserve_read_req(msg: &ReserveReadRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_RESERVE_READ_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_reserve_read_resp(msg: &ReserveReadResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_RESERVE_READ_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_reserve_read_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ReserveReadRequest(from_bytes!(
        payload,
        ReserveReadRequest,
        "ReserveReadRequest"
    )?))
}
pub(super) fn decode_reserve_read_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ReserveReadResponse(from_bytes!(
        payload,
        ReserveReadResponse,
        "ReserveReadResponse"
    )?))
}

pub(super) fn encode_release_reservation_req(
    msg: &ReleaseReservationRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_RELEASE_RESERVATION_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_release_reservation_resp(
    msg: &ReleaseReservationResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_RELEASE_RESERVATION_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_release_reservation_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ReleaseReservationRequest(from_bytes!(
        payload,
        ReleaseReservationRequest,
        "ReleaseReservationRequest"
    )?))
}
pub(super) fn decode_release_reservation_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ReleaseReservationResponse(from_bytes!(
        payload,
        ReleaseReservationResponse,
        "ReleaseReservationResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_reserve_req(req: ReserveReadRequest) -> ReserveReadRequest {
        let rpc = RaftRpc::ReserveReadRequest(req);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ReserveReadRequest(r) => r,
            other => panic!("expected ReserveReadRequest, got {other:?}"),
        }
    }

    fn roundtrip_reserve_resp(resp: ReserveReadResponse) -> ReserveReadResponse {
        let rpc = RaftRpc::ReserveReadResponse(resp);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ReserveReadResponse(r) => r,
            other => panic!("expected ReserveReadResponse, got {other:?}"),
        }
    }

    fn roundtrip_release_req(req: ReleaseReservationRequest) -> ReleaseReservationRequest {
        let rpc = RaftRpc::ReleaseReservationRequest(req);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ReleaseReservationRequest(r) => r,
            other => panic!("expected ReleaseReservationRequest, got {other:?}"),
        }
    }

    fn roundtrip_release_resp(resp: ReleaseReservationResponse) -> ReleaseReservationResponse {
        let rpc = RaftRpc::ReleaseReservationResponse(resp);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ReleaseReservationResponse(r) => r,
            other => panic!("expected ReleaseReservationResponse, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_reserve_read_request() {
        let req = ReserveReadRequest {
            lock_key_bytes: vec![0x01, 0x02, 0x03],
            vshard: 4,
            owner_bytes: None,
            deadline_remaining_ms: 10_000,
            trace_id: [9u8; 16],
        };
        let decoded = roundtrip_reserve_req(req);
        assert_eq!(decoded.lock_key_bytes, vec![0x01, 0x02, 0x03]);
        assert_eq!(decoded.vshard, 4);
        assert!(decoded.owner_bytes.is_none());
        assert_eq!(decoded.deadline_remaining_ms, 10_000);
        assert_eq!(decoded.trace_id, [9u8; 16]);
    }

    #[test]
    fn roundtrip_reserve_read_request_with_owner() {
        let req = ReserveReadRequest {
            lock_key_bytes: vec![],
            vshard: 0,
            owner_bytes: Some(vec![0xAA, 0xBB]),
            deadline_remaining_ms: 0,
            trace_id: [0u8; 16],
        };
        let decoded = roundtrip_reserve_req(req);
        assert_eq!(decoded.owner_bytes, Some(vec![0xAA, 0xBB]));
    }

    #[test]
    fn roundtrip_reserve_read_response_ok() {
        let decoded = roundtrip_reserve_resp(ReserveReadResponse {
            owner_bytes: Some(vec![0x0a, 0x0b]),
            error: None,
        });
        assert_eq!(decoded.owner_bytes, Some(vec![0x0a, 0x0b]));
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_reserve_read_response_error() {
        let decoded = roundtrip_reserve_resp(ReserveReadResponse {
            owner_bytes: None,
            error: Some(TypedClusterError::Internal {
                code: 0,
                message: "reserve-read not configured".into(),
            }),
        });
        assert!(decoded.owner_bytes.is_none());
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, 0);
                assert!(message.contains("reserve-read"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_release_reservation_request() {
        let req = ReleaseReservationRequest {
            owner_bytes: vec![0x01, 0x02],
            vshard: 2,
            reason_bytes: vec![0x03],
            deadline_remaining_ms: 5_000,
            trace_id: [3u8; 16],
        };
        let decoded = roundtrip_release_req(req);
        assert_eq!(decoded.owner_bytes, vec![0x01, 0x02]);
        assert_eq!(decoded.vshard, 2);
        assert_eq!(decoded.reason_bytes, vec![0x03]);
        assert_eq!(decoded.deadline_remaining_ms, 5_000);
        assert_eq!(decoded.trace_id, [3u8; 16]);
    }

    #[test]
    fn roundtrip_release_reservation_response_ok() {
        let decoded = roundtrip_release_resp(ReleaseReservationResponse { error: None });
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_release_reservation_response_error() {
        let decoded = roundtrip_release_resp(ReleaseReservationResponse {
            error: Some(TypedClusterError::Internal {
                code: 0,
                message: "release-reservation not configured".into(),
            }),
        });
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, 0);
                assert!(message.contains("release-reservation"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }
}
