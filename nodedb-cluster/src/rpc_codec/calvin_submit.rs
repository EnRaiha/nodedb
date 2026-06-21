// SPDX-License-Identifier: BUSL-1.1

//! Routed Calvin-submit one-shot RPC (Cv1).
//!
//! A coordinator that wants to run a cross-shard Calvin transaction must submit
//! the `TxClass` on the SEQUENCER-GROUP LEADER: only the leader's sequencer
//! service assigns transactions, and only the leader's local
//! `CalvinCompletionRegistry` receives BOTH the `note_assigned` (from the
//! service) and the replicated `note_completion_ack` (applied on every member).
//! A non-leader coordinator therefore cannot submit-and-await locally — its
//! inbox is drained and discarded, and its registry never sees the assignment.
//!
//! When this coordinator is NOT the sequencer leader it sends a
//! [`SubmitCalvinTxnRequest`] (carrying the msgpack-encoded `TxClass`) to the
//! leader; the leader runs the submit-and-await locally and replies with exactly
//! one [`SubmitCalvinTxnResponse`]. One-shot request/response; no streaming.
//!
//! Discriminants 36/37 are permanently assigned to these variants.

use super::discriminants::*;
use super::execute::TypedClusterError;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

// ── Wire types ──────────────────────────────────────────────────────────────

/// Coordinator → sequencer-leader routed Calvin-submit request (Cv1).
///
/// Carries the `TxClass` as opaque msgpack bytes (`tx_class_bytes`); the leader
/// decodes it, submits it to its local sequencer inbox, and awaits assignment +
/// completion before replying. The `deadline_remaining_ms` / `trace_id` fields
/// mirror [`AssignSurrogateRequest`](super::surrogate::AssignSurrogateRequest)
/// so the leader-side handler shares the same deadline / tracing prologue as the
/// other one-shot RPCs; the leader bounds its submit-and-await by
/// `deadline_remaining_ms`.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SubmitCalvinTxnRequest {
    /// The `TxClass` to submit, encoded with `zerompk::to_msgpack_vec`.
    pub tx_class_bytes: Vec<u8>,
    /// Deadline budget remaining for the submit-and-await on the leader (ms).
    pub deadline_remaining_ms: u64,
    pub trace_id: [u8; 16],
}

/// Terminal reply to a [`SubmitCalvinTxnRequest`].
///
/// `error: None` means the leader submitted the transaction and observed its
/// completion ack (the cross-shard write committed). `error: Some(e)` means the
/// leader-side submit-and-await failed (sequencer rejected the txn, assignment /
/// completion timed out, a channel closed, the `TxClass` failed to decode, or no
/// Calvin-submit hook is configured).
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SubmitCalvinTxnResponse {
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

pub(super) fn encode_submit_calvin_txn_req(
    msg: &SubmitCalvinTxnRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_SUBMIT_CALVIN_TXN_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_submit_calvin_txn_resp(
    msg: &SubmitCalvinTxnResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_SUBMIT_CALVIN_TXN_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_submit_calvin_txn_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::SubmitCalvinTxnRequest(from_bytes!(
        payload,
        SubmitCalvinTxnRequest,
        "SubmitCalvinTxnRequest"
    )?))
}
pub(super) fn decode_submit_calvin_txn_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::SubmitCalvinTxnResponse(from_bytes!(
        payload,
        SubmitCalvinTxnResponse,
        "SubmitCalvinTxnResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_req(req: SubmitCalvinTxnRequest) -> SubmitCalvinTxnRequest {
        let rpc = RaftRpc::SubmitCalvinTxnRequest(req);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::SubmitCalvinTxnRequest(r) => r,
            other => panic!("expected SubmitCalvinTxnRequest, got {other:?}"),
        }
    }

    fn roundtrip_resp(resp: SubmitCalvinTxnResponse) -> SubmitCalvinTxnResponse {
        let rpc = RaftRpc::SubmitCalvinTxnResponse(resp);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::SubmitCalvinTxnResponse(r) => r,
            other => panic!("expected SubmitCalvinTxnResponse, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_submit_calvin_txn_request() {
        let req = SubmitCalvinTxnRequest {
            tx_class_bytes: vec![0x01, 0x02, 0x03, 0x04],
            deadline_remaining_ms: 30_000,
            trace_id: [7u8; 16],
        };
        let decoded = roundtrip_req(req);
        assert_eq!(decoded.tx_class_bytes, vec![0x01, 0x02, 0x03, 0x04]);
        assert_eq!(decoded.deadline_remaining_ms, 30_000);
        assert_eq!(decoded.trace_id, [7u8; 16]);
    }

    #[test]
    fn roundtrip_submit_calvin_txn_request_empty() {
        let req = SubmitCalvinTxnRequest {
            tx_class_bytes: vec![],
            deadline_remaining_ms: 0,
            trace_id: [0u8; 16],
        };
        let decoded = roundtrip_req(req);
        assert!(decoded.tx_class_bytes.is_empty());
        assert_eq!(decoded.deadline_remaining_ms, 0);
    }

    #[test]
    fn roundtrip_submit_calvin_txn_response_ok() {
        let decoded = roundtrip_resp(SubmitCalvinTxnResponse { error: None });
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_submit_calvin_txn_response_error() {
        let decoded = roundtrip_resp(SubmitCalvinTxnResponse {
            error: Some(TypedClusterError::Internal {
                code: 0,
                message: "calvin-submit not configured".into(),
            }),
        });
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, 0);
                assert!(message.contains("calvin-submit"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }
}
