// SPDX-License-Identifier: BUSL-1.1

//! Routed-surrogate-exchange one-shot RPC (F1b).
//!
//! A coordinator planning a cross-shard graph edge needs the AUTHORITATIVE
//! global surrogate (the stable `u32` cross-engine identity) for an endpoint key
//! whose home vShard is owned by ANOTHER node. It sends an
//! [`AssignSurrogateRequest`] to that vShard's LEADER; the leader runs a LOCAL
//! `SurrogateAssigner::assign` for `(collection, pk)` — which, because the leader
//! IS the home node, yields the authoritative (first-wins, idempotent) value —
//! and replies with exactly one [`AssignSurrogateResponse`]. One-shot
//! request/response; no streaming.
//!
//! Discriminants 34/35 are permanently assigned to these variants.

use super::discriminants::*;
use super::execute::TypedClusterError;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

// ── Wire types ──────────────────────────────────────────────────────────────

/// Coordinator → leader routed-surrogate-exchange request (F1b).
///
/// Carries the endpoint key `(collection, pk)` plus the identity scope
/// (`database_id`, `tenant_id`) and routing key (`vshard_id`) the home vShard was
/// resolved from. The `deadline_remaining_ms` / `trace_id` fields mirror
/// [`ExecuteRequest`](super::execute::ExecuteRequest) so the leader-side handler
/// shares the same deadline / tracing prologue as the other one-shot RPCs.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct AssignSurrogateRequest {
    /// Home vShard the `(collection, pk)` endpoint key routes to.
    pub vshard_id: u32,
    pub database_id: u64,
    pub tenant_id: u64,
    /// Collection the surrogate is scoped to.
    pub collection: String,
    /// Primary-key bytes of the endpoint whose surrogate is being resolved.
    pub pk: Vec<u8>,
    pub deadline_remaining_ms: u64,
    pub trace_id: [u8; 16],
}

/// Terminal reply to an [`AssignSurrogateRequest`].
///
/// `error: None` carries the authoritative `surrogate` (a non-zero `u32` when the
/// leader has a wired catalog; `0` is only ever returned with an `error`).
/// `error: Some(e)` means the leader-side assign failed (lock poisoned, backend
/// error, or no assign-remote-surrogate hook configured); `surrogate` is `0` in
/// that case.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct AssignSurrogateResponse {
    /// The authoritative surrogate. `0` only on error.
    pub surrogate: u32,
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

pub(super) fn encode_assign_surrogate_req(
    msg: &AssignSurrogateRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_ASSIGN_SURROGATE_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_assign_surrogate_resp(
    msg: &AssignSurrogateResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_ASSIGN_SURROGATE_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_assign_surrogate_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AssignSurrogateRequest(from_bytes!(
        payload,
        AssignSurrogateRequest,
        "AssignSurrogateRequest"
    )?))
}
pub(super) fn decode_assign_surrogate_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AssignSurrogateResponse(from_bytes!(
        payload,
        AssignSurrogateResponse,
        "AssignSurrogateResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_req(req: AssignSurrogateRequest) -> AssignSurrogateRequest {
        let rpc = RaftRpc::AssignSurrogateRequest(req);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::AssignSurrogateRequest(r) => r,
            other => panic!("expected AssignSurrogateRequest, got {other:?}"),
        }
    }

    fn roundtrip_resp(resp: AssignSurrogateResponse) -> AssignSurrogateResponse {
        let rpc = RaftRpc::AssignSurrogateResponse(resp);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::AssignSurrogateResponse(r) => r,
            other => panic!("expected AssignSurrogateResponse, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_assign_surrogate_request() {
        let req = AssignSurrogateRequest {
            vshard_id: 512,
            database_id: 7,
            tenant_id: 42,
            collection: "people".into(),
            pk: vec![0x61, 0x6C, 0x69, 0x63, 0x65],
            deadline_remaining_ms: 5000,
            trace_id: [9u8; 16],
        };
        let decoded = roundtrip_req(req.clone());
        assert_eq!(decoded.vshard_id, 512);
        assert_eq!(decoded.database_id, 7);
        assert_eq!(decoded.tenant_id, 42);
        assert_eq!(decoded.collection, "people");
        assert_eq!(decoded.pk, vec![0x61, 0x6C, 0x69, 0x63, 0x65]);
        assert_eq!(decoded.deadline_remaining_ms, 5000);
        assert_eq!(decoded.trace_id, [9u8; 16]);
    }

    #[test]
    fn roundtrip_assign_surrogate_request_empty_pk() {
        let req = AssignSurrogateRequest {
            vshard_id: 0,
            database_id: 0,
            tenant_id: 0,
            collection: String::new(),
            pk: vec![],
            deadline_remaining_ms: 1000,
            trace_id: [0u8; 16],
        };
        let decoded = roundtrip_req(req);
        assert!(decoded.pk.is_empty());
        assert!(decoded.collection.is_empty());
        assert_eq!(decoded.deadline_remaining_ms, 1000);
    }

    #[test]
    fn roundtrip_assign_surrogate_response_ok() {
        let decoded = roundtrip_resp(AssignSurrogateResponse {
            surrogate: 12345,
            error: None,
        });
        assert_eq!(decoded.surrogate, 12345);
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_assign_surrogate_response_error() {
        let decoded = roundtrip_resp(AssignSurrogateResponse {
            surrogate: 0,
            error: Some(TypedClusterError::Internal {
                code: 0x7,
                message: "assign-remote-surrogate not configured".into(),
            }),
        });
        assert_eq!(decoded.surrogate, 0);
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, 0x7);
                assert!(message.contains("assign-remote-surrogate"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }
}
