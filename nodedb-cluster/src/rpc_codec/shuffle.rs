// SPDX-License-Identifier: BUSL-1.1

//! `ShufflePush` streaming RPC — cross-node streaming shuffle (E1).
//!
//! A producer opens one bidi stream per target partition and writes a
//! `ShufflePushRequest` frame, then a sequence of `ShufflePushChunk` frames
//! (each a standalone msgpack array of rows, mirroring `ExecuteStreamChunk`),
//! terminated by exactly one `ShufflePushEnd` frame. Unlike `ExecuteStream`
//! the direction is producer → receiver: the chunks travel on the *send*
//! half and the receiver does not write chunks back.
//!
//! Discriminants 25/26/27 are permanently assigned to these variants.

use super::discriminants::*;
use super::execute::TypedClusterError;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

// ── Wire types ──────────────────────────────────────────────────────────────

/// Opening frame of a shuffle push stream.
///
/// Carries the routing key `(shuffle_id, part, side)` plus the partition fan-out
/// (`num_parts`) and the number of producers (`producer_count`) the receiver
/// must see an `End` from before the per-part build barrier is complete.
///
/// `side` is `0` for the build side and `1` for the probe side of a hash join.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShufflePushRequest {
    pub shuffle_id: u64,
    pub part: u32,
    /// `0` = build side, `1` = probe side.
    pub side: u8,
    pub num_parts: u32,
    pub producer_count: u32,
}

/// One streamed chunk of a shuffle push stream.
///
/// `payload` is a standalone msgpack array of row elements — the same
/// convention as [`ExecuteStreamChunk`](super::execute::ExecuteStreamChunk)
/// and `RowBatch.payload`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShufflePushChunk {
    pub payload: Vec<u8>,
}

/// Terminal frame of a shuffle push stream.
///
/// `error: None` is a clean EOF (all chunks delivered for this producer).
/// `error: Some(e)` is a terminal failure — any chunks already delivered are
/// valid, but this producer's contribution is incomplete and the receiver must
/// surface the error. Mirrors
/// [`ExecuteStreamEnd`](super::execute::ExecuteStreamEnd).
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShufflePushEnd {
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

pub(super) fn encode_shuffle_push_req(msg: &ShufflePushRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_SHUFFLE_PUSH_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_shuffle_push_chunk(msg: &ShufflePushChunk, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_SHUFFLE_PUSH_CHUNK, &to_bytes!(msg)?, out)
}
pub(super) fn encode_shuffle_push_end(msg: &ShufflePushEnd, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_SHUFFLE_PUSH_END, &to_bytes!(msg)?, out)
}

pub(super) fn decode_shuffle_push_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShufflePushRequest(from_bytes!(
        payload,
        ShufflePushRequest,
        "ShufflePushRequest"
    )?))
}
pub(super) fn decode_shuffle_push_chunk(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShufflePushChunk(from_bytes!(
        payload,
        ShufflePushChunk,
        "ShufflePushChunk"
    )?))
}
pub(super) fn decode_shuffle_push_end(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShufflePushEnd(from_bytes!(
        payload,
        ShufflePushEnd,
        "ShufflePushEnd"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_req(req: ShufflePushRequest) -> ShufflePushRequest {
        let rpc = RaftRpc::ShufflePushRequest(req);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ShufflePushRequest(r) => r,
            other => panic!("expected ShufflePushRequest, got {other:?}"),
        }
    }

    fn roundtrip_chunk(chunk: ShufflePushChunk) -> ShufflePushChunk {
        let rpc = RaftRpc::ShufflePushChunk(chunk);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ShufflePushChunk(c) => c,
            other => panic!("expected ShufflePushChunk, got {other:?}"),
        }
    }

    fn roundtrip_end(end: ShufflePushEnd) -> ShufflePushEnd {
        let rpc = RaftRpc::ShufflePushEnd(end);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ShufflePushEnd(e) => e,
            other => panic!("expected ShufflePushEnd, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_shuffle_push_request() {
        let req = ShufflePushRequest {
            shuffle_id: 0xDEAD_BEEF_1234_5678,
            part: 7,
            side: 1,
            num_parts: 16,
            producer_count: 3,
        };
        let decoded = roundtrip_req(req.clone());
        assert_eq!(decoded.shuffle_id, req.shuffle_id);
        assert_eq!(decoded.part, 7);
        assert_eq!(decoded.side, 1);
        assert_eq!(decoded.num_parts, 16);
        assert_eq!(decoded.producer_count, 3);
    }

    #[test]
    fn roundtrip_shuffle_push_request_build_side() {
        let req = ShufflePushRequest {
            shuffle_id: 1,
            part: 0,
            side: 0,
            num_parts: 1,
            producer_count: 1,
        };
        let decoded = roundtrip_req(req);
        assert_eq!(decoded.side, 0);
        assert_eq!(decoded.num_parts, 1);
        assert_eq!(decoded.producer_count, 1);
    }

    #[test]
    fn roundtrip_shuffle_push_chunk_payload() {
        let chunk = ShufflePushChunk {
            payload: vec![0x93, 0x01, 0x02, 0x03],
        };
        let decoded = roundtrip_chunk(chunk.clone());
        assert_eq!(decoded.payload, chunk.payload);
    }

    #[test]
    fn roundtrip_shuffle_push_chunk_empty_payload() {
        let decoded = roundtrip_chunk(ShufflePushChunk { payload: vec![] });
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn roundtrip_shuffle_push_end_clean_eof() {
        let decoded = roundtrip_end(ShufflePushEnd { error: None });
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_shuffle_push_end_terminal_error() {
        let decoded = roundtrip_end(ShufflePushEnd {
            error: Some(TypedClusterError::Internal {
                code: 0xABCD,
                message: "shuffle producer failed mid-flight".into(),
            }),
        });
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, 0xABCD);
                assert!(message.contains("shuffle producer"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }
}
