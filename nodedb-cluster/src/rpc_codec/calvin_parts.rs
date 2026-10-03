// SPDX-License-Identifier: BUSL-1.1

//! Streamed parts of a multi-part Calvin transaction, coordinator to
//! sequencer leader.
//!
//! A coordinator submits a multi-part transaction's header first. It then
//! sends its parts in order, a bounded batch per [`CalvinPartsRequest`], to
//! the leader that took the header. The leader queues them and answers with
//! one [`CalvinPartsResponse`]: how far the stream got, and whether to go on,
//! wait for room, or stop. One request/response per batch.
//!
//! Discriminants 54/55 are permanently assigned to these variants.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// Plan bytes one request carries at most: a quarter of the 64 MiB RPC
/// payload limit, so a batch never nears it whatever its framing costs.
pub const MAX_PARTS_BATCH_BYTES: usize = 16 << 20;

/// One batch of streamed parts.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct CalvinPartsRequest {
    /// The stream's coordinator node.
    pub stream_node: u64,
    /// The stream's sequence on its coordinator.
    pub stream_seq: u64,
    /// The parts, as a msgpack-encoded `Vec<StreamedPart>` in index order.
    pub parts_bytes: Vec<u8>,
}

/// The leader's answer to a [`CalvinPartsRequest`].
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct CalvinPartsResponse {
    /// A `PartsOfferStatus` wire code.
    pub status: u8,
    /// The index of the next part the stream owes.
    pub next_index: u32,
    /// Why the leader rejected a part or could not take the batch.
    pub detail: Option<String>,
}

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

pub(super) fn encode_calvin_parts_req(msg: &CalvinPartsRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_CALVIN_PARTS_REQ, &to_bytes!(msg)?, out)
}

pub(super) fn encode_calvin_parts_resp(msg: &CalvinPartsResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_CALVIN_PARTS_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_calvin_parts_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::CalvinPartsRequest(from_bytes!(
        payload,
        CalvinPartsRequest,
        "CalvinPartsRequest"
    )?))
}

pub(super) fn decode_calvin_parts_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::CalvinPartsResponse(from_bytes!(
        payload,
        CalvinPartsResponse,
        "CalvinPartsResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(rpc: RaftRpc) -> RaftRpc {
        let epoch = crate::cluster_epoch::ClusterEpochState::default();
        let encoded = super::super::encode(&rpc, &epoch).expect("encode");
        super::super::decode(&encoded, &epoch).expect("decode")
    }

    #[test]
    fn a_parts_request_and_response_round_trip() {
        let RaftRpc::CalvinPartsRequest(req) =
            roundtrip(RaftRpc::CalvinPartsRequest(CalvinPartsRequest {
                stream_node: 3,
                stream_seq: 77,
                parts_bytes: vec![1, 2, 3],
            }))
        else {
            panic!("expected a parts request");
        };
        assert_eq!((req.stream_node, req.stream_seq), (3, 77));
        assert_eq!(req.parts_bytes, vec![1, 2, 3]);

        let RaftRpc::CalvinPartsResponse(resp) =
            roundtrip(RaftRpc::CalvinPartsResponse(CalvinPartsResponse {
                status: 1,
                next_index: 12,
                detail: Some("full".into()),
            }))
        else {
            panic!("expected a parts response");
        };
        assert_eq!((resp.status, resp.next_index), (1, 12));
        assert_eq!(resp.detail.as_deref(), Some("full"));
    }
}
