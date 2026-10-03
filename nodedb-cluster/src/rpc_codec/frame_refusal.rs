// SPDX-License-Identifier: BUSL-1.1

//! FrameRefusal wire type and codec: the answer to a request frame the
//! receiver's replay window refused.
//!
//! The frame's MAC verified, so the sender is who it says. Its sequence
//! number was below the receiver's window or seen before. The receiver
//! answers on the same stream instead of dropping it: the sender learns the
//! frame was refused, not that the link failed. It retries under a fresh
//! sequence number, and its circuit breaker does not count the refusal
//! against the peer's health.

use super::discriminants::RPC_FRAME_REFUSAL;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// A request frame the receiver refused before it read the request.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct FrameRefusal {
    /// Why the receiver refused the frame, as its replay window put it.
    pub detail: String,
}

pub(super) fn encode_frame_refusal(msg: &FrameRefusal, out: &mut Vec<u8>) -> Result<()> {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(msg)
        .map(|b| b.to_vec())
        .map_err(|e| ClusterError::Codec {
            detail: format!("rkyv serialize: {e}"),
        })?;
    write_frame(RPC_FRAME_REFUSAL, &bytes, out)
}

pub(super) fn decode_frame_refusal(payload: &[u8]) -> Result<RaftRpc> {
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    let refusal = rkyv::from_bytes::<FrameRefusal, rkyv::rancor::Error>(&aligned).map_err(|e| {
        ClusterError::Codec {
            detail: format!("rkyv deserialize FrameRefusal: {e}"),
        }
    })?;
    Ok(RaftRpc::FrameRefused(refusal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_epoch::ClusterEpochState;
    use crate::rpc_codec::{decode, encode};

    #[test]
    fn a_refusal_survives_the_wire() {
        let epoch = ClusterEpochState::default();
        let refusal = FrameRefusal {
            detail: "peer 1 sent stale sequence 9, window high is 5000".into(),
        };
        let bytes = encode(&RaftRpc::FrameRefused(refusal.clone()), &epoch).expect("encode");
        match decode(&bytes, &epoch).expect("decode") {
            RaftRpc::FrameRefused(back) => assert_eq!(back, refusal),
            other => panic!("decoded the wrong variant: {other:?}"),
        }
    }
}
