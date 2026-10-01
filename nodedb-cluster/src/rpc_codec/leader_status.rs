// SPDX-License-Identifier: BUSL-1.1

//! LeaderStatusRequest / LeaderStatusResponse wire types and codecs.
//!
//! A node asks another node which leader it knows for a Raft group. The
//! receiver answers from its own Raft state, with no quorum round: the leader
//! it knows and the term that leader leads. A node that leads answers with
//! itself. A routing hint needs no linearizable confirmation: the routing
//! table's term rules keep a stale answer from moving a hint backwards, and
//! a request sent to a node that no longer leads is redirected.
//!
//! A node that leads also answers the group's membership: its Raft voters
//! and learners. A leader changes its membership only by applying a
//! committed conf change, so its membership holds only committed changes.
//! A node that does not lead answers no membership.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// Ask a node which leader it knows for `group_id`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LeaderStatusRequest {
    pub group_id: u64,
}

/// The receiver's own view of `group_id`'s leader.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LeaderStatusResponse {
    /// The leader the receiver knows, `0` when it knows none or hosts no
    /// replica of the group.
    pub leader: u64,
    /// The term `leader` leads. `0` when `leader` is `0`.
    pub term: u64,
    /// The group's membership, when the receiver leads it. `None` from a
    /// receiver that does not lead.
    pub membership: Option<LeaderMembership>,
}

/// A leader's voters and learners of a group, sorted ascending.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LeaderMembership {
    /// Voting members, the leader included.
    pub voters: Vec<u64>,
    /// Non-voting learners.
    pub learners: Vec<u64>,
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

pub(super) fn encode_leader_status_req(msg: &LeaderStatusRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_LEADER_STATUS_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_leader_status_resp(
    msg: &LeaderStatusResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_LEADER_STATUS_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_leader_status_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::LeaderStatusRequest(from_bytes!(
        payload,
        LeaderStatusRequest,
        "LeaderStatusRequest"
    )?))
}
pub(super) fn decode_leader_status_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::LeaderStatusResponse(from_bytes!(
        payload,
        LeaderStatusResponse,
        "LeaderStatusResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_epoch::ClusterEpochState;
    use crate::rpc_codec::{decode, encode};

    fn roundtrip(rpc: RaftRpc) -> RaftRpc {
        let epoch = ClusterEpochState::default();
        let encoded = encode(&rpc, &epoch).expect("encode");
        decode(&encoded, &epoch).expect("decode")
    }

    #[test]
    fn a_request_and_a_response_survive_the_wire() {
        match roundtrip(RaftRpc::LeaderStatusRequest(LeaderStatusRequest {
            group_id: 7,
        })) {
            RaftRpc::LeaderStatusRequest(req) => assert_eq!(req.group_id, 7),
            other => panic!("decoded the wrong variant: {other:?}"),
        }
        for status in [
            LeaderStatusResponse {
                leader: 3,
                term: 9,
                membership: None,
            },
            LeaderStatusResponse {
                leader: 3,
                term: 9,
                membership: Some(LeaderMembership {
                    voters: vec![1, 3],
                    learners: vec![4],
                }),
            },
        ] {
            match roundtrip(RaftRpc::LeaderStatusResponse(status.clone())) {
                RaftRpc::LeaderStatusResponse(resp) => assert_eq!(resp, status),
                other => panic!("decoded the wrong variant: {other:?}"),
            }
        }
    }
}
