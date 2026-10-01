// SPDX-License-Identifier: BUSL-1.1

//! VShardEnvelope RPC glue.
//!
//! The VShardEnvelope carries graph BSP, timeseries scatter-gather, migration,
//! retention, and archival messages. The inner VShardMessageType determines
//! the handler. The envelope bytes are passed through raw (already serialized
//! in their own binary format).
//!
//! A handler that fails answers with a [`VShardRefusal`] frame instead of a
//! response envelope. The frame carries the handler's typed error.

use super::discriminants::{RPC_VSHARD_ENVELOPE, RPC_VSHARD_REFUSAL};
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use super::shard_error::ShardErrorWire;
use crate::error::{ClusterError, Result};

/// The typed error that answers a VShardEnvelope request. The caller
/// rebuilds it with `ClusterError::from`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct VShardRefusal {
    pub error: ShardErrorWire,
}

impl From<ClusterError> for VShardRefusal {
    fn from(error: ClusterError) -> Self {
        Self {
            error: error.into(),
        }
    }
}

pub(super) fn encode_vshard_envelope(bytes: &[u8], out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_VSHARD_ENVELOPE, bytes, out)
}

pub(super) fn decode_vshard_envelope(payload: &[u8]) -> Result<RaftRpc> {
    // VShardEnvelope is already in its own binary format — pass through raw.
    Ok(RaftRpc::VShardEnvelope(payload.to_vec()))
}

pub(super) fn encode_vshard_refusal(msg: &VShardRefusal, out: &mut Vec<u8>) -> Result<()> {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(msg).map_err(|e| ClusterError::Codec {
        detail: format!("rkyv serialize VShardRefusal: {e}"),
    })?;
    write_frame(RPC_VSHARD_REFUSAL, &bytes, out)
}

pub(super) fn decode_vshard_refusal(payload: &[u8]) -> Result<RaftRpc> {
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    let refusal =
        rkyv::from_bytes::<VShardRefusal, rkyv::rancor::Error>(&aligned).map_err(|e| {
            ClusterError::Codec {
                detail: format!("rkyv deserialize VShardRefusal: {e}"),
            }
        })?;
    Ok(RaftRpc::VShardRefusal(refusal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_epoch::ClusterEpochState;
    use crate::rpc_codec::DataPlaneErrorCode;
    use crate::rpc_codec::{decode, encode};

    /// Encode a shard error as a refusal frame, decode it, and rebuild it.
    fn round_trip(error: ClusterError) -> ClusterError {
        let epoch = ClusterEpochState::default();
        let rpc = RaftRpc::VShardRefusal(VShardRefusal::from(error));
        let encoded = encode(&rpc, &epoch).expect("encode");
        match decode(&encoded, &epoch).expect("decode") {
            RaftRpc::VShardRefusal(refusal) => ClusterError::from(refusal.error),
            other => panic!("decoded the wrong variant: {other:?}"),
        }
    }

    #[test]
    fn a_data_plane_refusal_survives_the_wire() {
        let code = DataPlaneErrorCode::Unsupported {
            detail: "not on this engine".into(),
        };
        match round_trip(ClusterError::DataPlane { code: code.clone() }) {
            ClusterError::DataPlane { code: rebuilt } => assert_eq!(rebuilt, code),
            other => panic!("expected the typed refusal, got {other:?}"),
        }
    }

    /// `WrongOwner` crosses typed, so the coordinator's reroute retry sees it.
    #[test]
    fn wrong_owner_survives_the_wire() {
        let error = ClusterError::WrongOwner {
            vshard_id: 7,
            expected_owner_node: Some(3),
        };
        assert!(matches!(
            round_trip(error),
            ClusterError::WrongOwner {
                vshard_id: 7,
                expected_owner_node: Some(3)
            }
        ));
    }

    #[test]
    fn a_raft_redirect_keeps_its_leader_hint() {
        let error = ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
            leader_hint: Some(5),
            term: 4,
        });
        assert!(matches!(
            round_trip(error),
            ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint: Some(5),
                term: 4,
            })
        ));
    }

    #[test]
    fn a_codec_error_survives_the_wire() {
        let error = ClusterError::Codec {
            detail: "bad request body".into(),
        };
        match round_trip(error) {
            ClusterError::Codec { detail } => assert_eq!(detail, "bad request body"),
            other => panic!("expected the codec error, got {other:?}"),
        }
    }

    /// An error with no wire mirror keeps its message.
    #[test]
    fn an_untyped_error_keeps_its_message() {
        let error =
            ClusterError::BspBarrier(crate::distributed_graph::BspBarrierError::Incomplete {
                algorithm: "pagerank".into(),
                iteration: 3,
                acked: 1,
                expected: 2,
            });
        let message = error.to_string();
        match round_trip(error) {
            ClusterError::RemoteUntyped { detail } => assert_eq!(detail, message),
            other => panic!("expected the untyped error, got {other:?}"),
        }
    }

    /// A shard's typed execution error crosses the wire with its typed form.
    #[test]
    fn a_shard_execution_error_survives_the_wire() {
        let typed = crate::rpc_codec::TypedClusterError::Internal {
            code: 2000,
            message: "permission denied on orders".into(),
        };
        let error = ClusterError::ShardExecution {
            error: Box::new(typed),
            detail: "array put: permission denied on orders".into(),
        };
        match round_trip(error) {
            ClusterError::ShardExecution { error, detail } => {
                assert!(matches!(
                    *error,
                    crate::rpc_codec::TypedClusterError::Internal { code: 2000, .. }
                ));
                assert_eq!(detail, "array put: permission denied on orders");
            }
            other => panic!("expected the shard execution error, got {other:?}"),
        }
    }
}
