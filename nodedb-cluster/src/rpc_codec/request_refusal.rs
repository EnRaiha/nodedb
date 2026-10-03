// SPDX-License-Identifier: BUSL-1.1

//! RequestRefusal wire type and codec: the answer to a request whose
//! handler failed.
//!
//! The request's MAC verified, so the sender is who it says and its link is
//! up. The receiver answers on the same stream instead of dropping it. The
//! sender learns that the peer refused this one request, not that the link
//! failed. Its circuit breaker counts the answer as a success, and it does
//! not resend the request.

use super::discriminants::RPC_REQUEST_REFUSAL;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use super::shard_error::ShardErrorWire;
use crate::error::{ClusterError, Result};

/// Why the receiver's handler refused a request.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum RefusalReason {
    /// The request names a Raft group this node does not host.
    GroupNotHosted { group_id: u64 },
    /// The handler failed with another error, in its typed wire form.
    ///
    /// The error is never a link failure. A handler error that is one, from
    /// the receiver's own outbound call, crosses as `Untyped`.
    Handler { error: ShardErrorWire },
}

/// A request the receiver's handler refused.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct RequestRefusal {
    pub reason: RefusalReason,
}

impl From<ClusterError> for RequestRefusal {
    fn from(error: ClusterError) -> Self {
        let reason = match error {
            ClusterError::GroupNotFound { group_id }
            | ClusterError::Raft(nodedb_raft::RaftError::GroupNotFound { group_id }) => {
                RefusalReason::GroupNotHosted { group_id }
            }
            // The receiver's own link to a third node failed. To the sender
            // that is an answer, so it must not read as its own link failing.
            link if link.is_link_failure() => RefusalReason::Handler {
                error: ShardErrorWire::Untyped {
                    detail: link.to_string(),
                },
            },
            other => RefusalReason::Handler {
                error: other.into(),
            },
        };
        Self { reason }
    }
}

impl RequestRefusal {
    /// The typed error the sender returns to its caller.
    ///
    /// `GroupNotHosted` becomes `GroupNotFound`. Neither result is a link
    /// failure, and neither is retryable.
    pub fn into_error(self) -> ClusterError {
        match self.reason {
            RefusalReason::GroupNotHosted { group_id } => ClusterError::GroupNotFound { group_id },
            RefusalReason::Handler { error } => ClusterError::from(error),
        }
    }
}

pub(super) fn encode_request_refusal(msg: &RequestRefusal, out: &mut Vec<u8>) -> Result<()> {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(msg).map_err(|e| ClusterError::Codec {
        detail: format!("rkyv serialize RequestRefusal: {e}"),
    })?;
    write_frame(RPC_REQUEST_REFUSAL, &bytes, out)
}

pub(super) fn decode_request_refusal(payload: &[u8]) -> Result<RaftRpc> {
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    let refusal =
        rkyv::from_bytes::<RequestRefusal, rkyv::rancor::Error>(&aligned).map_err(|e| {
            ClusterError::Codec {
                detail: format!("rkyv deserialize RequestRefusal: {e}"),
            }
        })?;
    Ok(RaftRpc::RequestRefused(refusal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::RetryPolicy;
    use crate::cluster_epoch::ClusterEpochState;
    use crate::rpc_codec::{decode, encode};

    /// Refuse with `error`, cross the wire, and rebuild the sender's error.
    fn round_trip(error: ClusterError) -> ClusterError {
        let epoch = ClusterEpochState::default();
        let rpc = RaftRpc::RequestRefused(RequestRefusal::from(error));
        let bytes = encode(&rpc, &epoch).expect("encode");
        match decode(&bytes, &epoch).expect("decode") {
            RaftRpc::RequestRefused(refusal) => refusal.into_error(),
            other => panic!("decoded the wrong variant: {other:?}"),
        }
    }

    #[test]
    fn an_unhosted_group_survives_the_wire() {
        let rebuilt = round_trip(ClusterError::GroupNotFound { group_id: 4 });
        assert!(matches!(
            rebuilt,
            ClusterError::GroupNotFound { group_id: 4 }
        ));
        assert!(!rebuilt.is_link_failure());
        assert!(!RetryPolicy::is_retryable(&rebuilt));
    }

    #[test]
    fn a_raft_unhosted_group_is_the_same_refusal() {
        let error = ClusterError::Raft(nodedb_raft::RaftError::GroupNotFound { group_id: 9 });
        match RequestRefusal::from(error).reason {
            RefusalReason::GroupNotHosted { group_id } => assert_eq!(group_id, 9),
            other => panic!("expected GroupNotHosted, got {other:?}"),
        }
    }

    #[test]
    fn a_handler_link_error_never_reads_as_the_senders_link() {
        for error in [
            ClusterError::Transport {
                detail: "node 3 reset the stream".into(),
            },
            ClusterError::CircuitOpen {
                node_id: 3,
                failures: 5,
            },
            ClusterError::NodeUnreachable { node_id: 3 },
        ] {
            let message = error.to_string();
            let rebuilt = round_trip(error);
            assert!(!rebuilt.is_link_failure(), "{rebuilt:?}");
            assert!(!RetryPolicy::is_retryable(&rebuilt), "{rebuilt:?}");
            match rebuilt {
                ClusterError::RemoteUntyped { detail } => assert_eq!(detail, message),
                other => panic!("expected RemoteUntyped, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_typed_handler_error_keeps_its_type() {
        let rebuilt = round_trip(ClusterError::ReadIndexNotLeader { group_id: 2 });
        assert!(matches!(
            rebuilt,
            ClusterError::ReadIndexNotLeader { group_id: 2 }
        ));
    }
}
