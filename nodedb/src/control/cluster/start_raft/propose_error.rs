// SPDX-License-Identifier: BUSL-1.1

//! The error an async Raft propose returns to its statement.

use nodedb_cluster::ClusterError;
use nodedb_raft::RaftError;

use crate::types::VShardId;

/// The error an async propose returns for a cluster error.
///
/// A group with no leader to take the proposal right now accepts the same
/// proposal once it has one, so the proposal is retried:
/// [`crate::Error::NoLeader`]. That covers an election, a leadership transfer
/// in flight, a leader that stepped down after this node or a forwarding node
/// chose it, and a vShard whose owner is moving. A forwarded refusal arrives
/// here with its typed Raft error (`DataProposeResponse::refusal_error`). A
/// typed verdict keeps its class. Every other failure is final here.
pub(super) fn async_propose_error(vshard_id: u32, error: ClusterError) -> crate::Error {
    match error {
        ClusterError::Raft(
            RaftError::LeadershipTransferInProgress | RaftError::NotLeader { .. },
        )
        | ClusterError::ReadIndexNotLeader { .. }
        | ClusterError::MigrationInProgress { .. }
        | ClusterError::WrongOwner { .. } => crate::Error::NoLeader {
            vshard_id: VShardId::new(vshard_id),
        },
        // The forward did not answer before its timeout. The statement's
        // deadline class, the same class the array fan-out gives it.
        ClusterError::ShardTimeout { .. } => crate::Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        },
        ClusterError::DataPlane { code } => crate::Error::DataPlane(code.into()),
        ClusterError::ShardExecution { error, .. } | ClusterError::StreamTerminal { error, .. } => {
            crate::Error::from(*error)
        }
        other @ (ClusterError::Raft(
            RaftError::LogCompacted { .. }
            | RaftError::CompactionAheadOfApplied { .. }
            | RaftError::ProposalRejected { .. }
            | RaftError::InvalidTransferTarget { .. }
            | RaftError::GroupNotFound { .. }
            | RaftError::Transport { .. }
            | RaftError::Storage { .. }
            | RaftError::Serialization { .. }
            | RaftError::SnapshotFormat { .. }
            | RaftError::Shutdown,
        )
        | ClusterError::VShardNotMapped { .. }
        | ClusterError::GroupNotFound { .. }
        | ClusterError::LearnerNotCaughtUp { .. }
        | ClusterError::MigrationPauseBudgetExceeded { .. }
        | ClusterError::NodeUnreachable { .. }
        | ClusterError::GhostNotFound { .. }
        | ClusterError::Transport { .. }
        | ClusterError::Storage { .. }
        | ClusterError::Codec { .. }
        | ClusterError::UnsupportedWireVersion { .. }
        | ClusterError::CircuitOpen { .. }
        | ClusterError::JoinGroupDisappeared { .. }
        | ClusterError::JoinCommitTimeout { .. }
        | ClusterError::ReadIndexTimeout { .. }
        | ClusterError::Config { .. }
        | ClusterError::MigrationCheckpoint(_)
        | ClusterError::MigrationRecovery(_)
        | ClusterError::Calvin(_)
        | ClusterError::SnapshotCrcMismatch { .. }
        | ClusterError::SnapshotOffsetRegression { .. }
        | ClusterError::PartialSnapshotCorrupt { .. }
        | ClusterError::PartialSnapshotCleanupFailed { .. }
        | ClusterError::SnapshotApplyFailed { .. }
        | ClusterError::Mirror(_)
        | ClusterError::BspBarrier(_)
        | ClusterError::VectorGather(_)
        | ClusterError::SpatialGather(_)
        | ClusterError::Bm25Gather(_)
        | ClusterError::TsGather(_)
        | ClusterError::ShufflePush(_)
        | ClusterError::RemoteUntyped { .. }) => crate::Error::Internal {
            detail: format!("raft propose (async): {other}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_leader_is_retryable() {
        let error = ClusterError::Raft(RaftError::NotLeader {
            leader_hint: None,
            term: 1,
        });
        assert!(matches!(
            async_propose_error(3, error),
            crate::Error::NoLeader { .. }
        ));
    }

    /// A moving vShard has no owner to take the proposal until the
    /// cut-over, so the statement answers the retryable no-leader class.
    #[test]
    fn a_moving_vshard_is_retryable() {
        let error = ClusterError::WrongOwner {
            vshard_id: 3,
            expected_owner_node: None,
        };
        assert!(matches!(
            async_propose_error(3, error),
            crate::Error::NoLeader { .. }
        ));
    }

    #[test]
    fn a_forward_timeout_is_a_deadline() {
        let error = ClusterError::ShardTimeout {
            vshard_id: 3,
            elapsed_ms: 50,
        };
        assert!(matches!(
            async_propose_error(3, error),
            crate::Error::DeadlineExceeded { .. }
        ));
    }
}
