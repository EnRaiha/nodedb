// SPDX-License-Identifier: BUSL-1.1

//! Wire mirror of `nodedb_raft::RaftError`. Variant order is the wire ABI:
//! append only.

use nodedb_raft::RaftError;

/// A `RaftError` carried across a node hop. `NotLeader` keeps its leader
/// hint and the term the responder knew it at, so the caller can chase the
/// redirect and rank it against the hint it holds.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum RaftErrorWire {
    NotLeader {
        leader_hint: Option<u64>,
        term: u64,
    },
    LogCompacted {
        requested: u64,
        first_available: u64,
    },
    CompactionAheadOfApplied {
        requested: u64,
        last_applied: u64,
    },
    ProposalRejected {
        reason: String,
    },
    InvalidTransferTarget {
        target: u64,
    },
    LeadershipTransferInProgress,
    GroupNotFound {
        group_id: u64,
    },
    Transport {
        detail: String,
    },
    Storage {
        detail: String,
    },
    Serialization {
        detail: String,
    },
    SnapshotFormat {
        detail: String,
    },
    Shutdown,
}

impl From<RaftError> for RaftErrorWire {
    fn from(error: RaftError) -> Self {
        match error {
            RaftError::NotLeader { leader_hint, term } => Self::NotLeader { leader_hint, term },
            RaftError::LogCompacted {
                requested,
                first_available,
            } => Self::LogCompacted {
                requested,
                first_available,
            },
            RaftError::CompactionAheadOfApplied {
                requested,
                last_applied,
            } => Self::CompactionAheadOfApplied {
                requested,
                last_applied,
            },
            RaftError::ProposalRejected { reason } => Self::ProposalRejected { reason },
            RaftError::InvalidTransferTarget { target } => Self::InvalidTransferTarget { target },
            RaftError::LeadershipTransferInProgress => Self::LeadershipTransferInProgress,
            RaftError::GroupNotFound { group_id } => Self::GroupNotFound { group_id },
            RaftError::Transport { detail } => Self::Transport { detail },
            RaftError::Storage { detail } => Self::Storage { detail },
            RaftError::Serialization { detail } => Self::Serialization { detail },
            RaftError::SnapshotFormat { detail } => Self::SnapshotFormat { detail },
            RaftError::Shutdown => Self::Shutdown,
        }
    }
}

impl From<RaftErrorWire> for RaftError {
    fn from(wire: RaftErrorWire) -> Self {
        match wire {
            RaftErrorWire::NotLeader { leader_hint, term } => Self::NotLeader { leader_hint, term },
            RaftErrorWire::LogCompacted {
                requested,
                first_available,
            } => Self::LogCompacted {
                requested,
                first_available,
            },
            RaftErrorWire::CompactionAheadOfApplied {
                requested,
                last_applied,
            } => Self::CompactionAheadOfApplied {
                requested,
                last_applied,
            },
            RaftErrorWire::ProposalRejected { reason } => Self::ProposalRejected { reason },
            RaftErrorWire::InvalidTransferTarget { target } => {
                Self::InvalidTransferTarget { target }
            }
            RaftErrorWire::LeadershipTransferInProgress => Self::LeadershipTransferInProgress,
            RaftErrorWire::GroupNotFound { group_id } => Self::GroupNotFound { group_id },
            RaftErrorWire::Transport { detail } => Self::Transport { detail },
            RaftErrorWire::Storage { detail } => Self::Storage { detail },
            RaftErrorWire::Serialization { detail } => Self::Serialization { detail },
            RaftErrorWire::SnapshotFormat { detail } => Self::SnapshotFormat { detail },
            RaftErrorWire::Shutdown => Self::Shutdown,
        }
    }
}
