// SPDX-License-Identifier: BUSL-1.1

use thiserror::Error;

pub type Result<T> = std::result::Result<T, RaftError>;

#[derive(Debug, Error)]
pub enum RaftError {
    #[error("not leader (leader hint: {leader_hint:?})")]
    NotLeader { leader_hint: Option<u64> },

    #[error("log compacted: requested index {requested}, first available {first_available}")]
    LogCompacted {
        requested: u64,
        first_available: u64,
    },

    #[error(
        "compaction ahead of applied: requested index {requested}, last applied {last_applied}"
    )]
    CompactionAheadOfApplied { requested: u64, last_applied: u64 },

    #[error("proposal rejected: {reason}")]
    ProposalRejected { reason: String },

    #[error("invalid leadership-transfer target: {target} is not a voter peer of this group")]
    InvalidTransferTarget { target: u64 },

    #[error("leadership transfer in progress; retry the proposal after it completes or aborts")]
    LeadershipTransferInProgress,

    #[error("group {group_id} not found on this node")]
    GroupNotFound { group_id: u64 },

    #[error("transport error: {detail}")]
    Transport { detail: String },

    #[error("storage error: {detail}")]
    Storage { detail: String },

    #[error("serialization error: {detail}")]
    Serialization { detail: String },

    #[error("snapshot format error: {detail}")]
    SnapshotFormat { detail: String },

    #[error("shutdown in progress")]
    Shutdown,
}
