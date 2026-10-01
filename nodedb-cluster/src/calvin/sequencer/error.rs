// SPDX-License-Identifier: BUSL-1.1

//! Sequencer-specific error types.
//!
//! [`SequencerError`] is a typed error for all failure modes observed by
//! the sequencer layer. It wraps into [`crate::error::CalvinError`] via a
//! `From` impl on that enum.

use thiserror::Error;

/// Errors produced by the Calvin sequencer service.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SequencerError {
    /// The submitted transaction conflicts with a transaction that was admitted
    /// earlier in the same epoch. The earlier txn holds the conflicting key;
    /// the caller should retry with fresh state.
    ///
    /// `position_admitted` is the zero-based position of the winning (admitted)
    /// transaction within the candidate batch.
    #[error(
        "transaction conflicts with admitted txn at batch position {position_admitted}; \
         retry with fresh state"
    )]
    Conflict { position_admitted: u32 },

    /// The sequencer inbox is full. Back off and retry.
    #[error("sequencer inbox is full; retry after back-off")]
    Overloaded,

    /// The sequencer is not reachable (e.g. Raft group has no leader yet).
    /// Retry after a brief back-off; the cluster is in election.
    #[error("sequencer is unavailable; retry after election completes")]
    Unavailable,

    /// This node is not the sequencer leader. Submissions must be directed
    /// to the leader.
    ///
    /// `hint` carries the node_id of the current leader when the receiving
    /// node knows it; `None` during election windows.
    #[error("not the sequencer leader{}", hint.map(|h| format!("; current leader may be node {h}")).unwrap_or_default())]
    NotLeader { hint: Option<u64> },

    /// The per-vshard output sender is gone. This means the scheduler task
    /// for `vshard` exited unexpectedly; the apply loop logs and drops the
    /// transaction rather than blocking.
    #[error("vshard {vshard} output sender is gone; scheduler may have exited")]
    VshardSenderGone { vshard: u32 },

    /// The transaction's `plans` blob exceeds the per-transaction byte cap.
    ///
    /// The caller must decompose the transaction or reduce payload size.
    #[error("transaction plans blob is {bytes} bytes, exceeds limit of {limit} bytes")]
    TxnTooLarge { bytes: usize, limit: usize },

    /// A single-entry transaction or one part targets more vShards than one
    /// sequencer entry carries.
    ///
    /// The coordinator must split the plans into parts that fit.
    #[error("transaction entry targets {vshards} vshards, exceeds limit of {limit}")]
    FanoutTooWide { vshards: usize, limit: usize },

    /// A multi-part transaction's manifest or one of its streamed parts is
    /// malformed.
    #[error("multi-part transaction is malformed: {detail}")]
    MalformedParts { detail: String },

    /// The submitting tenant already has `in_flight >= quota` transactions
    /// in the inbox. Back off and retry.
    #[error("tenant {tenant} inbox quota exceeded: {in_flight} in flight, quota {quota}")]
    TenantQuotaExceeded {
        tenant: u64,
        quota: usize,
        in_flight: usize,
    },

    /// The dependent-read transaction's declared read payload would exceed
    /// the per-transaction byte cap.
    ///
    /// Each passive read result is Raft-replicated; large dependent reads
    /// stall the per-vshard Raft log. The caller must reduce the declared
    /// read set or split the transaction.
    #[error("dependent-read payload is {bytes} bytes, exceeds per-txn limit of {limit} bytes")]
    DependentReadTooLarge { bytes: usize, limit: usize },

    /// The dependent-read transaction touches more passive vshards than the
    /// configured fan-in cap.
    ///
    /// The caller must reduce the number of passive participants.
    #[error("dependent-read txn touches {passives} passive vshards, exceeds limit of {limit}")]
    DependentReadFanoutTooWide { passives: usize, limit: usize },
}
