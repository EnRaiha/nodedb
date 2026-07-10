// SPDX-License-Identifier: Apache-2.0

//! Error types for the CRDT engine.

/// Errors produced by CRDT operations.
#[derive(Debug, thiserror::Error)]
pub enum CrdtError {
    /// A constraint was violated during validation.
    #[error("constraint violation: {constraint} on collection `{collection}`: {detail}")]
    ConstraintViolation {
        constraint: String,
        collection: String,
        detail: String,
    },

    /// The delta could not be applied to the current state.
    #[error("delta application failed: {0}")]
    DeltaApplyFailed(String),

    /// Loro internal error.
    #[error("loro error: {0}")]
    Loro(String),

    /// Dead-letter queue is full.
    #[error("dead-letter queue full: capacity {capacity}, pending {pending}")]
    DlqFull { capacity: usize, pending: usize },

    /// The collection does not exist.
    #[error("unknown collection: {0}")]
    UnknownCollection(String),

    /// A scalar field write targets a key already held by a nested CRDT
    /// container (e.g. a row's block list).
    ///
    /// Overwriting would destroy the container's identity and every
    /// concurrent edit converging on it; skipping would silently discard the
    /// caller's write. Neither is acceptable, so the write is rejected.
    #[error(
        "field `{field}` on row `{row_id}` in collection `{collection}` is a nested CRDT container \
         and cannot be overwritten by a scalar value"
    )]
    ScalarFieldShadowsContainer {
        collection: String,
        row_id: String,
        field: String,
    },

    /// Auth context has expired — agent must re-authenticate before syncing.
    #[error("auth expired: user {user_id} must re-authenticate (expired at {expired_at})")]
    AuthExpired { user_id: u64, expired_at: u64 },

    /// Delta signature verification failed.
    #[error("delta signature invalid for user {user_id}: {detail}")]
    InvalidSignature { user_id: u64, detail: String },

    /// Replay attack detected: seq_no already seen for this (user_id, device_id).
    ///
    /// The submitted `seq_no` is not strictly greater than the last accepted
    /// sequence number, indicating a replayed or out-of-order delta.
    #[error(
        "replay detected for user {user_id} device {device_id}: \
         seq_no {seq_no} <= last_seen {last_seen}"
    )]
    ReplayDetected {
        user_id: u64,
        device_id: u64,
        seq_no: u64,
        last_seen: u64,
    },
}

pub type Result<T> = std::result::Result<T, CrdtError>;
