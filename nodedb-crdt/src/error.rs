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

    /// A generic CRDT import exceeded its encoded byte limit before metadata
    /// parsing, forking, or import.
    #[error("CRDT import exceeds byte limit: {actual} > {limit}")]
    ImportTooLarge { limit: usize, actual: usize },

    /// Authenticated Loro import metadata could not be decoded.
    #[error("CRDT import metadata malformed: {detail}")]
    ImportMalformed { detail: String },

    /// Authenticated Loro import metadata contained a regressing or
    /// unrepresentable operation range.
    #[error("CRDT import operation range is invalid")]
    ImportInvalidOperationRange,

    /// A generic CRDT import encoded or contributed more operations than its limit.
    #[error("CRDT import has too many operations: {actual} > {limit}")]
    ImportOperationLimitExceeded { limit: usize, actual: usize },

    /// The imported update depends on operations this document has never seen,
    /// so Loro buffered them as causally pending instead of applying them.
    ///
    /// The document state did NOT advance. Reporting such an import as success
    /// is silent data loss: the caller acknowledges a write that was never
    /// applied and may never be, since the missing predecessors are not part of
    /// this document's operation history.
    #[error("CRDT import depends on operations absent from this document")]
    ImportPendingDependencies,

    /// A delta preview exceeded its encoded byte limit before import.
    #[error("delta preview exceeds byte limit: {actual} > {limit}")]
    PreviewDeltaTooLarge { limit: usize, actual: usize },

    /// Preview requires a quiescent authoritative state and refuses to commit
    /// a caller's pending auto-commit transaction as a side effect of forking.
    #[error("delta preview source has {operations} pending operations")]
    PreviewSourceTransactionPending { operations: usize },

    /// A delta preview could not decode the supplied Loro update bytes.
    #[error("delta preview malformed: {detail}")]
    PreviewMalformed { detail: String },

    /// A delta preview depends on operations absent from the authoritative state.
    #[error("delta preview has pending dependencies")]
    PreviewPendingDependencies,

    /// An imported operation range was malformed or overflowed while counted.
    #[error("delta preview operation range is invalid")]
    PreviewInvalidOperationRange,

    /// A delta preview imported more operations than its explicit limit.
    #[error("delta preview imports too many operations: {actual} > {limit}")]
    PreviewOperationLimitExceeded { limit: usize, actual: usize },

    /// A delta preview wrote more rows than its explicit limit.
    #[error("delta preview writes too many rows: {actual} > {limit}")]
    PreviewWriteSetLimitExceeded { limit: usize, actual: usize },

    /// A non-idempotent delta preview did not write its declared target row.
    #[error(
        "delta preview target mismatch: expected `{expected_collection}/{expected_row}`, got `{actual_collection}/{actual_row}`"
    )]
    PreviewTargetMismatch {
        expected_collection: String,
        expected_row: String,
        actual_collection: String,
        actual_row: String,
    },

    /// A delta preview's canonical post-image exceeds its explicit byte limit.
    #[error("delta preview post-image exceeds byte limit: {actual} > {limit}")]
    PreviewPostImageTooLarge { limit: usize, actual: usize },

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
