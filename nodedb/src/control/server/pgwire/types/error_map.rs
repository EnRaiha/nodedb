// SPDX-License-Identifier: BUSL-1.1

//! NodeDB `Error` and Data Plane `ErrorCode` to PostgreSQL SQLSTATE mapping.

use nodedb_types::error::sqlstate;
use pgwire::error::{ErrorInfo, PgWireError};

use crate::bridge::envelope::{ErrorCode, Status};

/// Create a pgwire ErrorResponse with a SQLSTATE code.
pub fn sqlstate_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.to_owned(),
    )))
}

/// Map a NodeDB `Error` to a PostgreSQL SQLSTATE code + message.
pub fn error_to_sqlstate(err: &crate::Error) -> (&'static str, &'static str, String) {
    match err {
        crate::Error::BadRequest { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        crate::Error::PlanError { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        crate::Error::CollectionNotFound { collection, .. } => (
            "ERROR",
            sqlstate::UNDEFINED_TABLE,
            format!("collection \"{collection}\" does not exist"),
        ),
        crate::Error::CollectionDeactivated { collection, .. } => (
            "ERROR",
            // UNDEFINED_TABLE is the canonical pg code; the distinct message
            // carries the UNDROP hint so client UX can surface a restore
            // button without a custom sqlstate.
            sqlstate::UNDEFINED_TABLE,
            format!(
                "collection \"{collection}\" was dropped and is within its retention \
                 window; restore it with `UNDROP COLLECTION {collection}` before \
                 it is hard-deleted"
            ),
        ),
        crate::Error::DocumentNotFound {
            collection,
            document_id,
        } => (
            "ERROR",
            sqlstate::NO_DATA,
            format!("document \"{document_id}\" not found in \"{collection}\""),
        ),
        crate::Error::RejectedConstraint { detail, .. } => {
            ("ERROR", sqlstate::UNIQUE_VIOLATION, detail.clone())
        }
        crate::Error::DeadlineExceeded { .. } => {
            ("ERROR", sqlstate::QUERY_CANCELED, err.to_string())
        }
        crate::Error::ConflictRetry { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        crate::Error::SourceFrozen { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        crate::Error::RejectedAuthz { .. } => {
            ("ERROR", sqlstate::INSUFFICIENT_PRIVILEGE, err.to_string())
        }
        crate::Error::MemoryExhausted { .. } => ("ERROR", sqlstate::OUT_OF_MEMORY, err.to_string()),
        crate::Error::Backpressure { .. } => ("ERROR", sqlstate::OUT_OF_MEMORY, err.to_string()),
        crate::Error::FanOutExceeded { .. } => {
            ("ERROR", sqlstate::STATEMENT_TOO_COMPLEX, err.to_string())
        }
        crate::Error::NoLeader { .. } => ("ERROR", sqlstate::LOCK_NOT_AVAILABLE, err.to_string()),
        // DATABASE_DROPPED (57P04) — the closest Postgres canonical code for
        // "try again later, different node". Client libraries that recognise
        // the 57P* family treat this as retryable transient unavailability,
        // which is exactly the semantics we want. The message carries the
        // hinted leader address so an operator inspecting logs can see the
        // redirect target.
        crate::Error::NotLeader { leader_addr, .. } => (
            "ERROR",
            sqlstate::DATABASE_DROPPED,
            format!("cluster in leader election; leader hint: {leader_addr}"),
        ),
        _ => ("ERROR", sqlstate::INTERNAL_ERROR, err.to_string()),
    }
}

/// Map a Data Plane `ErrorCode` to SQLSTATE.
pub fn error_code_to_sqlstate(code: &ErrorCode) -> (&'static str, &'static str, String) {
    match code {
        ErrorCode::DeadlineExceeded => (
            "ERROR",
            sqlstate::QUERY_CANCELED,
            "query cancelled due to deadline".into(),
        ),
        ErrorCode::RejectedConstraint { constraint, detail } => (
            "ERROR",
            sqlstate::UNIQUE_VIOLATION,
            if detail.is_empty() {
                format!("constraint violation: {constraint}")
            } else {
                format!("constraint violation: {constraint}: {detail}")
            },
        ),
        ErrorCode::RejectedPrevalidation { reason } => (
            "ERROR",
            sqlstate::CHECK_VIOLATION,
            format!("pre-validation rejected: {reason}"),
        ),
        ErrorCode::NotFound => ("ERROR", sqlstate::NO_DATA, "not found".into()),
        ErrorCode::RejectedAuthz => (
            "ERROR",
            sqlstate::INSUFFICIENT_PRIVILEGE,
            "authorization denied".into(),
        ),
        ErrorCode::ConflictRetry => (
            "ERROR",
            sqlstate::SERIALIZATION_FAILURE,
            "write conflict, retry".into(),
        ),
        ErrorCode::FanOutExceeded => (
            "ERROR",
            sqlstate::STATEMENT_TOO_COMPLEX,
            "fan-out limit exceeded".into(),
        ),
        ErrorCode::ResourcesExhausted => (
            "ERROR",
            sqlstate::OUT_OF_MEMORY,
            "query result exceeded the scan memory budget; add a LIMIT clause \
             or a more selective filter, or raise \
             [tuning.query] max_scan_result_bytes"
                .into(),
        ),
        ErrorCode::RejectedDanglingEdge { missing_node } => (
            "ERROR",
            sqlstate::FOREIGN_KEY_VIOLATION,
            format!("edge rejected: node \"{missing_node}\" does not exist"),
        ),
        ErrorCode::DuplicateWrite => (
            "ERROR",
            sqlstate::UNIQUE_VIOLATION,
            "duplicate write detected via idempotency key".into(),
        ),
        ErrorCode::AppendOnlyViolation { collection } => (
            "ERROR",
            sqlstate::APPEND_ONLY_VIOLATION,
            format!("append-only violation: UPDATE/DELETE not allowed on {collection}"),
        ),
        ErrorCode::BalanceViolation { collection, detail } => (
            "ERROR",
            sqlstate::BALANCE_VIOLATION,
            format!("balance violation on {collection}: {detail}"),
        ),
        ErrorCode::PeriodLocked { collection } => (
            "ERROR",
            sqlstate::PERIOD_LOCKED,
            format!("period locked: writes rejected on {collection}"),
        ),
        ErrorCode::RetentionViolation { collection } => (
            "ERROR",
            sqlstate::RETENTION_VIOLATION,
            format!("retention violation: cannot delete from {collection}"),
        ),
        ErrorCode::LegalHoldActive { collection } => (
            "ERROR",
            sqlstate::LEGAL_HOLD_ACTIVE,
            format!("legal hold active: cannot delete from {collection}"),
        ),
        ErrorCode::StateTransitionViolation { collection, detail } => (
            "ERROR",
            sqlstate::STATE_TRANSITION_VIOLATION,
            format!("state transition violation on {collection}: {detail}"),
        ),
        ErrorCode::TransitionCheckViolation { collection } => (
            "ERROR",
            sqlstate::TRANSITION_CHECK_VIOLATION,
            format!("transition check violation on {collection}"),
        ),
        ErrorCode::TypeGuardViolation { collection, detail } => (
            "ERROR",
            sqlstate::TYPE_GUARD_VIOLATION,
            format!("type guard violation on {collection}: {detail}"),
        ),
        ErrorCode::TypeMismatch { collection, detail } => (
            "ERROR",
            sqlstate::CANNOT_COERCE,
            format!("type mismatch on {collection}: {detail}"),
        ),
        ErrorCode::OverflowError { collection } => (
            "ERROR",
            sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
            format!("arithmetic overflow on {collection}"),
        ),
        ErrorCode::InsufficientBalance { collection, detail } => (
            "ERROR",
            sqlstate::CHECK_VIOLATION,
            format!("insufficient balance on {collection}: {detail}"),
        ),
        ErrorCode::RateExceeded {
            gate,
            retry_after_ms,
        } => (
            "ERROR",
            sqlstate::STATEMENT_TOO_COMPLEX,
            format!("rate limit exceeded for {gate}, retry after {retry_after_ms}ms"),
        ),
        ErrorCode::CollectionDraining { collection } => (
            "ERROR",
            sqlstate::CANNOT_CONNECT_NOW,
            format!(
                "collection '{collection}' is draining for hard-delete; retry after purge completes"
            ),
        ),
        ErrorCode::RecursionDepthExceeded {
            cte_name,
            max_depth,
        } => (
            "ERROR",
            sqlstate::PROGRAM_LIMIT_EXCEEDED,
            format!(
                "WITH RECURSIVE CTE '{cte_name}' exceeded max recursion depth {max_depth}; \
                 add a stricter termination condition or raise max_recursion_depth"
            ),
        ),
        ErrorCode::Internal { detail } => ("ERROR", sqlstate::INTERNAL_ERROR, detail.clone()),
        ErrorCode::Unsupported { detail } => {
            ("ERROR", sqlstate::FEATURE_NOT_SUPPORTED, detail.clone())
        }
        ErrorCode::RollbackFailed {
            entry_index,
            detail,
        } => (
            "ERROR",
            sqlstate::INTERNAL_ERROR,
            format!(
                "transaction rollback failed at undo entry {entry_index}: {detail}; \
                 shard state is unknown — restart required"
            ),
        ),
        // OllpRetryRequired is an internal scheduler signal and should not
        // reach the pgwire layer as a user-visible error. If it does, surface
        // it as a serialization failure so clients retry automatically.
        ErrorCode::OllpRetryRequired => (
            "ERROR",
            sqlstate::SERIALIZATION_FAILURE,
            "optimistic predicate retry required; transaction will be retried".into(),
        ),
    }
}

/// Create a notice response (WARNING level).
pub fn notice_warning(message: &str) -> pgwire::messages::response::NoticeResponse {
    pgwire::messages::response::NoticeResponse::from(pgwire::error::ErrorInfo::new(
        "WARNING".to_owned(),
        sqlstate::WARNING.to_owned(),
        message.to_owned(),
    ))
}

/// Map a Data Plane response status + error code to a SQLSTATE triple.
pub fn response_status_to_sqlstate(
    status: Status,
    error_code: &Option<ErrorCode>,
) -> Option<(&'static str, &'static str, String)> {
    match status {
        Status::Ok | Status::Partial => None,
        Status::Error => {
            if let Some(code) = error_code {
                Some(error_code_to_sqlstate(code))
            } else {
                Some(("ERROR", "XX000", "unknown data plane error".into()))
            }
        }
    }
}
