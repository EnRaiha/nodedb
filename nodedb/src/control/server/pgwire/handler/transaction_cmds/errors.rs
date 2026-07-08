// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructors for the transaction-command handlers.

use pgwire::error::{ErrorInfo, PgWireError};

/// Builds the canonical SQLSTATE 57014 error emitted when a Calvin coordinator
/// channel is closed (coordinator task dropped due to deadline expiry).  Both
/// the assignment-recv and completion-recv arms use this constructor so the
/// mapping is defined exactly once and the tests exercise the production path.
pub(super) fn calvin_cancelled_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "57014".to_owned(),
        "Calvin coordinator cancelled (deadline exceeded)".to_owned(),
    )))
}

/// Converts a batch-dispatch result into a COMMIT-time error, if any.
/// `dispatch_task_no_wal` returns `Ok(Response { status: Error, .. })` for a
/// failed batch rather than a Rust `Err` — callers must check `status`
/// explicitly or a failed sub-plan reports as COMMIT success.
pub(super) fn batch_dispatch_to_commit_error(
    result: crate::Result<crate::bridge::envelope::Response>,
) -> Result<(), PgWireError> {
    match result {
        Err(e) => {
            tracing::warn!(error = %e, "transaction batch dispatch failed");
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "40001".to_owned(),
                format!("transaction commit failed: {e}"),
            ))))
        }
        Ok(resp) if resp.status != crate::bridge::envelope::Status::Ok => {
            let code = resp.error_code.clone().unwrap_or(
                crate::bridge::envelope::ErrorCode::RejectedPrevalidation {
                    reason: "transaction commit failed".to_owned(),
                },
            );
            let (severity, sqlstate, message) =
                crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(&code);
            tracing::warn!(
                sqlstate = sqlstate,
                message = %message,
                "transaction batch reported error status"
            );
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                sqlstate.to_owned(),
                message,
            ))))
        }
        Ok(_) => Ok(()),
    }
}
