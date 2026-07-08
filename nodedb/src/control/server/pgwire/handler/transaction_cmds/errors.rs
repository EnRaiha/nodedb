// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructors for the transaction-command handlers.

use pgwire::error::{ErrorInfo, PgWireError};

/// Builds the canonical SQLSTATE 57014 error emitted when a Calvin coordinator
/// channel is closed (coordinator task dropped due to deadline expiry).  The
/// neutral commit orchestrator surfaces this as `AbortReason::CalvinCancelled`;
/// this constructor defines the mapping in exactly one place so the tests
/// exercise the production path.
pub(super) fn calvin_cancelled_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "57014".to_owned(),
        "Calvin coordinator cancelled (deadline exceeded)".to_owned(),
    )))
}
