// SPDX-License-Identifier: BUSL-1.1

//! Shared result / error constructors for the scope DDL family.

use super::super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`], preserving the exact SQLSTATE codes and messages
/// the pgwire scope handlers produced (via `sqlstate_error`).
pub(super) fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Build a single-tag status result.
pub(super) fn status(command: impl Into<String>) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.into(),
        rows_affected: None,
    }]
}
