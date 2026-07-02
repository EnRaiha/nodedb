// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructor for the protocol-neutral maintenance handlers.

use super::super::super::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// Preserves the exact SQLSTATE / message the pgwire maintenance handlers
/// produced (via `ErrorInfo::new` / `sqlstate_error`), so error parity stays
/// byte-identical after the migration off the pgwire router.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}
