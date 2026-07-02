// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructor for the protocol-neutral tree-ops handlers.

use super::super::super::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// Preserves the exact SQLSTATE / message the pgwire tree-ops handlers produced
/// (via `sqlstate_error`), so error parity stays byte-identical after the
/// migration off the pgwire router.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}
