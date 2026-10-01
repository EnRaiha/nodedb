// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructor for the protocol-neutral DSL handlers.

use super::super::super::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// The SQLSTATE and message reach the client unchanged.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}
