// SPDX-License-Identifier: BUSL-1.1

//! Shared error helper for the protocol-neutral constraint DDL handlers.

use crate::control::server::shared::ddl::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and message.
pub(super) fn err(code: &str, msg: &str) -> DdlError {
    DdlError::new(code, msg)
}
