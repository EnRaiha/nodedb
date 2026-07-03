// SPDX-License-Identifier: BUSL-1.1

//! Shared error / result constructors for the protocol-neutral database DDL
//! handlers.

use crate::control::server::response_shape::types::{DdlColType, ShapedRows};

use super::super::super::result::{DdlError, DdlResult};

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// Preserves the exact SQLSTATE / message the pgwire database handlers
/// produced (via `sqlstate_error`), so error parity stays byte-identical after
/// the migration off the pgwire router.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Build a single-element command-tag result (`rows_affected: None`), mirroring
/// the pgwire `Tag::new(command)` execution response.
pub(super) fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Build an all-text [`ShapedRows`] result. Every column produced by the pgwire
/// database SHOW handlers was a `text_field`, so `column_types` is uniformly
/// `Text`, matching the pgwire schema exactly.
pub(super) fn text_rows(
    columns: Vec<String>,
    rows: Vec<serde_json::Map<String, serde_json::Value>>,
) -> Vec<DdlResult> {
    let column_types = vec![DdlColType::Text; columns.len()];
    vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })]
}
