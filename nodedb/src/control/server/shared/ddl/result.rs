// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL dispatch result types.
//!
//! These carry no pgwire wire types, so every server entrypoint (native,
//! http) can encode from them without depending on the pgwire `Response`
//! representation.

use crate::control::server::response_shape::types::ShapedRows;

/// Protocol-neutral result of a DDL dispatch, encoded per-entrypoint.
#[derive(Debug, Clone)]
pub enum DdlResult {
    /// A command tag (e.g. "CREATE TABLE"), optional affected-row count.
    Status {
        command: String,
        rows_affected: Option<u64>,
    },
    /// A row-returning result (SHOW / EXPLAIN / introspection).
    Rows(ShapedRows),
    /// An empty query.
    Empty,
}

/// Protocol-neutral DDL error: ANSI SQLSTATE + message (both entrypoints
/// encode from this).
#[derive(Debug, Clone)]
pub struct DdlError {
    pub sqlstate: String,
    pub message: String,
}
