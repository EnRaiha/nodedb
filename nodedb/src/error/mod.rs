// SPDX-License-Identifier: BUSL-1.1

//! Crate-central internal error type (`crate::Error`) and its `Result` alias.
//!
//! `types` owns the `Error` enum itself — one `thiserror` sum type spanning
//! every subsystem (write path, read path, routing, client input,
//! infrastructure) — plus the `Result<T>` alias built on it. `conversions`
//! owns `From` impls that turn external-crate error types into `Error`.
//!
//! Conversions into the crate's *public* error type (`NodeDbError`) and
//! cluster wire-error conversions live in `crate::error_from` rather than
//! here — that module already isolates the "how does an internal `Error`
//! present at the API boundary" concern from "what does an internal `Error`
//! look like" — with the `Error` to `NodeDbError` mapping table itself in
//! `crate::error_classify`, which borrows so the wire paths that only hold a
//! `&Error` classify through the same table.

mod conversions;
mod types;

pub use types::{Error, Result};
