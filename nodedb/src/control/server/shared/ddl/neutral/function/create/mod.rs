// SPDX-License-Identifier: BUSL-1.1

//! `CREATE [OR REPLACE] FUNCTION` — split by concern.
//!
//! - [`handler`] — the `create_function` protocol-neutral entry point
//! - [`parse`]   — `CREATE FUNCTION` grammar parser
//! - [`deps`]    — dependency extraction from function bodies

pub mod deps;
pub mod handler;
pub mod parse;

#[cfg(test)]
mod tests;

pub use handler::create_function;
pub(super) use handler::emit_function_put;
// Re-export so `super::create::ParsedCreateFunction` continues to
// work for `validate.rs` and other callers.
pub use parse::ParsedCreateFunction;
