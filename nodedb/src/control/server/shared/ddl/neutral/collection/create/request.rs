// SPDX-License-Identifier: BUSL-1.1

//! Parsed request struct shared by `CREATE COLLECTION` and `CREATE TABLE`.
//!
//! Relocated verbatim from the pgwire
//! `pgwire::ddl::collection::create::request` module (now deleted); also used
//! by the pgwire typed-AST router (`router/ast/async_ops.rs`) before this
//! migration — that caller is gone now that `CreateCollection` / `CreateTable`
//! are handled by the neutral router directly.

/// Pre-parsed fields from the `nodedb-sql` AST for a `CREATE COLLECTION` /
/// `CREATE TABLE` statement.
///
/// - `engine`: value of `engine=` from the WITH clause (lowercased),
///   or `None` for the default.
/// - `columns`: `(name, type)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `flags`: free-standing modifier keywords: `APPEND_ONLY`,
///   `HASH_CHAIN`, `BITEMPORAL`.
/// - `balanced_raw`: raw inner content of `BALANCED ON (...)`, if present.
#[derive(Clone, Copy)]
pub struct CreateCollectionRequest<'a> {
    pub name: &'a str,
    pub engine: Option<&'a str>,
    pub columns: &'a [(String, String)],
    pub options: &'a [(String, String)],
    pub flags: &'a [String],
    pub balanced_raw: Option<&'a str>,
}
