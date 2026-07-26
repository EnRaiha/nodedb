// SPDX-License-Identifier: Apache-2.0

//! The public entry point: parse once, run both inference passes, report one
//! slot per placeholder position.

use sqlparser::ast::{Statement, Visit};

use super::column_backed;
use super::slots::{InferenceContext, InferredParamType};
use crate::catalog::SqlCatalog;
use crate::parser::array_stmt::try_parse_array_statement;
use crate::parser::preprocess;
use crate::parser::statement::parse_sql;

/// Best-effort, catalog-aware inference of `$N` placeholder types.
///
/// Returns one slot per placeholder, indexed by `N - 1`. `None` means the
/// position is not one this pass can resolve unambiguously.
///
/// Under-inference is always safe: a PostgreSQL client that receives an
/// unknown parameter type sends the value in text format, which this server
/// already handles. Over-inference is NOT safe — reporting a concrete OID
/// makes the client commit to a binary encoding for that type, so any
/// position whose type is ambiguous MUST stay `None` rather than be guessed.
///
/// The pass never errors and never panics: unparseable SQL yields an empty
/// result, and a catalog lookup that fails leaves the positions behind it
/// unresolved.
///
/// # Resolved forms
///
/// Catalog-free:
///
/// * `LIMIT $N` / `OFFSET $N` (including the `UPDATE` / `DELETE` limit forms)
///   → [`crate::types_expr::SqlDataType::Int64`].
/// * `$N::<type>` and `CAST($N AS <type>)` → the named type.
///
/// Catalog-backed, each carrying the column's declared
/// [`nodedb_types::columnar::IntWidth`]:
///
/// * `col <cmp> $N` and `$N <cmp> col` in `WHERE` / `HAVING`.
/// * `col IN ($N, ...)` and `col BETWEEN $N AND $N`.
/// * `UPDATE t SET col = $N`.
/// * `INSERT INTO t (cols...) VALUES ($N, ...)`, including multi-row
///   `VALUES` and the positional (no column list) form.
pub fn infer_placeholder_types(
    sql: &str,
    catalog: &dyn SqlCatalog,
) -> Vec<Option<InferredParamType>> {
    let Some(statements) = parse_best_effort(sql) else {
        return Vec::new();
    };
    let mut ctx = InferenceContext::default();
    for statement in &statements {
        // `Visit` only breaks when the visitor asks it to; this one never
        // does. It sizes the slot table and resolves the catalog-free forms.
        let _ = statement.visit(&mut ctx);
        column_backed::infer_from_statement(&mut ctx, catalog, statement);
    }
    ctx.finish()
}

/// Parse `sql` the same way `plan_sql` does, but swallowing every failure.
///
/// Mirrors `plan_sql`'s front end so a statement that plans successfully is
/// also one this pass sees: NodeDB's `ARRAY` DDL/DML family bypasses
/// sqlparser entirely (and carries no placeholders), and everything else goes
/// through the preprocessor before `parse_sql`. If the preprocessor rejects
/// the input we still try the raw text, since a preprocessor-only failure
/// (e.g. an unsupported NodeDB extension) does not imply the placeholders are
/// unreadable.
fn parse_best_effort(sql: &str) -> Option<Vec<Statement>> {
    // Array statements accept no bound parameters.
    if let Ok(Some(_)) = try_parse_array_statement(sql) {
        return None;
    }
    let preprocessed = preprocess::preprocess(sql).ok().flatten();
    let effective_sql = preprocessed.as_ref().map_or(sql, |p| p.sql.as_str());
    parse_sql(effective_sql).ok()
}
