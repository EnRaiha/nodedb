// SPDX-License-Identifier: Apache-2.0

//! GROUP BY key conversion and output naming.
//!
//! Two concerns live here: turning a parsed GROUP BY clause into grouping key
//! expressions (including resolving a key written as a SELECT-list output
//! alias), and deriving the output column name each key is emitted under.

use sqlparser::ast::{self, GroupByExpr};

use crate::error::Result;
use crate::parser::normalize::normalize_ident;
use crate::resolver::expr::convert_expr;
use crate::types::{ColumnInfo, SqlExpr};

/// Convert GROUP BY clause to SqlExpr list.
pub fn convert_group_by(group_by: &GroupByExpr) -> Result<Vec<SqlExpr>> {
    convert_group_by_with_projection(group_by, &[], &[])
}

/// Convert a GROUP BY clause, resolving SELECT-list output aliases.
///
/// `GROUP BY k` may name an output column declared in the SELECT list
/// (`SELECT 10 / denom AS k ... GROUP BY k`) — PostgreSQL resolves it to that
/// expression. Left unresolved, the key builder looks for a stored field named
/// `k`, finds none in any row, and buckets every row under one null key: the
/// query reports a single global total where the client asked for one row per
/// computed group.
///
/// PostgreSQL's precedence applies: a name that matches an actual input column
/// refers to that column, not to the output alias. `table_columns` carries the
/// declared columns when the collection has a schema; a schemaless collection
/// passes an empty slice, where any alias match is the only interpretation
/// available.
pub fn convert_group_by_with_projection(
    group_by: &GroupByExpr,
    projection: &[ast::SelectItem],
    table_columns: &[ColumnInfo],
) -> Result<Vec<SqlExpr>> {
    match group_by {
        GroupByExpr::All(_) => Ok(Vec::new()),
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(
                |e| match resolve_output_alias(e, projection, table_columns) {
                    Some(aliased) => convert_expr(aliased),
                    None => convert_expr(e),
                },
            )
            .collect(),
    }
}

/// The SELECT-list expression a bare GROUP BY identifier refers to, if the
/// identifier names an output alias rather than an input column.
fn resolve_output_alias<'a>(
    expr: &ast::Expr,
    projection: &'a [ast::SelectItem],
    table_columns: &[ColumnInfo],
) -> Option<&'a ast::Expr> {
    let ast::Expr::Identifier(ident) = expr else {
        return None;
    };
    let needle = normalize_ident(ident);

    // An input column of the same name wins over the output alias.
    if table_columns.iter().any(|c| c.name == needle) {
        return None;
    }

    projection.iter().find_map(|item| match item {
        ast::SelectItem::ExprWithAlias { expr, alias }
            if normalize_ident(alias) == needle
                // `SELECT col AS col` resolves to itself; leaving it alone
                // keeps the key a plain column reference.
                && !matches!(expr, ast::Expr::Identifier(i) if normalize_ident(i) == needle) =>
        {
            Some(expr)
        }
        _ => None,
    })
}

/// Derive the SELECT-list output alias for each GROUP BY key by correlating
/// each key with the projection item that references the same column. Returns
/// a `Vec` parallel to `group_by`: `Some(alias)` when a projection item
/// explicitly aliases that grouped column (`SELECT k AS label ... GROUP BY k`),
/// otherwise `None` (the output column name falls back to the raw grouped
/// column name).
pub fn group_by_output_aliases(
    projection: &[ast::SelectItem],
    group_by: &[SqlExpr],
) -> Vec<Option<String>> {
    group_by
        .iter()
        .map(|key| {
            key_column_name(key).and_then(|name| projection_alias_for_column(projection, name))
        })
        .collect()
}

/// The bare column name of a GROUP BY key, when the key is a column reference.
pub fn key_column_name(key: &SqlExpr) -> Option<&str> {
    match key {
        SqlExpr::Column { name, .. } => Some(name.as_str()),
        _ => None,
    }
}

/// The explicit alias of the projection item that references bare column
/// `col`, if any. Only `expr AS alias` items where `expr` is that column
/// (bare or qualified) qualify; unaliased items yield `None`.
fn projection_alias_for_column(projection: &[ast::SelectItem], col: &str) -> Option<String> {
    for item in projection {
        if let ast::SelectItem::ExprWithAlias { expr, alias } = item
            && expr_column_name(expr).is_some_and(|n| n == col)
        {
            return Some(normalize_ident(alias));
        }
    }
    None
}

/// The bare column name referenced by an `ast::Expr`, when it is a simple or
/// compound identifier; `None` for any other expression shape.
pub fn expr_column_name(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Identifier(ident) => Some(normalize_ident(ident)),
        ast::Expr::CompoundIdentifier(parts) => parts.last().map(normalize_ident),
        _ => None,
    }
}
