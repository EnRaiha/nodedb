// SPDX-License-Identifier: Apache-2.0

//! GROUP BY key conversion and output naming.
//!
//! Two concerns live here: turning a parsed GROUP BY clause into grouping key
//! expressions (including resolving a key written as a SELECT-list output
//! alias), and deriving the output column name each key is emitted under.

use sqlparser::ast::{self, GroupByExpr};

use crate::error::Result;
use crate::parser::normalize::normalize_ident;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::{ColumnInfo, SqlExpr};

/// Convert GROUP BY clause to SqlExpr list.
pub fn convert_group_by(group_by: &GroupByExpr, scope: &TableScope) -> Result<Vec<SqlExpr>> {
    convert_group_by_with_projection(group_by, &[], &[], scope)
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
    scope: &TableScope,
) -> Result<Vec<SqlExpr>> {
    // A key written as an output alias is substituted for its SELECT-list
    // expression above, so only the remaining names need the alias widening.
    // `SELECT col AS col` names the column itself, so that alias is left out:
    // the key resolves to the column and, over a join, to its owning side.
    let key_scope = scope.with_output_names(
        crate::planner::select::select_output_aliases(projection)
            .into_iter()
            .filter(|alias| !is_self_alias(projection, alias)),
    );
    let key_scope = ColumnScope::Relations(&key_scope);
    match group_by {
        GroupByExpr::All(_) => Ok(Vec::new()),
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| {
                reject_ordinal_out_of_range(e, projection)?;
                match resolve_output_alias(e, projection, table_columns) {
                    Some(aliased) => convert_expr(aliased, &key_scope),
                    None => convert_expr(e, &key_scope),
                }
            })
            .collect(),
    }
}

/// Whether `alias` is the output name of a SELECT item `col AS col` that
/// projects the column of the same name, bare or qualified.
fn is_self_alias(projection: &[ast::SelectItem], alias: &str) -> bool {
    projection.iter().any(|item| match item {
        ast::SelectItem::ExprWithAlias { expr, alias: name } => {
            normalize_ident(name) == alias && expr_column_name(expr).is_some_and(|c| c == alias)
        }
        ast::SelectItem::UnnamedExpr(_)
        | ast::SelectItem::ExprWithAliases { .. }
        | ast::SelectItem::QualifiedWildcard(..)
        | ast::SelectItem::Wildcard(_) => false,
    })
}

/// Refuse `GROUP BY n` when the SELECT list has no `n`-th expression, as
/// PostgreSQL does. An integer key is never a constant group.
///
/// A projection-less call (`convert_group_by`) has no SELECT list to check
/// against and leaves the key as written.
fn reject_ordinal_out_of_range(expr: &ast::Expr, projection: &[ast::SelectItem]) -> Result<()> {
    if projection.is_empty() {
        return Ok(());
    }
    let ast::Expr::Value(v) = expr else {
        return Ok(());
    };
    let ast::Value::Number(n, _) = &v.value else {
        return Ok(());
    };
    let in_range = n
        .parse::<usize>()
        .is_ok_and(|position| position >= 1 && position <= projection.len());
    if in_range {
        return Ok(());
    }
    Err(crate::error::SqlError::Unsupported {
        detail: format!("GROUP BY position {n} is not in select list"),
    })
}

/// The SELECT-list expression a bare GROUP BY identifier refers to, if the
/// identifier names an output alias rather than an input column.
fn resolve_output_alias<'a>(
    expr: &ast::Expr,
    projection: &'a [ast::SelectItem],
    table_columns: &[ColumnInfo],
) -> Option<&'a ast::Expr> {
    // `GROUP BY 2` names the second SELECT-list expression.
    if let ast::Expr::Value(v) = expr
        && let ast::Value::Number(n, _) = &v.value
        && let Ok(position) = n.parse::<usize>()
        && position >= 1
    {
        return match projection.get(position - 1) {
            Some(
                ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. },
            ) => Some(e),
            Some(_) | None => None,
        };
    }
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
/// each key with the projection item that produces it. Returns a `Vec`
/// parallel to `group_by`: `Some(alias)` when a projection item explicitly
/// aliases that key, otherwise `None` (the output column name falls back to
/// the key's own name).
///
/// A column key matches an aliased item that references the same column
/// (`SELECT k AS label ... GROUP BY k`). A computed key matches an aliased
/// item whose expression converts to the same key
/// (`SELECT time_bucket('1 hour', ts) AS b ... GROUP BY b`).
pub fn group_by_output_aliases(
    projection: &[ast::SelectItem],
    group_by: &[SqlExpr],
    scope: &TableScope,
) -> Vec<Option<String>> {
    group_by
        .iter()
        .map(|key| match key_column_name(key) {
            Some(name) => projection_alias_for_column(projection, name),
            None => projection_alias_for_expr(projection, key, scope),
        })
        .collect()
}

/// The explicit alias of the projection item whose expression converts to
/// the computed GROUP BY key `key`.
///
/// `SqlExpr` has no `PartialEq` (it can carry a subquery plan), so the match
/// compares the `Debug` rendering. Both sides pass through `convert_expr`, so
/// equal expressions render identically. An item that does not convert names
/// no key.
fn projection_alias_for_expr(
    projection: &[ast::SelectItem],
    key: &SqlExpr,
    scope: &TableScope,
) -> Option<String> {
    let rendered_key = format!("{key:?}");
    projection.iter().find_map(|item| match item {
        ast::SelectItem::ExprWithAlias { expr, alias } => {
            convert_expr(expr, &ColumnScope::Relations(scope))
                .ok()
                .filter(|converted| format!("{converted:?}") == rendered_key)
                .map(|_| normalize_ident(alias))
        }
        ast::SelectItem::UnnamedExpr(_)
        | ast::SelectItem::ExprWithAliases { .. }
        | ast::SelectItem::QualifiedWildcard(..)
        | ast::SelectItem::Wildcard(_) => None,
    })
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
