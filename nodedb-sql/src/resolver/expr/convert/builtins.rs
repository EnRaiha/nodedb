// SPDX-License-Identifier: Apache-2.0

//! Built-in SQL constructs that parse to dedicated AST nodes and lower
//! to ordinary function calls.

use sqlparser::ast::Expr;

use crate::error::Result;
use crate::resolver::ColumnScope;
use crate::types::*;

use super::entry::convert_expr_depth;

/// TRIM([BOTH|LEADING|TRAILING] [what FROM] expr)
pub(super) fn convert_trim(
    expr: &Expr,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    Ok(SqlExpr::Function {
        name: "trim".into(),
        args: vec![convert_expr_depth(expr, depth, scope)?],
        distinct: false,
    })
}

/// CEIL(expr)
pub(super) fn convert_ceil(
    expr: &Expr,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    Ok(SqlExpr::Function {
        name: "ceil".into(),
        args: vec![convert_expr_depth(expr, depth, scope)?],
        distinct: false,
    })
}

/// FLOOR(expr)
pub(super) fn convert_floor(
    expr: &Expr,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    Ok(SqlExpr::Function {
        name: "floor".into(),
        args: vec![convert_expr_depth(expr, depth, scope)?],
        distinct: false,
    })
}

/// SUBSTRING(expr FROM start FOR len)
pub(super) fn convert_substring(
    expr: &Expr,
    substring_from: Option<&Expr>,
    substring_for: Option<&Expr>,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    let mut args = vec![convert_expr_depth(expr, depth, scope)?];
    if let Some(from) = substring_from {
        args.push(convert_expr_depth(from, depth, scope)?);
    }
    if let Some(len) = substring_for {
        args.push(convert_expr_depth(len, depth, scope)?);
    }
    Ok(SqlExpr::Function {
        name: "substring".into(),
        args,
        distinct: false,
    })
}
