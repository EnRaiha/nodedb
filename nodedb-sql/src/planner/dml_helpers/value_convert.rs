// SPDX-License-Identifier: Apache-2.0

//! Conversion of parsed `VALUES`-clause expressions into planner `SqlValue`s.
//!
//! This is the single surface every DML path (`INSERT`, `UPSERT`, KV entries,
//! `WHERE pk = <literal>` extraction) uses to turn an `ast::Expr` into a
//! concrete value, so constant folding and spatial-constructor handling never
//! drift between them.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::resolver::expr::convert_value;
use crate::types::*;

pub(crate) fn convert_value_rows(
    columns: &[String],
    rows: &[Vec<ast::Expr>],
) -> Result<Vec<Vec<(String, SqlValue)>>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, expr)| {
                    let col = columns.get(i).cloned().unwrap_or_else(|| format!("col{i}"));
                    let val = expr_to_sql_value(expr)?;
                    Ok((col, val))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect()
}

pub(crate) fn expr_to_sql_value(expr: &ast::Expr) -> Result<SqlValue> {
    match expr {
        ast::Expr::Value(v) => convert_value(&v.value),
        // Array literals lower element-wise into `SqlValue::Array`; there is
        // no array-literal `SqlValue` the constant folder could produce.
        ast::Expr::Array(ast::Array { elem, .. }) => {
            let vals = elem.iter().map(expr_to_sql_value).collect::<Result<_>>()?;
            Ok(SqlValue::Array(vals))
        }
        // A geometry-producing call resolves through the geometry-expression
        // resolver, which turns a malformed geometry into an error rather
        // than the NULL the generic folder would produce — storing NULL for
        // `ST_GeomFromText('POINT(')` would lose the row's geometry silently.
        ast::Expr::Function(func) => {
            match crate::planner::geometry_expr::fold_geometry_function(func) {
                Some(result) => result,
                // Non-geometry functions (`now()`, `date_add(...)`, registered
                // scalars) fold through the shared pipeline below.
                None => fold_constant_value(expr),
            }
        }
        // Everything else — `::TYPE` / `CAST(... AS TYPE)` casts, arithmetic,
        // string concatenation, parenthesised literals — goes through the
        // same resolver and constant folder the `SELECT` projection path
        // uses, so the two surfaces never drift. Only genuinely row- or
        // runtime-dependent expressions (column refs, subqueries, unknown
        // functions) fail here.
        _ => fold_constant_value(expr),
    }
}

fn fold_constant_value(expr: &ast::Expr) -> Result<SqlValue> {
    let sql_expr = crate::resolver::expr::convert_expr(expr)?;
    crate::planner::const_fold::fold_constant_default(&sql_expr).ok_or_else(|| {
        SqlError::Unsupported {
            detail: format!("value expression: {expr}"),
        }
    })
}
