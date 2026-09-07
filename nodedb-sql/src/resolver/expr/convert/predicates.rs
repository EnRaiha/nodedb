// SPDX-License-Identifier: Apache-2.0

//! Predicate and conditional expression conversion.

use sqlparser::ast::{CaseWhen, Expr};

use crate::error::Result;
use crate::resolver::ColumnScope;
use crate::types::*;

use super::entry::convert_expr_depth;

pub(super) fn convert_is_null(
    inner: &Expr,
    negated: bool,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    Ok(SqlExpr::IsNull {
        expr: Box::new(convert_expr_depth(inner, depth, scope)?),
        negated,
    })
}

pub(super) fn convert_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    Ok(SqlExpr::InList {
        expr: Box::new(convert_expr_depth(expr, depth, scope)?),
        list: list
            .iter()
            .map(|e| convert_expr_depth(e, depth, scope))
            .collect::<Result<_>>()?,
        negated,
    })
}

pub(super) fn convert_between(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    Ok(SqlExpr::Between {
        expr: Box::new(convert_expr_depth(expr, depth, scope)?),
        low: Box::new(convert_expr_depth(low, depth, scope)?),
        high: Box::new(convert_expr_depth(high, depth, scope)?),
        negated,
    })
}

pub(super) fn convert_like(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    case_insensitive: bool,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    Ok(SqlExpr::Like {
        expr: Box::new(convert_expr_depth(expr, depth, scope)?),
        pattern: Box::new(convert_expr_depth(pattern, depth, scope)?),
        negated,
        case_insensitive,
    })
}

pub(super) fn convert_case(
    operand: Option<&Expr>,
    conditions: &[CaseWhen],
    else_result: Option<&Expr>,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    let when_then = conditions
        .iter()
        .map(|cw| {
            Ok((
                convert_expr_depth(&cw.condition, depth, scope)?,
                convert_expr_depth(&cw.result, depth, scope)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SqlExpr::Case {
        operand: operand
            .map(|e| convert_expr_depth(e, depth, scope).map(Box::new))
            .transpose()?,
        when_then,
        else_expr: else_result
            .map(|e| convert_expr_depth(e, depth, scope).map(Box::new))
            .transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use crate::resolver::expr::convert::entry::tests::where_sql_expr;
    use crate::types::*;

    #[test]
    fn like_is_case_sensitive() {
        let expr = where_sql_expr("SELECT * FROM t WHERE name LIKE 'foo%'");
        match expr {
            SqlExpr::Like {
                negated,
                case_insensitive,
                ..
            } => {
                assert!(!negated, "LIKE should not be negated");
                assert!(!case_insensitive, "LIKE should be case-sensitive");
            }
            other => panic!("expected SqlExpr::Like, got {other:?}"),
        }
    }

    #[test]
    fn ilike_is_case_insensitive() {
        let expr = where_sql_expr("SELECT * FROM t WHERE name ILIKE 'foo%'");
        match expr {
            SqlExpr::Like {
                negated,
                case_insensitive,
                ..
            } => {
                assert!(!negated, "ILIKE should not be negated");
                assert!(case_insensitive, "ILIKE should be case-insensitive");
            }
            other => panic!("expected SqlExpr::Like, got {other:?}"),
        }
    }

    #[test]
    fn not_like_is_negated_case_sensitive() {
        let expr = where_sql_expr("SELECT * FROM t WHERE name NOT LIKE 'foo%'");
        match expr {
            SqlExpr::Like {
                negated,
                case_insensitive,
                ..
            } => {
                assert!(negated, "NOT LIKE should be negated");
                assert!(!case_insensitive, "NOT LIKE should be case-sensitive");
            }
            other => panic!("expected SqlExpr::Like, got {other:?}"),
        }
    }

    #[test]
    fn not_ilike_is_negated_case_insensitive() {
        let expr = where_sql_expr("SELECT * FROM t WHERE name NOT ILIKE 'foo%'");
        match expr {
            SqlExpr::Like {
                negated,
                case_insensitive,
                ..
            } => {
                assert!(negated, "NOT ILIKE should be negated");
                assert!(case_insensitive, "NOT ILIKE should be case-insensitive");
            }
            other => panic!("expected SqlExpr::Like, got {other:?}"),
        }
    }
}
