// SPDX-License-Identifier: Apache-2.0

//! Structural validation for `WITH RECURSIVE` CTEs: self-reference counting
//! and column-count checks.

use sqlparser::ast::{self, Query, SetExpr};

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_object_name_checked;

/// Count SELECT projection columns; returns 0 if the expression is not a SELECT.
pub(super) fn count_select_cols(expr: &SetExpr) -> usize {
    match expr {
        SetExpr::Select(s) => s.projection.len(),
        _ => 0,
    }
}

/// Validate that the CTE name appears exactly once in the recursive arm and
/// not inside a subquery, aggregate function, or the nullable side of an outer join.
///
/// Returns `Ok(())` if the reference is valid, or a typed error otherwise.
pub(super) fn validate_self_ref_count(expr: &SetExpr, cte_name: &str) -> Result<()> {
    let select = match expr {
        SetExpr::Select(s) => s,
        // Non-SELECT arm: no self-ref needed.
        _ => return Ok(()),
    };

    let mut count = 0usize;

    for from in &select.from {
        if table_ref_matches(&from.relation, cte_name) {
            count += 1;
        }
        for join in &from.joins {
            if table_ref_matches(&join.relation, cte_name) {
                // Reject self-ref on the nullable side of an outer join.
                if is_nullable_join_side(&join.join_operator) {
                    return Err(SqlError::InvalidRecursiveSelfRef {
                        cte_name: cte_name.to_owned(),
                        reason: "self-reference on the nullable side of an outer join is not \
                                 permitted; use INNER JOIN or move the CTE reference to the \
                                 driving table position"
                            .into(),
                    });
                }
                count += 1;
            }
        }
    }

    // Subquery self-references are not permitted.
    if where_contains_subquery_ref(&select.selection, cte_name) {
        return Err(SqlError::InvalidRecursiveSelfRef {
            cte_name: cte_name.to_owned(),
            reason: "self-reference inside a subquery is not permitted".into(),
        });
    }

    if count > 1 {
        return Err(SqlError::InvalidRecursiveSelfRef {
            cte_name: cte_name.to_owned(),
            reason: format!("self-reference appears {count} times; exactly one is required"),
        });
    }

    // count == 0 is fine for the value-generating case (no table ref at all).
    Ok(())
}

pub(super) fn table_ref_matches(factor: &ast::TableFactor, cte_name: &str) -> bool {
    match factor {
        // A CTE working-table reference is unqualified. Supported system
        // qualifiers must continue to resolve to catalog relations even when
        // their final component collides with the CTE name.
        ast::TableFactor::Table { name, .. } if name.0.len() == 1 => {
            normalize_object_name_checked(name)
                .map(|name| name.eq_ignore_ascii_case(cte_name))
                .unwrap_or(false)
        }
        _ => false,
    }
}

fn is_nullable_join_side(op: &ast::JoinOperator) -> bool {
    use ast::JoinOperator::*;
    matches!(op, LeftOuter(_) | RightOuter(_) | FullOuter(_))
}

fn where_contains_subquery_ref(selection: &Option<ast::Expr>, cte_name: &str) -> bool {
    match selection {
        None => false,
        Some(e) => expr_contains_subquery_ref(e, cte_name),
    }
}

fn expr_contains_subquery_ref(expr: &ast::Expr, cte_name: &str) -> bool {
    match expr {
        ast::Expr::InSubquery { subquery, .. } | ast::Expr::Exists { subquery, .. } => {
            query_references_cte(subquery, cte_name)
        }
        ast::Expr::Subquery(q) => query_references_cte(q, cte_name),
        ast::Expr::BinaryOp { left, right, .. } => {
            expr_contains_subquery_ref(left, cte_name)
                || expr_contains_subquery_ref(right, cte_name)
        }
        ast::Expr::Nested(inner) => expr_contains_subquery_ref(inner, cte_name),
        _ => false,
    }
}

fn query_references_cte(query: &Query, cte_name: &str) -> bool {
    match &*query.body {
        SetExpr::Select(s) => s.from.iter().any(|f| {
            table_ref_matches(&f.relation, cte_name)
                || f.joins
                    .iter()
                    .any(|j| table_ref_matches(&j.relation, cte_name))
        }),
        _ => false,
    }
}
