// SPDX-License-Identifier: Apache-2.0

//! The WHERE-clause walk that pulls subquery predicates out into joins.
//!
//! Rewrites WHERE-clause subqueries into semi/anti joins so the existing
//! hash-join executor handles them without a dedicated subquery engine.
//!
//! Supported patterns:
//!   - `WHERE col IN (SELECT col2 FROM tbl ...)` → semi-join
//!   - `WHERE col NOT IN (SELECT col2 FROM tbl ...)` → anti-join
//!   - `WHERE EXISTS (SELECT ... )` → semi-join
//!   - `WHERE NOT EXISTS (SELECT ... )` → anti-join
//!   - `WHERE col > (SELECT AGG(...) FROM tbl ...)` → scalar subquery (materialized)

use sqlparser::ast::{self, Expr};

use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::resolver::columns::TableScope;
use crate::types::*;

/// Result of extracting subqueries from a WHERE clause.
pub struct SubqueryExtraction {
    /// Semi/anti joins to wrap around the base scan.
    pub joins: Vec<SubqueryJoin>,
    /// Remaining WHERE expression with subqueries removed (None if nothing remains).
    pub remaining_where: Option<Expr>,
}

/// A subquery that was rewritten as a join.
pub struct SubqueryJoin {
    /// Equi-join keys as `(outer column, inner column)` pairs. Empty for an
    /// uncorrelated subquery: the probe then treats every inner row as a
    /// candidate, which is what `EXISTS` over an unrelated table means.
    pub on: Vec<(String, String)>,
    /// The planned inner SELECT.
    pub inner_plan: SqlPlan,
    /// Semi (IN / EXISTS) or Anti (NOT IN / NOT EXISTS).
    pub join_type: JoinType,
}

/// Extract subquery predicates from a WHERE clause.
///
/// `outer` is the scope of the enclosing query, so a correlated reference
/// inside a subquery resolves against the relation that owns it.
///
/// Returns the extracted subquery joins and the remaining WHERE expression
/// (with subquery predicates removed). If the entire WHERE is a single
/// subquery predicate, `remaining_where` is `None`.
pub fn extract_subqueries(
    expr: &Expr,
    outer: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SubqueryExtraction> {
    let mut joins = Vec::new();
    let remaining = extract_recursive(expr, &mut joins, outer, catalog, functions, temporal)?;
    Ok(SubqueryExtraction {
        joins,
        remaining_where: remaining,
    })
}

/// Recursively walk the WHERE expression, extracting subquery predicates.
///
/// Returns `None` if the entire expression was consumed (subquery-only),
/// or `Some(expr)` with the remaining non-subquery predicates.
fn extract_recursive(
    expr: &Expr,
    joins: &mut Vec<SubqueryJoin>,
    outer: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<Option<Expr>> {
    match expr {
        // AND: recurse both sides, reconstruct with remaining parts.
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            let left_remaining =
                extract_recursive(left, joins, outer, catalog, functions, temporal)?;
            let right_remaining =
                extract_recursive(right, joins, outer, catalog, functions, temporal)?;
            match (left_remaining, right_remaining) {
                (None, None) => Ok(None),
                (Some(l), None) => Ok(Some(l)),
                (None, Some(r)) => Ok(Some(r)),
                (Some(l), Some(r)) => Ok(Some(Expr::BinaryOp {
                    left: Box::new(l),
                    op: ast::BinaryOperator::And,
                    right: Box::new(r),
                })),
            }
        }

        // IN (SELECT ...): rewrite as semi-join.
        Expr::InSubquery {
            expr: outer_expr,
            subquery,
            negated,
        } => {
            if let Some(join) = super::in_list::try_plan_in_subquery(
                outer_expr, subquery, *negated, outer, catalog, functions, temporal,
            )? {
                joins.push(join);
                Ok(None) // This predicate is consumed.
            } else {
                // Cannot plan as join — return original expression.
                Ok(Some(expr.clone()))
            }
        }

        // Scalar subquery comparison: `col > (SELECT AGG(...) FROM ...)`
        Expr::BinaryOp { left, op, right } if is_comparison_op(op) => {
            if let Expr::Subquery(subquery) = right.as_ref() {
                if let Some(scalar) =
                    super::scalar::try_plan_scalar_subquery(subquery, catalog, functions, temporal)?
                {
                    joins.push(scalar.join);
                    Ok(Some(Expr::BinaryOp {
                        left: left.clone(),
                        op: op.clone(),
                        right: Box::new(scalar.replacement_expr),
                    }))
                } else {
                    Ok(Some(expr.clone()))
                }
            } else {
                Ok(Some(expr.clone()))
            }
        }

        // EXISTS (SELECT ...): rewrite as semi-join.
        // NOT EXISTS (SELECT ...): rewrite as anti-join.
        //
        // A shape the planner cannot lower raises a typed error naming that
        // shape. Leaving the node in the residual WHERE would instead surface
        // it as an unsupported *expression*, which says nothing actionable.
        Expr::Exists { subquery, negated } => {
            joins.push(super::exists::plan_exists_subquery(
                subquery, *negated, outer, catalog, functions, temporal,
            )?);
            Ok(None)
        }

        // Nested parentheses.
        Expr::Nested(inner) => extract_recursive(inner, joins, outer, catalog, functions, temporal),

        // Not a subquery pattern — return as-is.
        _ => Ok(Some(expr.clone())),
    }
}

fn is_comparison_op(op: &ast::BinaryOperator) -> bool {
    matches!(
        op,
        ast::BinaryOperator::Gt
            | ast::BinaryOperator::GtEq
            | ast::BinaryOperator::Lt
            | ast::BinaryOperator::LtEq
            | ast::BinaryOperator::Eq
            | ast::BinaryOperator::NotEq
    )
}
