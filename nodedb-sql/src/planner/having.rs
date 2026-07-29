// SPDX-License-Identifier: Apache-2.0

//! HAVING planning: bind the predicate to computed aggregate columns.
//!
//! A HAVING predicate is evaluated against finalized group rows, so every
//! aggregate it mentions must (a) actually be computed and (b) be addressed by
//! the column name it lands under.
//!
//! Left untranslated, `HAVING SUM(amount) > 0` reaches the executor as a
//! literal `sum(...)` *call* over a group row that has no such column and no
//! scalar `sum` to apply — every group fails the predicate and the query
//! returns nothing, for data that plainly satisfies it. And an aggregate named
//! only in HAVING was never added to the aggregate list, so it was never
//! computed at all.
//!
//! This module rewrites the predicate into references to the canonical
//! aggregate output keys and registers any aggregate that HAVING alone
//! introduced.

use sqlparser::ast;

use crate::aggregate_walk::{contains_aggregate, extract_aggregates};
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionCategory, FunctionRegistry};
use crate::parser::normalize::normalize_ident;
use crate::planner::agg_naming::aggregate_output_key;
use crate::types::{AggregateExpr, Filter};

/// Convert a HAVING clause into filters over finalized group rows.
///
/// `aggregates` is extended in place with any aggregate that appears only in
/// HAVING, so it is computed alongside the projected ones.
pub fn plan_having(
    having: &ast::Expr,
    projection: &[ast::SelectItem],
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
) -> Result<Vec<Filter>> {
    let resolved = resolve_select_aliases(having, projection);
    let rewritten = bind_aggregates(&resolved, aggregates, functions)?;

    // Any aggregate call still standing is one this rewrite did not reach.
    // Failing loudly beats handing the executor a predicate that silently
    // matches no group.
    if contains_aggregate(&rewritten, functions) {
        return Err(SqlError::Unsupported {
            detail: format!(
                "HAVING predicate shape is not supported: aggregate call in `{having}` could not \
                 be bound to a computed group column"
            ),
        });
    }

    crate::planner::select::convert_where_to_filters(&rewritten)
}

/// Substitute SELECT-list output aliases referenced by the predicate.
///
/// `SELECT SUM(amount) AS total ... HAVING total > 0` names an output column
/// that does not exist yet when HAVING runs — the rename to `total` happens
/// after filtering. Replacing it with the underlying expression lets the
/// aggregate binding below address the column that does exist.
fn resolve_select_aliases(expr: &ast::Expr, projection: &[ast::SelectItem]) -> ast::Expr {
    match expr {
        ast::Expr::Identifier(ident) => {
            let needle = normalize_ident(ident);
            projection
                .iter()
                .find_map(|item| match item {
                    ast::SelectItem::ExprWithAlias { expr, alias }
                        if normalize_ident(alias) == needle =>
                    {
                        Some(expr.clone())
                    }
                    _ => None,
                })
                .unwrap_or_else(|| expr.clone())
        }
        ast::Expr::BinaryOp { left, op, right } => ast::Expr::BinaryOp {
            left: Box::new(resolve_select_aliases(left, projection)),
            op: op.clone(),
            right: Box::new(resolve_select_aliases(right, projection)),
        },
        ast::Expr::UnaryOp { op, expr } => ast::Expr::UnaryOp {
            op: *op,
            expr: Box::new(resolve_select_aliases(expr, projection)),
        },
        ast::Expr::Nested(inner) => {
            ast::Expr::Nested(Box::new(resolve_select_aliases(inner, projection)))
        }
        other => other.clone(),
    }
}

/// Replace every aggregate call with a reference to its canonical output
/// column, registering aggregates that the projection did not already request.
fn bind_aggregates(
    expr: &ast::Expr,
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
) -> Result<ast::Expr> {
    match expr {
        ast::Expr::Function(func) if is_aggregate_call(func, functions) => {
            let key = register_aggregate(expr, aggregates, functions)?;
            Ok(ast::Expr::Identifier(ast::Ident::new(key)))
        }
        ast::Expr::BinaryOp { left, op, right } => Ok(ast::Expr::BinaryOp {
            left: Box::new(bind_aggregates(left, aggregates, functions)?),
            op: op.clone(),
            right: Box::new(bind_aggregates(right, aggregates, functions)?),
        }),
        ast::Expr::UnaryOp { op, expr } => Ok(ast::Expr::UnaryOp {
            op: *op,
            expr: Box::new(bind_aggregates(expr, aggregates, functions)?),
        }),
        ast::Expr::Nested(inner) => Ok(ast::Expr::Nested(Box::new(bind_aggregates(
            inner, aggregates, functions,
        )?))),
        ast::Expr::IsNull(inner) => Ok(ast::Expr::IsNull(Box::new(bind_aggregates(
            inner, aggregates, functions,
        )?))),
        ast::Expr::IsNotNull(inner) => Ok(ast::Expr::IsNotNull(Box::new(bind_aggregates(
            inner, aggregates, functions,
        )?))),
        ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(ast::Expr::Between {
            expr: Box::new(bind_aggregates(expr, aggregates, functions)?),
            negated: *negated,
            low: Box::new(bind_aggregates(low, aggregates, functions)?),
            high: Box::new(bind_aggregates(high, aggregates, functions)?),
        }),
        other => Ok(other.clone()),
    }
}

fn is_aggregate_call(func: &ast::Function, functions: &FunctionRegistry) -> bool {
    let name = func
        .name
        .0
        .iter()
        .map(|part| match part {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".");
    matches!(
        functions.lookup(&name).map(|m| m.category),
        Some(FunctionCategory::Aggregate)
    )
}

/// Ensure the aggregate `expr` is in the computed list and return the column
/// key its value lands under.
fn register_aggregate(
    expr: &ast::Expr,
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
) -> Result<String> {
    // The alias is replaced by the canonical key below, so the placeholder is
    // never observable.
    let mut extracted = extract_aggregates(expr, "", functions)?;
    let Some(mut agg) = extracted.pop() else {
        return Err(SqlError::Unsupported {
            detail: format!("HAVING aggregate `{expr}` could not be extracted"),
        });
    };
    if !extracted.is_empty() {
        return Err(SqlError::Unsupported {
            detail: format!("nested aggregates in HAVING are not supported: `{expr}`"),
        });
    }

    let key = aggregate_output_key(&agg);

    // An aggregate the projection already computes needs no second entry —
    // both address the same canonical column.
    if aggregates
        .iter()
        .any(|existing| aggregate_output_key(existing) == key)
    {
        return Ok(key);
    }

    // Carry the canonical key as the alias so no user-facing rename is
    // attached: a HAVING-only aggregate is a filtering input, not an output
    // column the client asked for.
    agg.alias = key.clone();
    aggregates.push(agg);
    Ok(key)
}
