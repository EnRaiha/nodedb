// SPDX-License-Identifier: Apache-2.0

//! Binding aggregate calls in HAVING and post-aggregate ORDER BY.
//!
//! Both clauses are evaluated against finalized group rows, so an aggregate
//! either mentions must actually be computed and must be addressed by the
//! column name it lands under. Left as a literal `sum(...)` call, it reaches
//! the executor as a scalar function over a row that has no such column — the
//! predicate matches nothing, or the sort key resolves to nothing.
//!
//! The two clauses differ in *which* name they need, because they run on
//! opposite sides of the rename to user aliases:
//!
//! - HAVING filters before the rename, so it addresses the canonical key
//!   (`sum(amount)`).
//! - ORDER BY sorts after the rename, so it addresses the output name
//!   (`total`, when the projection said `SUM(amount) AS total`).

use sqlparser::ast;

use crate::aggregate_walk::extract_aggregates;
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionCategory, FunctionRegistry};
use crate::parser::normalize::normalize_ident;
use crate::planner::agg_naming::aggregate_output_key;
use crate::types::AggregateExpr;

/// Which name an aggregate call is rewritten to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindName {
    /// The canonical key a group row carries before user aliases are applied.
    Canonical,
    /// The output column name the client sees, after that rename.
    Output,
}

/// Rewrite every aggregate call in `expr` to a reference to its computed
/// column, registering aggregates the projection did not already request.
pub fn bind_aggregate_calls(
    expr: &ast::Expr,
    projection: &[ast::SelectItem],
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
    name: BindName,
) -> Result<ast::Expr> {
    let resolved = resolve_select_aliases(expr, projection);
    bind(&resolved, aggregates, functions, name)
}

/// Substitute SELECT-list output aliases referenced by the expression.
///
/// `SELECT SUM(amount) AS total ... HAVING total > 0` names an output column
/// that does not exist yet when the clause runs. Replacing it with the
/// underlying expression lets the aggregate binding below address the column
/// that does exist.
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

fn bind(
    expr: &ast::Expr,
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
    name: BindName,
) -> Result<ast::Expr> {
    match expr {
        ast::Expr::Function(func) if is_aggregate_call(func, functions) => {
            let column = register_aggregate(expr, aggregates, functions, name)?;
            Ok(ast::Expr::Identifier(ast::Ident::new(column)))
        }
        ast::Expr::BinaryOp { left, op, right } => Ok(ast::Expr::BinaryOp {
            left: Box::new(bind(left, aggregates, functions, name)?),
            op: op.clone(),
            right: Box::new(bind(right, aggregates, functions, name)?),
        }),
        ast::Expr::UnaryOp { op, expr } => Ok(ast::Expr::UnaryOp {
            op: *op,
            expr: Box::new(bind(expr, aggregates, functions, name)?),
        }),
        ast::Expr::Nested(inner) => Ok(ast::Expr::Nested(Box::new(bind(
            inner, aggregates, functions, name,
        )?))),
        ast::Expr::IsNull(inner) => Ok(ast::Expr::IsNull(Box::new(bind(
            inner, aggregates, functions, name,
        )?))),
        ast::Expr::IsNotNull(inner) => Ok(ast::Expr::IsNotNull(Box::new(bind(
            inner, aggregates, functions, name,
        )?))),
        ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(ast::Expr::Between {
            expr: Box::new(bind(expr, aggregates, functions, name)?),
            negated: *negated,
            low: Box::new(bind(low, aggregates, functions, name)?),
            high: Box::new(bind(high, aggregates, functions, name)?),
        }),
        other => Ok(other.clone()),
    }
}

pub(super) fn is_aggregate_call(func: &ast::Function, functions: &FunctionRegistry) -> bool {
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
/// name it should be addressed by.
fn register_aggregate(
    expr: &ast::Expr,
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
    name: BindName,
) -> Result<String> {
    // The alias is replaced below for a newly registered aggregate, so the
    // placeholder is never observable.
    let mut extracted = extract_aggregates(expr, "", functions)?;
    let Some(mut agg) = extracted.pop() else {
        return Err(SqlError::Unsupported {
            detail: format!("aggregate `{expr}` could not be extracted"),
        });
    };
    if !extracted.is_empty() {
        return Err(SqlError::Unsupported {
            detail: format!("nested aggregates are not supported: `{expr}`"),
        });
    }

    let key = aggregate_output_key(&agg);

    // An aggregate the projection already computes needs no second entry. Its
    // output name is its alias, which is what a post-rename caller must use.
    if let Some(existing) = aggregates
        .iter()
        .find(|existing| aggregate_output_key(existing) == key)
    {
        return Ok(match name {
            BindName::Canonical => key,
            BindName::Output => existing.alias.clone(),
        });
    }

    // Carry the canonical key as the alias so no user-facing rename is
    // attached: an aggregate introduced only by HAVING or ORDER BY is an input
    // to filtering or sorting, not an output column the client asked for. Both
    // names therefore coincide.
    agg.alias = key.clone();
    aggregates.push(agg);
    Ok(key)
}
