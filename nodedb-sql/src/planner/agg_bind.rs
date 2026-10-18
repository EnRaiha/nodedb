// SPDX-License-Identifier: Apache-2.0

//! Binding aggregate calls in HAVING, post-aggregate ORDER BY, and computed
//! grouped SELECT items.
//!
//! These clauses are evaluated against finalized group rows. An aggregate one
//! mentions must be computed and addressed by the column it lands under. Left
//! as a literal `sum(...)` call, it reaches the executor as a scalar function
//! over a row that has no such column. The predicate matches nothing, or the
//! value resolves to nothing.
//!
//! The clauses differ in *which* name they need, because they run on
//! opposite sides of the rename to user aliases:
//!
//! - HAVING filters before the rename, so it addresses the canonical key
//!   (`sum(amount)`).
//! - ORDER BY and a computed SELECT item run after the rename, so they
//!   address the output name (`total`, when the projection said
//!   `SUM(amount) AS total`).

use std::ops::ControlFlow;

use sqlparser::ast::{self, VisitMut, VisitorMut};

use crate::aggregate_walk::extract_aggregates;
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionCategory, FunctionRegistry};
use crate::parser::normalize::normalize_ident;
use crate::planner::agg_naming::aggregate_output_key;
use crate::resolver::columns::TableScope;
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
///
/// `scope` carries the input relations each aggregate argument resolves
/// against.
pub fn bind_aggregate_calls(
    expr: &ast::Expr,
    projection: &[ast::SelectItem],
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
    name: BindName,
    scope: &TableScope,
) -> Result<ast::Expr> {
    let resolved = resolve_select_aliases(expr, projection);
    bind(&resolved, aggregates, functions, name, scope)
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

/// Rewrite every aggregate call in `expr` at any depth: inside a scalar
/// function (`ROUND(AVG(v), 1)`), a CASE, a CAST, an IN list, and every other
/// expression position. An aggregate inside a subquery belongs to that
/// subquery, so it stays as written.
fn bind(
    expr: &ast::Expr,
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
    name: BindName,
    scope: &TableScope,
) -> Result<ast::Expr> {
    let mut bound = expr.clone();
    let mut binder = AggregateBinder {
        aggregates,
        functions,
        name,
        scope,
        query_depth: 0,
        error: None,
    };
    let _ = bound.visit(&mut binder);
    match binder.error {
        Some(error) => Err(error),
        None => Ok(bound),
    }
}

/// Replaces each aggregate call outside a subquery with a reference to its
/// computed column. The first registration error stops the walk.
struct AggregateBinder<'a> {
    aggregates: &'a mut Vec<AggregateExpr>,
    functions: &'a FunctionRegistry,
    name: BindName,
    scope: &'a TableScope,
    /// How many subqueries enclose the expression being visited.
    query_depth: usize,
    error: Option<SqlError>,
}

impl VisitorMut for AggregateBinder<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    /// The walk visits the children of the replacement, an identifier, so it
    /// does not descend into the aggregate's arguments.
    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        if self.query_depth > 0 {
            return ControlFlow::Continue(());
        }
        let ast::Expr::Function(func) = &*expr else {
            return ControlFlow::Continue(());
        };
        if !is_aggregate_call(func, self.functions) {
            return ControlFlow::Continue(());
        }
        match register_aggregate(expr, self.aggregates, self.functions, self.name, self.scope) {
            Ok(column) => {
                *expr = ast::Expr::Identifier(ast::Ident::new(column));
                ControlFlow::Continue(())
            }
            Err(error) => {
                self.error = Some(error);
                ControlFlow::Break(())
            }
        }
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
    scope: &TableScope,
) -> Result<String> {
    // The alias is replaced below for a newly registered aggregate, so the
    // placeholder is never observable.
    let mut extracted = extract_aggregates(expr, "", functions, scope)?;
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
