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
//! HAVING filters *before* aggregates are renamed to their user aliases, so it
//! binds to the canonical key.

use sqlparser::ast;

use crate::aggregate_walk::contains_aggregate;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::agg_bind::{BindName, bind_aggregate_calls};
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
    let rewritten = bind_aggregate_calls(
        having,
        projection,
        aggregates,
        functions,
        BindName::Canonical,
    )?;

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
