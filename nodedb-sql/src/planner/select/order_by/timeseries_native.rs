// SPDX-License-Identifier: Apache-2.0

//! ORDER BY over a native grouped timeseries scan.
//!
//! The native scan sorts its encoded group rows before LIMIT. Those rows
//! carry each GROUP BY column under its own name, the time bucket under
//! `bucket`, and each aggregate under its canonical key (`count(*)`,
//! `sum(v)`). The SELECT-list aliases are applied later, by the Control
//! Plane. So a sort key must name the encoded column: an alias resolves to
//! its expression, an aggregate call binds to its canonical key, and a
//! `time_bucket(...)` call names `bucket`.

use sqlparser::ast;

use super::aliases::select_output_aliases;
use crate::SqlError;
use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::planner::agg_bind::{BindName, bind_aggregate_calls};
use crate::planner::sort::{sort_ascending, sort_nulls_first};
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::{AggregateExpr, SortKey};

/// The column the native scan writes the time bucket under.
const BUCKET_COLUMN: &str = "bucket";

/// Sort keys for a native grouped timeseries scan computing `aggregates`.
///
/// The planner routes a query whose ORDER BY names an aggregate the SELECT
/// list does not compute to the generic aggregate, so binding here never
/// adds one. If it does, the plan is inconsistent and planning fails.
pub(super) fn native_timeseries_sort_keys(
    exprs: &[ast::OrderByExpr],
    aggregates: &[AggregateExpr],
    select_items: &[ast::SelectItem],
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<SortKey>> {
    let mut extended = aggregates.to_vec();
    let bound: Vec<ast::Expr> = exprs
        .iter()
        .map(|o| {
            bind_aggregate_calls(
                &o.expr,
                select_items,
                &mut extended,
                functions,
                BindName::Canonical,
                scope,
            )
            .map(name_bucket)
        })
        .collect::<Result<_>>()?;
    if extended.len() != aggregates.len() {
        return Err(SqlError::Unsupported {
            detail: "ORDER BY names an aggregate the grouped timeseries scan does not compute"
                .into(),
        });
    }
    let sort_scope = scope.with_output_names(
        aggregates
            .iter()
            .map(crate::planner::agg_naming::aggregate_output_key)
            .chain(std::iter::once(BUCKET_COLUMN.to_string()))
            .chain(select_output_aliases(select_items)),
    );
    exprs
        .iter()
        .zip(&bound)
        .map(|(o, expr)| {
            Ok(SortKey {
                expr: convert_expr(expr, &ColumnScope::Relations(&sort_scope))?,
                ascending: sort_ascending(&o.options)?,
                nulls_first: sort_nulls_first(&o.options)?,
            })
        })
        .collect()
}

/// A `time_bucket(...)` call as a reference to the `bucket` column.
fn name_bucket(expr: ast::Expr) -> ast::Expr {
    let is_time_bucket = match &expr {
        ast::Expr::Function(f) => matches!(
            f.name.0.last(),
            Some(ast::ObjectNamePart::Identifier(ident))
                if ident.value.eq_ignore_ascii_case("time_bucket")
        ),
        _ => false,
    };
    if is_time_bucket {
        ast::Expr::Identifier(ast::Ident::new(BUCKET_COLUMN))
    } else {
        expr
    }
}
