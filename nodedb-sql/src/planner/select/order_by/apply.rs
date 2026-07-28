// SPDX-License-Identifier: Apache-2.0

//! ORDER BY entry point.
//!
//! Maps an ORDER BY clause to either sort keys on the existing scan plan or
//! a search-shaped plan (`VectorSearch`, `TextSearch`, `HybridSearch`) when
//! the leading sort expression matches a registered `SearchTrigger`.

use sqlparser::ast;

use super::aliases::resolve_order_by_target;
use super::triggers::try_extract_sort_search;
use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Apply ORDER BY, detecting search-triggering sort expressions.
///
/// `select_items` is the raw SELECT list from the AST. It is required so
/// that an ORDER BY referencing an alias (`ORDER BY score DESC` where the
/// SELECT carries `rrf_score(...) AS score`) can be resolved back to the
/// underlying function call before the search-trigger check runs. Without
/// this resolution the search trigger would only fire when the literal
/// function call appears in ORDER BY — a shape no SQL author would write
/// when the same expression is also being projected.
pub(in crate::planner::select) fn apply_order_by(
    plan: &SqlPlan,
    order_by: &ast::OrderBy,
    functions: &FunctionRegistry,
    select_items: &[ast::SelectItem],
) -> Result<SqlPlan> {
    let exprs = match &order_by.kind {
        ast::OrderByKind::Expressions(exprs) => exprs,
        ast::OrderByKind::All(_) => return Ok(plan.clone()),
    };

    if exprs.is_empty() {
        return Ok(plan.clone());
    }

    // Two resolution rules apply before the trigger check:
    //   (a) Bare-identifier ORDER BY → look up the alias in the SELECT
    //       projection and substitute the underlying expression.
    //   (b) Literal function-call ORDER BY → also check the SELECT for the
    //       same call under an alias, and propagate that alias.
    let first = &exprs[0];
    let (resolved_expr, score_alias) = resolve_order_by_target(&first.expr, select_items);
    if let Some(search_plan) =
        try_extract_sort_search(resolved_expr, plan, functions, score_alias.as_deref())?
    {
        return Ok(search_plan);
    }

    // Normal sort keys.
    let sort_keys: Vec<SortKey> = exprs
        .iter()
        .map(|o| {
            Ok(SortKey {
                expr: convert_expr(&o.expr)?,
                ascending: o.options.asc.unwrap_or(true),
                nulls_first: o.options.nulls_first.unwrap_or(false),
            })
        })
        .collect::<Result<_>>()?;

    match plan {
        SqlPlan::Scan {
            collection,
            alias,
            engine,
            filters,
            projection,
            limit,
            offset,
            distinct,
            window_functions,
            temporal,
            ..
        } => Ok(SqlPlan::Scan {
            collection: collection.clone(),
            alias: alias.clone(),
            engine: *engine,
            filters: filters.clone(),
            projection: projection.clone(),
            sort_keys,
            limit: *limit,
            offset: *offset,
            distinct: *distinct,
            window_functions: window_functions.clone(),
            temporal: *temporal,
        }),
        // ORDER BY applied to a GROUP BY result: stash the sort keys
        // on the Aggregate plan; the executor sorts the finalized
        // group rows before returning. Without this branch the sort
        // is silently dropped — every `… GROUP BY x ORDER BY x` query
        // comes back in hash-map iteration order, which is a
        // data-correctness bug for any downstream consumer.
        SqlPlan::Aggregate {
            input,
            group_by,
            group_by_aliases,
            output_order,
            aggregates,
            having,
            limit,
            grouping_sets,
            ..
        } => Ok(SqlPlan::Aggregate {
            input: input.clone(),
            group_by: group_by.clone(),
            group_by_aliases: group_by_aliases.clone(),
            output_order: output_order.clone(),
            aggregates: aggregates.clone(),
            having: having.clone(),
            limit: *limit,
            grouping_sets: grouping_sets.clone(),
            sort_keys,
        }),
        // A timeseries scan carries its own sort keys: the engine returns
        // rows in the order it finds them (memtable, then partitions), which
        // is not the order the client asked for. Dropping the sort here would
        // silently answer `ORDER BY ts DESC` with ascending rows.
        SqlPlan::TimeseriesScan {
            collection,
            time_range,
            bucket_interval_ms,
            group_by,
            aggregates,
            filters,
            projection,
            gap_fill,
            limit,
            tiered,
            temporal,
            ..
        } => Ok(SqlPlan::TimeseriesScan {
            collection: collection.clone(),
            time_range: *time_range,
            bucket_interval_ms: *bucket_interval_ms,
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
            filters: filters.clone(),
            projection: projection.clone(),
            gap_fill: gap_fill.clone(),
            limit: *limit,
            sort_keys,
            tiered: *tiered,
            temporal: *temporal,
        }),
        // Cte wraps an inner outer plan; push ORDER BY into that outer
        // so derived-table queries (`SELECT … FROM (…) AS t ORDER BY …`)
        // honour the sort. inline_cte downstream merges the outer Scan
        // with the inner subquery plan; the sort_keys ride along.
        SqlPlan::Cte { definitions, outer } => Ok(SqlPlan::Cte {
            definitions: definitions.clone(),
            outer: Box::new(apply_order_by(outer, order_by, functions, select_items)?),
        }),
        _ => Ok(plan.clone()),
    }
}
