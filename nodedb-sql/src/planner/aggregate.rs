// SPDX-License-Identifier: Apache-2.0

//! GROUP BY and aggregate planning over a single collection.

use sqlparser::ast;

use crate::engine_rules::{self, AggregateParams, TimeBucketKey};
use crate::error::Result;
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::planner::aggregate_clauses::plan_aggregate_clauses;
use crate::resolver::ColumnScope;
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::resolver::expr::convert_expr;
use crate::temporal::TemporalScope;
use crate::types::*;

/// The inputs of one aggregate query.
pub struct AggregateQuery<'a> {
    pub select: &'a ast::Select,
    pub order_by: Option<&'a ast::OrderBy>,
    /// The single collection the aggregate reads.
    pub table: &'a ResolvedTable,
    /// The WHERE clause, already converted against `scope`.
    pub filters: &'a [Filter],
    pub scope: &'a TableScope,
    pub temporal: &'a TemporalScope,
    /// Set when semi or anti subquery joins must filter the input rows below
    /// the aggregate. The engine then plans an aggregate over an input plan
    /// the caller can wrap.
    pub input_joins: bool,
    /// The designated time-key column of a timeseries collection.
    pub time_key: Option<String>,
}

/// The aggregate's row limit without a `LIMIT` clause: every group. An
/// explicit `LIMIT n` overwrites it in `apply_limit`.
pub const AGGREGATE_NO_LIMIT: usize = usize::MAX;

/// Plan an aggregate query (GROUP BY + aggregate functions).
pub fn plan_aggregate(query: AggregateQuery<'_>, functions: &FunctionRegistry) -> Result<SqlPlan> {
    let AggregateQuery {
        select,
        order_by,
        table,
        filters,
        scope,
        temporal,
        input_joins,
        time_key,
    } = query;
    let clauses = plan_aggregate_clauses(select, order_by, &table.info.columns, scope, functions)?;
    let time_bucket = time_bucket_key(&clauses.group_by, functions);
    let group_keys = clauses.group_by.clone();

    let rules = engine_rules::resolve_engine_rules(table.info.engine);
    let mut plan = rules.plan_aggregate(AggregateParams {
        collection: table.name.clone(),
        alias: table.alias.clone(),
        filters: filters.to_vec(),
        group_by: clauses.group_by,
        aggregates: clauses.aggregates,
        having: clauses.having,
        limit: AGGREGATE_NO_LIMIT,
        time_bucket,
        grouping_sets: clauses.grouping_sets,
        columns: table.info.columns.clone(),
        has_auto_tier: table.info.has_auto_tier,
        bitemporal: table.info.bitemporal,
        temporal: *temporal,
        hidden_aggregates: clauses.hidden_aggregates,
        input_joins,
        time_key,
    })?;

    // The engine rules build the Aggregate node without a projection in scope,
    // so the SELECT-list output aliases for the GROUP BY keys are attached here
    // (Postgres: `SELECT k AS label ... GROUP BY k` yields output column
    // `label`, not `k`).
    if let SqlPlan::Aggregate {
        group_by_aliases,
        output_order,
        ..
    } = &mut plan
    {
        *group_by_aliases = clauses.group_by_aliases;
        *output_order = clauses.output_order;
    } else if let SqlPlan::TimeseriesScan(scan) = &mut plan {
        // A native grouped timeseries scan emits its group rows under fixed
        // keys. The SELECT-list order and aliases are recorded here, so the
        // response shaper reads each key under the name the client asked for.
        scan.output = timeseries_agg_output(
            &group_keys,
            &clauses.group_by_aliases,
            &clauses.output_order,
        );
    }
    Ok(plan)
}

/// The SELECT-list output columns of a native grouped timeseries scan.
///
/// `group_by` holds the GROUP BY keys the slots in `output_order` index.
/// The native scan accepts only column keys and one `time_bucket` key, so a
/// non-column key is its time bucket.
fn timeseries_agg_output(
    group_by: &[SqlExpr],
    group_by_aliases: &[Option<String>],
    output_order: &[AggOutputSlot],
) -> Vec<TimeseriesAggOutput> {
    output_order
        .iter()
        .filter_map(|slot| match slot {
            AggOutputSlot::GroupKey(index) => {
                let alias = group_by_aliases.get(*index).cloned().flatten();
                group_by.get(*index).map(|key| match key {
                    SqlExpr::Column { name, .. } => TimeseriesAggOutput::GroupColumn {
                        name: name.clone(),
                        alias,
                    },
                    other => TimeseriesAggOutput::Bucket {
                        alias,
                        source: bucket_source_column(other),
                    },
                })
            }
            AggOutputSlot::Aggregate(index) => Some(TimeseriesAggOutput::Aggregate(*index)),
        })
        .collect()
}

/// The column a `time_bucket(...)` call buckets: its one column argument.
fn bucket_source_column(key: &SqlExpr) -> Option<String> {
    match key {
        SqlExpr::Function { args, .. } => args.iter().find_map(|arg| match arg {
            SqlExpr::Column { name, .. } => Some(name.clone()),
            _ => None,
        }),
        _ => None,
    }
}

/// The `time_bucket(...)` key among the GROUP BY keys, with its interval.
fn time_bucket_key(group_by: &[SqlExpr], functions: &FunctionRegistry) -> Option<TimeBucketKey> {
    group_by
        .iter()
        .enumerate()
        .find_map(|(group_index, key)| match key {
            SqlExpr::Function { name, args, .. }
                if functions.search_trigger(name) == SearchTrigger::TimeBucket =>
            {
                Some(TimeBucketKey {
                    group_index,
                    interval_ms: bucket_interval_ms(args),
                })
            }
            _ => None,
        })
}

/// The bucket width a `time_bucket` call names, in milliseconds.
///
/// Both argument orders are accepted: `time_bucket('1 hour', ts)`,
/// `time_bucket(ts, '1 hour')`, and integer seconds `time_bucket(3600, ts)`.
/// 0 when no argument is a readable positive interval literal.
fn bucket_interval_ms(args: &[SqlExpr]) -> i64 {
    args.iter()
        .find_map(|arg| match arg {
            SqlExpr::Literal(SqlValue::String(s)) => {
                Some(parse_interval_to_ms(s)).filter(|ms| *ms > 0)
            }
            SqlExpr::Literal(SqlValue::Int(secs)) if *secs > 0 => secs.checked_mul(1000),
            _ => None,
        })
        .unwrap_or(0)
}

/// Parse an interval string to milliseconds.
///
/// Delegates to the canonical `nodedb_types::kv_parsing::parse_interval_to_ms`.
fn parse_interval_to_ms(s: &str) -> i64 {
    nodedb_types::kv_parsing::parse_interval_to_ms(s)
        .map(|ms| ms as i64)
        .unwrap_or(0)
}

/// Scan the SELECT projection for `GROUPING(col)` calls and return synthetic
/// `AggregateExpr` entries so the executor can compute the per-set bitmask value.
///
/// For `GROUPING(col)`, the canonical index of `col` in `canonical_keys` is
/// encoded into the `field` of the resulting `AggregateExpr` (as a decimal string).
/// The executor reads this index and checks the grouping-set bitmask at run time.
pub(super) fn extract_grouping_calls(
    items: &[ast::SelectItem],
    canonical_keys: &[SqlExpr],
    scope: &TableScope,
) -> Result<Vec<AggregateExpr>> {
    let mut out = Vec::new();
    for item in items {
        let (expr, alias): (&ast::Expr, String) = match item {
            ast::SelectItem::UnnamedExpr(expr) => (expr, format!("{expr}")),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            _ => continue,
        };
        collect_grouping_from_expr(expr, &alias, canonical_keys, &mut out, scope)?;
    }
    Ok(out)
}

/// Recursively collect `GROUPING(col)` calls from an expression.
fn collect_grouping_from_expr(
    expr: &ast::Expr,
    alias: &str,
    canonical_keys: &[SqlExpr],
    out: &mut Vec<AggregateExpr>,
    scope: &TableScope,
) -> Result<()> {
    match expr {
        ast::Expr::Function(f) => {
            let name = normalize_function_name(f);
            if name.eq_ignore_ascii_case("grouping") {
                // Extract the column argument(s).
                let args = function_args_exprs(f);
                for col_expr in &args {
                    let canonical_idx = crate::planner::grouping_sets::resolve_grouping_col(
                        col_expr,
                        canonical_keys,
                    )?;
                    // Encode index in the field name; alias is user-visible output name.
                    out.push(AggregateExpr {
                        function: "grouping".into(),
                        args: vec![convert_expr(col_expr, &ColumnScope::Relations(scope))?],
                        alias: alias.to_string(),
                        distinct: false,
                        grouping_col_index: Some(canonical_idx),
                        filter_text: None,
                        arg_text: None,
                    });
                }
            }
        }
        // Recurse into binary ops and other wrappers.
        ast::Expr::BinaryOp { left, right, .. } => {
            collect_grouping_from_expr(left, alias, canonical_keys, out, scope)?;
            collect_grouping_from_expr(right, alias, canonical_keys, out, scope)?;
        }
        _ => {}
    }
    Ok(())
}

/// Extract the positional expression arguments from a function call.
pub(super) fn function_args_exprs(f: &ast::Function) -> Vec<&ast::Expr> {
    match &f.args {
        ast::FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Return the simple lowercase name of a function (unqualified part only).
pub(super) fn normalize_function_name(f: &ast::Function) -> String {
    f.name
        .0
        .last()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .unwrap_or_default()
}

/// Extract aggregate expressions from SELECT projection.
///
/// `scope` gates each aggregate argument, so a column no relation declares
/// raises an undefined-column error instead of planning.
pub fn extract_aggregates_from_projection(
    items: &[ast::SelectItem],
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<AggregateExpr>> {
    let mut aggregates = Vec::new();
    for item in items {
        let (expr, alias): (&ast::Expr, String) = match item {
            // Lowercase the unparsed expression text so the resulting
            // alias (which becomes the JSON row key after the
            // `apply_user_aliases_to_rows` rename) matches the pgwire
            // lookup key built by `expr_column_names`, which also
            // lowercases. Without this match the row description
            // column `count(distinct user_id)` would not resolve to
            // the JSON value stored under `COUNT(DISTINCT user_id)`,
            // and the client would see NULL.
            ast::SelectItem::UnnamedExpr(expr) => (expr, format!("{expr}").to_lowercase()),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            _ => continue,
        };
        let mut extracted =
            crate::aggregate_walk::extract_aggregates(expr, &alias, functions, scope)?;
        aggregates.append(&mut extracted);
    }
    Ok(aggregates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_intervals() {
        assert_eq!(parse_interval_to_ms("1h"), 3_600_000);
        assert_eq!(parse_interval_to_ms("15m"), 900_000);
        assert_eq!(parse_interval_to_ms("30s"), 30_000);
        assert_eq!(parse_interval_to_ms("7d"), 604_800_000);
        // Word-form intervals.
        assert_eq!(parse_interval_to_ms("1 hour"), 3_600_000);
        assert_eq!(parse_interval_to_ms("2 hours"), 7_200_000);
        assert_eq!(parse_interval_to_ms("15 minutes"), 900_000);
        assert_eq!(parse_interval_to_ms("30 seconds"), 30_000);
        assert_eq!(parse_interval_to_ms("1 day"), 86_400_000);
        assert_eq!(parse_interval_to_ms("5 min"), 300_000);
    }

    fn column(name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: None,
            name: name.into(),
        }
    }

    fn time_bucket(interval: SqlValue) -> SqlExpr {
        SqlExpr::Function {
            name: "time_bucket".into(),
            args: vec![SqlExpr::Literal(interval), column("ts")],
            distinct: false,
        }
    }

    #[test]
    fn time_bucket_key_finds_the_bucket_among_plain_keys() {
        let functions = FunctionRegistry::new();
        let keys = vec![
            column("host"),
            time_bucket(SqlValue::String("1 hour".into())),
        ];
        assert_eq!(
            time_bucket_key(&keys, &functions),
            Some(TimeBucketKey {
                group_index: 1,
                interval_ms: 3_600_000,
            })
        );
    }

    #[test]
    fn time_bucket_key_reads_integer_seconds() {
        let functions = FunctionRegistry::new();
        let keys = vec![time_bucket(SqlValue::Int(300))];
        assert_eq!(
            time_bucket_key(&keys, &functions).map(|k| k.interval_ms),
            Some(300_000)
        );
    }

    #[test]
    fn time_bucket_key_is_absent_for_plain_columns() {
        let functions = FunctionRegistry::new();
        assert_eq!(time_bucket_key(&[column("qtype")], &functions), None);
    }

    /// The native scan's output follows the SELECT list: an aggregate, the
    /// bucket under its alias with the column it buckets, then a tag column.
    #[test]
    fn timeseries_output_follows_the_select_list() {
        let keys = vec![
            column("host"),
            time_bucket(SqlValue::String("1 hour".into())),
        ];
        let output = timeseries_agg_output(
            &keys,
            &[None, Some("b".into())],
            &[
                AggOutputSlot::Aggregate(0),
                AggOutputSlot::GroupKey(1),
                AggOutputSlot::GroupKey(0),
            ],
        );
        assert_eq!(
            output,
            vec![
                TimeseriesAggOutput::Aggregate(0),
                TimeseriesAggOutput::Bucket {
                    alias: Some("b".into()),
                    source: Some("ts".into()),
                },
                TimeseriesAggOutput::GroupColumn {
                    name: "host".into(),
                    alias: None,
                },
            ]
        );
    }

    #[test]
    fn unreadable_interval_is_zero() {
        let functions = FunctionRegistry::new();
        let keys = vec![time_bucket(SqlValue::String("soon".into()))];
        assert_eq!(
            time_bucket_key(&keys, &functions).map(|k| k.interval_ms),
            Some(0)
        );
    }
}
