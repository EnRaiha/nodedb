// SPDX-License-Identifier: Apache-2.0

//! Grouped output projection: the SELECT list restated over the group rows.

use super::expression::{GroupRowNames, group_row_columns};
use crate::aggregate_walk::contains_aggregate;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::planner::agg_bind::{BindName, bind_aggregate_calls};
use crate::planner::agg_naming::group_key_row_name;
use crate::planner::aggregate_order::compute_output_order_by_item;
use crate::planner::cp_projection::ast_calls_sequence_accessor;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::plan::{first_sequence_accessor, referenced_columns};
use crate::types::{
    AggOutputSlot, AggregateExpr, Projection, SqlExpr, SqlPlan, TimeseriesAggOutput,
    TimeseriesScanPlan,
};
use sqlparser::ast;

/// The names a grouped plan's finalized rows carry.
pub(super) struct GroupRow<'a> {
    /// The GROUP BY keys as planned.
    group_by: &'a [SqlExpr],
    /// Each key's name before any alias rename: the bare column, or
    /// `group_{index}` for a computed key.
    key_names: Vec<String>,
    /// Each key's name in the finalized row.
    row_names: Vec<String>,
    /// The computed aggregates. Each one's alias is its name in the row.
    aggregates: &'a [AggregateExpr],
}

impl<'a> GroupRow<'a> {
    /// The row names of `plan`, a grouped `Aggregate` or an aggregating
    /// timeseries scan.
    pub(super) fn of(plan: &'a SqlPlan) -> Result<Self> {
        match plan {
            SqlPlan::Aggregate {
                group_by,
                group_by_aliases,
                aggregates,
                ..
            } => {
                let key_names = key_names(group_by);
                // The aggregate renames each aliased key to its alias.
                let row_names = key_names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        group_by_aliases
                            .get(index)
                            .cloned()
                            .flatten()
                            .unwrap_or_else(|| name.clone())
                    })
                    .collect();
                Ok(Self {
                    group_by,
                    key_names,
                    row_names,
                    aggregates,
                })
            }
            SqlPlan::TimeseriesScan(scan) => Self::of_timeseries(scan),
            other => Err(SqlError::Unsupported {
                detail: format!("a grouped SELECT list over a {} plan", other.variant_name()),
            }),
        }
    }

    /// The row names of an aggregating timeseries scan. Its rows carry every
    /// key under the output name `scan.output` gives it.
    fn of_timeseries(scan: &'a TimeseriesScanPlan) -> Result<Self> {
        let key_names = key_names(&scan.group_keys);
        let row_names = scan
            .group_keys
            .iter()
            .map(|key| {
                scan.output
                    .iter()
                    .find(|column| match (column, key) {
                        (
                            TimeseriesAggOutput::GroupColumn { name, .. },
                            SqlExpr::Column { name: key_name, .. },
                        ) => name == key_name,
                        (TimeseriesAggOutput::Bucket { .. }, SqlExpr::Column { .. }) => false,
                        (TimeseriesAggOutput::Bucket { .. }, _) => true,
                        _ => false,
                    })
                    .and_then(|column| column.key_and_name(&scan.aggregates))
                    .map(|(_, name)| name)
                    .ok_or_else(|| SqlError::Unsupported {
                        detail: format!(
                            "the grouped timeseries scan of '{}' emits no column for its \
                             GROUP BY key {key:?}",
                            scan.collection
                        ),
                    })
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            group_by: &scan.group_keys,
            key_names,
            row_names,
            aggregates: &scan.aggregates,
        })
    }

    /// The renames and computed-key matches that read an expression over the
    /// input relations from the finalized row.
    fn names(&self) -> GroupRowNames {
        GroupRowNames {
            renames: self
                .key_names
                .iter()
                .zip(&self.row_names)
                .filter(|(from, to)| from != to)
                .map(|(from, to)| (from.clone(), to.clone()))
                .collect(),
            computed_keys: self
                .group_by
                .iter()
                .zip(&self.row_names)
                .filter(|(key, _)| !matches!(key, SqlExpr::Column { .. }))
                .map(|(key, name)| (format!("{key:?}"), name.clone()))
                .collect(),
        }
    }
}

/// The pre-rename row name of each GROUP BY key.
fn key_names(group_by: &[SqlExpr]) -> Vec<String> {
    group_by
        .iter()
        .enumerate()
        .map(|(index, key)| group_key_row_name(key, index))
        .collect()
}

/// The projection restating `plan`'s output columns in SELECT-list order.
///
/// A bare GROUP BY key or a bare aggregate reads its row column. A sequence
/// accessor item becomes a [`Projection::CpComputed`] entry. Any other item
/// becomes a [`Projection::Computed`] entry evaluated over the group row:
/// `ROUND(AVG(v), 1)`, `SUM(a) / COUNT(*)`, `CASE WHEN COUNT(*) > 1 ...`,
/// `UPPER(host)` over the key `host`, or a literal.
pub(super) fn aggregate_cp_projection(
    plan: &SqlPlan,
    items: &[ast::SelectItem],
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<Projection>> {
    let row = GroupRow::of(plan)?;
    let names = row.names();
    let by_item = compute_output_order_by_item(items, row.group_by, functions, scope)?;
    // An item resolves against the input relations, so an unknown column is
    // the usual resolve error, and against the group row's names, so a
    // computed key (`group_0`) and an aggregate (`avg(v)`) are addressable.
    // The reference check below then narrows to those names: the finalized
    // group row carries nothing else.
    let row_scope = scope.with_output_names(
        row.key_names
            .iter()
            .chain(&row.row_names)
            .cloned()
            .chain(row.aggregates.iter().map(|a| a.alias.clone())),
    );
    let readable: Vec<&str> = row
        .row_names
        .iter()
        .map(String::as_str)
        .chain(row.aggregates.iter().map(|a| a.alias.as_str()))
        .collect();

    let mut projection = Vec::with_capacity(items.len());
    for (item, slots) in items.iter().zip(by_item) {
        let (expr, alias) = match item {
            ast::SelectItem::UnnamedExpr(expr) => (expr, format!("{expr}").to_lowercase()),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            ast::SelectItem::ExprWithAliases { .. }
            | ast::SelectItem::Wildcard(_)
            | ast::SelectItem::QualifiedWildcard(..) => continue,
        };
        if ast_calls_sequence_accessor(expr) {
            let cp_scope = row_scope.clone().allowing_cp_functions();
            let converted = convert_expr(expr, &ColumnScope::Relations(&cp_scope))?;
            if let Some(name) = first_sequence_accessor(&converted) {
                if contains_aggregate(expr, functions) {
                    return Err(SqlError::SequencePerRowUnsupported {
                        name: name.to_string(),
                    });
                }
                let expr = group_row_columns(converted, &names);
                require_group_row_columns(&expr, &readable)?;
                projection.push(Projection::CpComputed { expr, alias });
                continue;
            }
        }
        if item_reads_its_slots(expr, &slots, functions) {
            for slot in slots {
                projection.push(Projection::Column(slot_row_name(slot, &row)?));
            }
            continue;
        }
        let expr = computed_over_group_row(expr, &row, &row_scope, functions)?;
        let expr = group_row_columns(expr, &names);
        require_group_row_columns(&expr, &readable)?;
        projection.push(Projection::Computed { expr, alias });
    }
    Ok(projection)
}

/// Whether a SELECT list over a plan grouped by `group_by` holds an item the
/// grouped plan does not emit as one of its columns, so a tail must evaluate
/// it over the group rows.
pub(crate) fn items_need_group_row_tail(
    group_by: &[SqlExpr],
    items: &[ast::SelectItem],
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<bool> {
    let by_item = compute_output_order_by_item(items, group_by, functions, scope)?;
    Ok(items.iter().zip(&by_item).any(|(item, slots)| match item {
        ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
            !item_reads_its_slots(expr, slots, functions)
        }
        ast::SelectItem::ExprWithAliases { .. }
        | ast::SelectItem::Wildcard(_)
        | ast::SelectItem::QualifiedWildcard(..) => false,
    }))
}

/// Whether the item is exactly its output slots: one bare GROUP BY key or one
/// bare aggregate. `GROUPING(col)` items are slots too.
fn item_reads_its_slots(
    expr: &ast::Expr,
    slots: &[AggOutputSlot],
    functions: &FunctionRegistry,
) -> bool {
    match slots {
        [AggOutputSlot::GroupKey(_)] => true,
        [AggOutputSlot::Aggregate(_)] => {
            !crate::aggregate_walk::computes_over_aggregate(expr, functions)
        }
        [] => false,
        // Several slots from one item: `GROUPING(a) + GROUPING(b)` or an
        // expression over several aggregates. An expression over aggregates
        // is evaluated; a GROUPING pair keeps its slots.
        _ => !contains_aggregate(expr, functions),
    }
}

/// `expr` with each aggregate call bound to the column the group row
/// carries it under, converted against the group row's names.
///
/// A SELECT item names input columns, never another item's output alias, so
/// no alias substitution applies.
fn computed_over_group_row(
    expr: &ast::Expr,
    row: &GroupRow<'_>,
    row_scope: &TableScope,
    functions: &FunctionRegistry,
) -> Result<SqlExpr> {
    let mut known = row.aggregates.to_vec();
    let bound = bind_aggregate_calls(
        expr,
        &[],
        &mut known,
        functions,
        BindName::Output,
        row_scope,
    )?;
    if known.len() != row.aggregates.len() {
        return Err(SqlError::Unsupported {
            detail: format!("the grouped plan computes no aggregate for `{expr}`"),
        });
    }
    convert_expr(&bound, &ColumnScope::Relations(row_scope))
}

/// Refuse an item that reads a column the finalized group row does not
/// carry: a column that is neither a GROUP BY key nor inside an aggregate.
///
/// A column inside a GROUP BY expression is not a key by itself. With
/// `GROUP BY x / 0`, the item `x` reads no key, as PostgreSQL rules. The item
/// `x / 0` reads the key, and `group_row_columns` has already rewritten it to
/// the key's row column.
fn require_group_row_columns(expr: &SqlExpr, readable: &[&str]) -> Result<()> {
    match referenced_columns(expr).into_iter().find(|column| {
        !readable
            .iter()
            .any(|name| name.eq_ignore_ascii_case(column))
    }) {
        Some(column) => Err(SqlError::UngroupedColumn { column }),
        None => Ok(()),
    }
}

/// The key a finalized group row carries one output slot under.
fn slot_row_name(slot: AggOutputSlot, row: &GroupRow<'_>) -> Result<String> {
    match slot {
        AggOutputSlot::GroupKey(index) => row.row_names.get(index).cloned(),
        AggOutputSlot::Aggregate(index) => row.aggregates.get(index).map(|a| a.alias.clone()),
    }
    .ok_or_else(|| SqlError::Unsupported {
        detail: format!("aggregate output slot {slot:?} names no output column"),
    })
}
