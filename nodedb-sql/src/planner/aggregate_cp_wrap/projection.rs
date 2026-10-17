// SPDX-License-Identifier: Apache-2.0

//! Grouped output projection and sequence-accessor checks.

use super::expression::group_row_columns;
use crate::aggregate_walk::contains_aggregate;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::planner::agg_naming::group_key_row_name;
use crate::planner::aggregate_order::compute_output_order_by_item;
use crate::planner::cp_projection::ast_calls_sequence_accessor;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::plan::{first_sequence_accessor, referenced_columns};
use crate::types::{AggOutputSlot, AggregateExpr, Projection, SqlExpr, SqlPlan};
use sqlparser::ast;

/// The projection restating `plan`'s output columns in SELECT-list order,
/// with a [`Projection::CpComputed`] entry at each accessor item's position.
/// `plan` is the `Aggregate` the items were planned into.
pub(super) fn aggregate_cp_projection(
    plan: &SqlPlan,
    items: &[ast::SelectItem],
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<Projection>> {
    let (group_by, group_by_aliases, aggregates) = match plan {
        SqlPlan::Aggregate {
            group_by,
            group_by_aliases,
            aggregates,
            ..
        } => (group_by, group_by_aliases, aggregates),
        other => {
            return Err(SqlError::Unsupported {
                detail: format!("aggregate wrap over a {} plan", other.variant_name()),
            });
        }
    };
    let key_names: Vec<String> = group_by
        .iter()
        .enumerate()
        .map(|(index, key)| group_key_row_name(key, index))
        .collect();
    // The aggregate renames each aliased key to its alias in the finalized
    // group row, so the restated columns read that name.
    let renames: Vec<(String, String)> = key_names
        .iter()
        .zip(group_by_aliases)
        .filter_map(|(name, alias)| alias.as_ref().map(|alias| (name.clone(), alias.clone())))
        .collect();
    let row_names: Vec<String> = key_names
        .iter()
        .map(|name| {
            renames
                .iter()
                .find(|(from, _)| from == name)
                .map_or_else(|| name.clone(), |(_, to)| to.clone())
        })
        .collect();
    let by_item = compute_output_order_by_item(items, group_by, functions, scope)?;
    // The item resolves against the input relations, so an unknown column is
    // the usual resolve error, and against the group-key row names, so a
    // computed key (`group_0`) is addressable. The reference check below
    // then narrows to the keys: the finalized group row carries nothing
    // else the Control Plane can read.
    let cp_scope = scope
        .with_output_names(key_names.iter().cloned())
        .allowing_cp_functions();

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
            let converted = convert_expr(expr, &ColumnScope::Relations(&cp_scope))?;
            if let Some(name) = first_sequence_accessor(&converted) {
                let name = name.to_string();
                projection.push(cp_item(
                    converted,
                    CpItemNames {
                        accessor: name,
                        alias,
                        key_names: &key_names,
                        renames: &renames,
                    },
                    expr,
                    functions,
                )?);
                continue;
            }
        }
        for slot in slots {
            projection.push(Projection::Column(slot_row_name(
                slot, &row_names, aggregates,
            )?));
        }
    }
    Ok(projection)
}

/// The names one accessor item over a grouped result is checked and keyed by.
struct CpItemNames<'a> {
    /// The sequence accessor the item calls.
    accessor: String,
    /// The item's output name.
    alias: String,
    /// The row name of each GROUP BY key.
    key_names: &'a [String],
    /// `(row name, alias)` of each aliased GROUP BY key.
    renames: &'a [(String, String)],
}

/// The Control-Plane entry for one accessor item over a grouped result.
///
/// The item holds no aggregate: the Control Plane evaluates it over the
/// finalized group row, which carries aggregate values under their output
/// names but no per-row aggregate state. Every column it references is a
/// group key, unqualified and renamed so the reference matches the key the
/// row carries it under.
fn cp_item(
    converted: SqlExpr,
    names: CpItemNames<'_>,
    raw: &ast::Expr,
    functions: &FunctionRegistry,
) -> Result<Projection> {
    let CpItemNames {
        accessor,
        alias,
        key_names,
        renames,
    } = names;
    if contains_aggregate(raw, functions) {
        return Err(SqlError::SequencePerRowUnsupported { name: accessor });
    }
    for column in referenced_columns(&converted) {
        let bare = column.rsplit('.').next().unwrap_or(&column);
        if !key_names.iter().any(|key| key.eq_ignore_ascii_case(bare)) {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "column '{column}' beside a sequence accessor in a grouped SELECT list \
                     must be a GROUP BY key"
                ),
            });
        }
    }
    Ok(Projection::CpComputed {
        expr: group_row_columns(converted, renames),
        alias,
    })
}

/// The key a finalized group row carries one output slot under.
fn slot_row_name(
    slot: AggOutputSlot,
    key_names: &[String],
    aggregates: &[AggregateExpr],
) -> Result<String> {
    match slot {
        AggOutputSlot::GroupKey(index) => key_names.get(index).cloned(),
        AggOutputSlot::Aggregate(index) => aggregates.get(index).map(|a| a.alias.clone()),
    }
    .ok_or_else(|| SqlError::Unsupported {
        detail: format!("aggregate output slot {slot:?} names no output column"),
    })
}
