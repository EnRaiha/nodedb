// SPDX-License-Identifier: Apache-2.0

//! Value-generating `WITH RECURSIVE` CTE planning (no collection reference).

use sqlparser::ast::{self, SetExpr};

use crate::error::{Result, SqlError};
use crate::types::*;

use super::recursive_scan::DEFAULT_MAX_RECURSION_DEPTH;

/// Plan a value-generating WITH RECURSIVE CTE (no collection reference).
///
/// Produces a `SqlPlan::RecursiveValue` that carries the anchor and step
/// expressions as raw SQL text for evaluation in the Data Plane.
pub(super) fn plan_recursive_value(
    left: &SetExpr,
    right: &SetExpr,
    cte_name: &str,
    declared_columns: &[String],
    distinct: bool,
) -> Result<SqlPlan> {
    let init_exprs = extract_select_exprs_as_text(left).ok_or_else(|| SqlError::Parse {
        detail: "WITH RECURSIVE anchor must be a SELECT".into(),
    })?;

    // Validate column count against declared columns list.
    if !declared_columns.is_empty() && init_exprs.len() != declared_columns.len() {
        return Err(SqlError::RecursiveColumnMismatch {
            cte_name: cte_name.to_owned(),
            anchor_cols: init_exprs.len(),
            declared_cols: declared_columns.len(),
        });
    }

    let (step_exprs, condition) =
        extract_step_exprs_and_condition(right).ok_or_else(|| SqlError::Parse {
            detail: "WITH RECURSIVE step must be a SELECT".into(),
        })?;

    // Infer column names from anchor if not declared.
    let columns = if declared_columns.is_empty() {
        // Default column names: col0, col1, ...
        (0..init_exprs.len()).map(|i| format!("col{i}")).collect()
    } else {
        declared_columns.to_vec()
    };

    Ok(SqlPlan::RecursiveValue {
        cte_name: cte_name.to_owned(),
        columns,
        init_exprs,
        step_exprs,
        condition,
        max_depth: DEFAULT_MAX_RECURSION_DEPTH,
        distinct,
    })
}

/// Extract SELECT projection items as raw SQL text strings.
fn extract_select_exprs_as_text(expr: &SetExpr) -> Option<Vec<String>> {
    let select = match expr {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    Some(
        select
            .projection
            .iter()
            .map(|item| match item {
                ast::SelectItem::UnnamedExpr(e) => format!("{e}"),
                ast::SelectItem::ExprWithAlias { expr: e, .. } => format!("{e}"),
                ast::SelectItem::Wildcard(_) => "*".into(),
                ast::SelectItem::QualifiedWildcard(name, _) => format!("{name}.*"),
            })
            .collect(),
    )
}

/// Extract step SELECT expressions and optional WHERE condition as SQL text.
///
/// Returns `(step_exprs, condition)`.
fn extract_step_exprs_and_condition(expr: &SetExpr) -> Option<(Vec<String>, Option<String>)> {
    let select = match expr {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    let step_exprs = select
        .projection
        .iter()
        .map(|item| match item {
            ast::SelectItem::UnnamedExpr(e) => format!("{e}"),
            ast::SelectItem::ExprWithAlias { expr: e, .. } => format!("{e}"),
            ast::SelectItem::Wildcard(_) => "*".into(),
            ast::SelectItem::QualifiedWildcard(name, _) => format!("{name}.*"),
        })
        .collect();
    let condition = select.selection.as_ref().map(|e| format!("{e}"));
    Some((step_exprs, condition))
}
