// SPDX-License-Identifier: Apache-2.0

//! Entry point and collection-backed recursive-scan planning for
//! `WITH RECURSIVE` CTEs.

use sqlparser::ast::{self, Query, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::types::*;

use super::recursive_value::plan_recursive_value;
use super::validate::{count_select_cols, validate_self_ref_count};

/// Default maximum recursion depth for WITH RECURSIVE queries.
pub const DEFAULT_MAX_RECURSION_DEPTH: usize = 1000;

/// Plan a WITH RECURSIVE query.
///
/// Dispatches to either `plan_recursive_scan` (collection-backed) or
/// `plan_recursive_value` (pure expression / value-generating) based on
/// whether the anchor arm references a real collection.
pub fn plan_recursive_cte(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SqlPlan> {
    let with = query.with.as_ref().ok_or_else(|| SqlError::Parse {
        detail: "expected WITH clause".into(),
    })?;

    let cte = with.cte_tables.first().ok_or_else(|| SqlError::Parse {
        detail: "empty WITH clause".into(),
    })?;

    let cte_name = normalize_ident(&cte.alias.name);
    let declared_columns: Vec<String> = cte
        .alias
        .columns
        .iter()
        .map(|c| normalize_ident(&c.name))
        .collect();

    let cte_query = &cte.query;

    // Validate set operator: only UNION / UNION ALL permitted.
    let (left, right, set_quantifier) = match &*cte_query.body {
        SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            left,
            right,
            set_quantifier,
        } => (left, right, set_quantifier),
        SetExpr::SetOperation { op, .. } => {
            return Err(SqlError::InvalidRecursiveSetOp {
                op: format!("{op}"),
            });
        }
        _ => {
            return Err(SqlError::InvalidRecursiveSetOp {
                op: "non-set-operation".into(),
            });
        }
    };

    // Validate self-reference count in the recursive arm.
    validate_self_ref_count(right, &cte_name)?;

    let distinct = !matches!(set_quantifier, ast::SetQuantifier::All);

    // Try to detect whether this is a collection-backed or value-generating CTE
    // by attempting to plan the anchor arm against the catalog.
    match plan_cte_branch(left, catalog, functions, temporal) {
        Ok(base) => {
            let collection = extract_collection(&base);
            if collection.is_empty() {
                // Anchor planned but produced no collection → treat as value-gen.
                plan_recursive_value(left, right, &cte_name, &declared_columns, distinct)
            } else {
                plan_recursive_scan_from_parts(
                    &cte_name,
                    &base,
                    &RecursiveParts {
                        left,
                        right,
                        declared_columns: &declared_columns,
                        distinct,
                    },
                    catalog,
                    functions,
                    temporal,
                )
            }
        }
        Err(_) => {
            // Anchor references CTE name or uses value expressions → value-gen.
            plan_recursive_value(left, right, &cte_name, &declared_columns, distinct)
        }
    }
}

// ── Collection-backed recursive scan ─────────────────────────────────────────

struct RecursiveParts<'a> {
    left: &'a SetExpr,
    right: &'a SetExpr,
    declared_columns: &'a [String],
    distinct: bool,
}

fn plan_recursive_scan_from_parts(
    cte_name: &str,
    base: &SqlPlan,
    parts: &RecursiveParts<'_>,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SqlPlan> {
    let RecursiveParts {
        left,
        right,
        declared_columns,
        distinct,
    } = parts;
    let collection = extract_collection(base);

    // Validate column count if columns were declared.
    if !declared_columns.is_empty() {
        let anchor_cols = count_select_cols(left);
        if anchor_cols != 0 && anchor_cols != declared_columns.len() {
            return Err(SqlError::RecursiveColumnMismatch {
                cte_name: cte_name.to_owned(),
                anchor_cols,
                declared_cols: declared_columns.len(),
            });
        }
    }

    let (recursive_filters, join_link) = match plan_cte_branch(right, catalog, functions, temporal)
    {
        Ok(plan) => (extract_filters(&plan), None),
        Err(_) => super::join_link::extract_recursive_info(right, cte_name)?,
    };

    // The anchor plan carries the CTE's resolved output columns; propagate
    // them so the recursive scan self-describes its output schema.
    let projection = match base {
        SqlPlan::Scan { projection, .. } | SqlPlan::Join { projection, .. } => projection.clone(),
        _ => Vec::new(),
    };

    Ok(SqlPlan::RecursiveScan {
        collection,
        base_filters: extract_filters(base),
        recursive_filters,
        join_link,
        max_iterations: DEFAULT_MAX_RECURSION_DEPTH,
        distinct: *distinct,
        limit: 10000,
        projection,
    })
}

pub(super) fn plan_cte_branch(
    expr: &SetExpr,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SqlPlan> {
    match expr {
        SetExpr::Select(select) => {
            let query = Query {
                with: None,
                body: Box::new(SetExpr::Select(select.clone())),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            };
            crate::planner::select::plan_query(&query, catalog, functions, temporal)
        }
        _ => Err(SqlError::Unsupported {
            detail: "CTE branch must be SELECT".into(),
        }),
    }
}

pub(super) fn extract_collection(plan: &SqlPlan) -> String {
    match plan {
        SqlPlan::Scan { collection, .. } => collection.clone(),
        _ => String::new(),
    }
}

pub(super) fn extract_filters(plan: &SqlPlan) -> Vec<Filter> {
    match plan {
        SqlPlan::Scan { filters, .. } => filters.clone(),
        _ => Vec::new(),
    }
}
