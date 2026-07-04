// SPDX-License-Identifier: Apache-2.0

//! CTE (WITH clause) and WITH RECURSIVE planning.

use sqlparser::ast::{self, Query, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::types::*;

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
        Err(_) => extract_recursive_info(right, cte_name)?,
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

// ── Value-generating recursive CTE ───────────────────────────────────────────

/// Plan a value-generating WITH RECURSIVE CTE (no collection reference).
///
/// Produces a `SqlPlan::RecursiveValue` that carries the anchor and step
/// expressions as raw SQL text for evaluation in the Data Plane.
fn plan_recursive_value(
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

// ── Validation ────────────────────────────────────────────────────────────────

/// Count SELECT projection columns; returns 0 if the expression is not a SELECT.
fn count_select_cols(expr: &SetExpr) -> usize {
    match expr {
        SetExpr::Select(s) => s.projection.len(),
        _ => 0,
    }
}

/// Validate that the CTE name appears exactly once in the recursive arm and
/// not inside a subquery, aggregate function, or the nullable side of an outer join.
///
/// Returns `Ok(())` if the reference is valid, or a typed error otherwise.
fn validate_self_ref_count(expr: &SetExpr, cte_name: &str) -> Result<()> {
    let select = match expr {
        SetExpr::Select(s) => s,
        // Non-SELECT arm: no self-ref needed.
        _ => return Ok(()),
    };

    let mut count = 0usize;

    for from in &select.from {
        if table_ref_matches(&from.relation, cte_name) {
            count += 1;
        }
        for join in &from.joins {
            if table_ref_matches(&join.relation, cte_name) {
                // Reject self-ref on the nullable side of an outer join.
                if is_nullable_join_side(&join.join_operator) {
                    return Err(SqlError::InvalidRecursiveSelfRef {
                        cte_name: cte_name.to_owned(),
                        reason: "self-reference on the nullable side of an outer join is not \
                                 permitted; use INNER JOIN or move the CTE reference to the \
                                 driving table position"
                            .into(),
                    });
                }
                count += 1;
            }
        }
    }

    // Subquery self-references are not permitted.
    if where_contains_subquery_ref(&select.selection, cte_name) {
        return Err(SqlError::InvalidRecursiveSelfRef {
            cte_name: cte_name.to_owned(),
            reason: "self-reference inside a subquery is not permitted".into(),
        });
    }

    if count > 1 {
        return Err(SqlError::InvalidRecursiveSelfRef {
            cte_name: cte_name.to_owned(),
            reason: format!("self-reference appears {count} times; exactly one is required"),
        });
    }

    // count == 0 is fine for the value-generating case (no table ref at all).
    Ok(())
}

fn table_ref_matches(factor: &ast::TableFactor, cte_name: &str) -> bool {
    match factor {
        ast::TableFactor::Table { name, .. } => normalize_object_name_checked(name)
            .map(|n| n.eq_ignore_ascii_case(cte_name))
            .unwrap_or(false),
        _ => false,
    }
}

fn is_nullable_join_side(op: &ast::JoinOperator) -> bool {
    use ast::JoinOperator::*;
    matches!(op, LeftOuter(_) | RightOuter(_) | FullOuter(_))
}

fn where_contains_subquery_ref(selection: &Option<ast::Expr>, cte_name: &str) -> bool {
    match selection {
        None => false,
        Some(e) => expr_contains_subquery_ref(e, cte_name),
    }
}

fn expr_contains_subquery_ref(expr: &ast::Expr, cte_name: &str) -> bool {
    match expr {
        ast::Expr::InSubquery { subquery, .. } | ast::Expr::Exists { subquery, .. } => {
            query_references_cte(subquery, cte_name)
        }
        ast::Expr::Subquery(q) => query_references_cte(q, cte_name),
        ast::Expr::BinaryOp { left, right, .. } => {
            expr_contains_subquery_ref(left, cte_name)
                || expr_contains_subquery_ref(right, cte_name)
        }
        ast::Expr::Nested(inner) => expr_contains_subquery_ref(inner, cte_name),
        _ => false,
    }
}

fn query_references_cte(query: &Query, cte_name: &str) -> bool {
    match &*query.body {
        SetExpr::Select(s) => s.from.iter().any(|f| {
            table_ref_matches(&f.relation, cte_name)
                || f.joins
                    .iter()
                    .any(|j| table_ref_matches(&j.relation, cte_name))
        }),
        _ => false,
    }
}

// ── Helpers shared with collection-backed path ────────────────────────────────

/// Extract recursive info from the AST when normal planning fails
/// because the FROM clause references the CTE name.
///
/// Returns `(filters, join_link)` where `join_link` is the
/// `(collection_field, working_table_field)` pair for the working-table
/// hash-join.
type RecursiveInfo = (Vec<Filter>, Option<(String, String)>);

fn extract_recursive_info(expr: &SetExpr, cte_name: &str) -> Result<RecursiveInfo> {
    let select = match expr {
        SetExpr::Select(s) => s,
        _ => {
            return Err(SqlError::Unsupported {
                detail: "recursive CTE branch must be SELECT".into(),
            });
        }
    };

    let mut real_table_alias = None;
    let mut cte_alias = None;
    let mut join_on_expr = None;

    for from in &select.from {
        let table_name = extract_table_name(&from.relation);
        let table_alias = extract_table_alias(&from.relation);

        if let Some(name) = &table_name {
            if name.eq_ignore_ascii_case(cte_name) {
                cte_alias = table_alias.or_else(|| Some(name.clone()));
            } else {
                real_table_alias = table_alias.or_else(|| Some(name.clone()));
            }
        }

        for join in &from.joins {
            let join_table = extract_table_name(&join.relation);
            let join_alias = extract_table_alias(&join.relation);
            if let Some(jt) = &join_table {
                if jt.eq_ignore_ascii_case(cte_name) {
                    cte_alias = join_alias.or_else(|| Some(jt.clone()));
                    if let Some(cond) = extract_join_on_condition(&join.join_operator) {
                        join_on_expr = Some(cond.clone());
                    }
                } else {
                    real_table_alias = join_alias.or_else(|| Some(jt.clone()));
                    if join_on_expr.is_none()
                        && let Some(cond) = extract_join_on_condition(&join.join_operator)
                    {
                        join_on_expr = Some(cond.clone());
                    }
                }
            }
        }
    }

    // Extract the join link from the ON condition.
    let join_link = if let (Some(real_alias), Some(cte_al), Some(on_expr)) =
        (&real_table_alias, &cte_alias, &join_on_expr)
    {
        extract_equi_link(on_expr, real_alias, cte_al)
    } else {
        None
    };

    let mut filters = Vec::new();
    if let Some(where_expr) = &select.selection {
        let converted = crate::resolver::expr::convert_expr(where_expr)?;
        filters.push(Filter {
            expr: FilterExpr::Expr(converted),
        });
    }

    Ok((filters, join_link))
}

/// Extract `(collection_field, cte_field)` from an equi-join ON clause.
fn extract_equi_link(
    expr: &ast::Expr,
    real_alias: &str,
    cte_alias: &str,
) -> Option<(String, String)> {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            let left_parts = extract_qualified_column(left)?;
            let right_parts = extract_qualified_column(right)?;

            if left_parts.0.eq_ignore_ascii_case(real_alias)
                && right_parts.0.eq_ignore_ascii_case(cte_alias)
            {
                Some((left_parts.1, right_parts.1))
            } else if right_parts.0.eq_ignore_ascii_case(real_alias)
                && left_parts.0.eq_ignore_ascii_case(cte_alias)
            {
                Some((right_parts.1, left_parts.1))
            } else {
                None
            }
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => extract_equi_link(left, real_alias, cte_alias)
            .or_else(|| extract_equi_link(right, real_alias, cte_alias)),
        _ => None,
    }
}

fn extract_qualified_column(expr: &ast::Expr) -> Option<(String, String)> {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((normalize_ident(&parts[0]), normalize_ident(&parts[1])))
        }
        _ => None,
    }
}

fn extract_table_name(relation: &ast::TableFactor) -> Option<String> {
    match relation {
        ast::TableFactor::Table { name, .. } => normalize_object_name_checked(name).ok(),
        _ => None,
    }
}

fn extract_table_alias(relation: &ast::TableFactor) -> Option<String> {
    match relation {
        ast::TableFactor::Table { alias, .. } => alias.as_ref().map(|a| normalize_ident(&a.name)),
        _ => None,
    }
}

fn extract_join_on_condition(op: &ast::JoinOperator) -> Option<&ast::Expr> {
    use ast::JoinOperator::*;
    let constraint = match op {
        Inner(c) | LeftOuter(c) | RightOuter(c) | FullOuter(c) => c,
        _ => return None,
    };
    match constraint {
        ast::JoinConstraint::On(expr) => Some(expr),
        _ => None,
    }
}

fn plan_cte_branch(
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
            super::select::plan_query(&query, catalog, functions, temporal)
        }
        _ => Err(SqlError::Unsupported {
            detail: "CTE branch must be SELECT".into(),
        }),
    }
}

fn extract_collection(plan: &SqlPlan) -> String {
    match plan {
        SqlPlan::Scan { collection, .. } => collection.clone(),
        _ => String::new(),
    }
}

fn extract_filters(plan: &SqlPlan) -> Vec<Filter> {
    match plan {
        SqlPlan::Scan { filters, .. } => filters.clone(),
        _ => Vec::new(),
    }
}
