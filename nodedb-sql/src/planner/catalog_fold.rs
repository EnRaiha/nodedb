// SPDX-License-Identifier: Apache-2.0

//! Plan-time constant folding for catalog-dependent expressions.
//!
//! `fold_catalog_exprs_in_plan` walks a `SqlPlan` and replaces
//! `Cast { expr: Literal(String(s)), to_type: "regclass" }` and
//! `Cast { expr: Literal(String(s)), to_type: "regtype" }` nodes with
//! their resolved OID literals using the `SqlCatalog` trait.  This keeps
//! the data-plane evaluator pure (no catalog/session context) while still
//! supporting the `'name'::regclass` / `'name'::regtype` PostgreSQL idiom.

use nodedb_types::DatabaseId;

use crate::catalog::SqlCatalog;
use crate::types::{Filter, FilterExpr, SqlExpr, SqlPlan, SqlValue};

/// Walk every `Filter` in `plan` and fold catalog-dependent cast expressions
/// to their constant OID equivalents.
///
/// Mutates the plan in-place (via owned `SqlPlan`). The caller owns the plan
/// after `plan_sql` returns; this pass runs between planning and physical
/// conversion where the catalog is still available.
pub fn fold_catalog_exprs_in_plan(
    plan: SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> SqlPlan {
    walk_plan(plan, catalog, database_id, tenant_id)
}

fn walk_plan(
    plan: SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> SqlPlan {
    match plan {
        SqlPlan::Scan {
            collection,
            alias,
            engine,
            mut filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            temporal,
        } => {
            for f in &mut filters {
                fold_filter(f, catalog, database_id, tenant_id);
            }
            SqlPlan::Scan {
                collection,
                alias,
                engine,
                filters,
                projection,
                sort_keys,
                limit,
                offset,
                distinct,
                window_functions,
                temporal,
            }
        }

        SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            condition,
            limit,
            projection,
            mut filters,
        } => {
            for f in &mut filters {
                fold_filter(f, catalog, database_id, tenant_id);
            }
            let condition = condition.map(|e| fold_expr(e, catalog, database_id, tenant_id));
            SqlPlan::Join {
                left: Box::new(walk_plan(*left, catalog, database_id, tenant_id)),
                right: Box::new(walk_plan(*right, catalog, database_id, tenant_id)),
                on,
                join_type,
                condition,
                limit,
                projection,
                filters,
            }
        }

        // All other plan variants are passed through unchanged.
        other => other,
    }
}

fn fold_filter(
    filter: &mut Filter,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) {
    fold_filter_expr(&mut filter.expr, catalog, database_id, tenant_id);
}

fn fold_filter_expr(
    expr: &mut FilterExpr,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) {
    match expr {
        FilterExpr::Expr(sql_expr) => {
            let owned = std::mem::replace(sql_expr, SqlExpr::Wildcard);
            *sql_expr = fold_expr(owned, catalog, database_id, tenant_id);
        }
        FilterExpr::And(children) | FilterExpr::Or(children) => {
            for child in children {
                fold_filter_expr(&mut child.expr, catalog, database_id, tenant_id);
            }
        }
        FilterExpr::Not(child) => {
            fold_filter_expr(&mut child.expr, catalog, database_id, tenant_id);
        }
        // Simple comparison, InList, Between, IsNull, IsNotNull — no sub-expressions to fold.
        FilterExpr::Comparison { .. }
        | FilterExpr::InList { .. }
        | FilterExpr::Between { .. }
        | FilterExpr::IsNull { .. }
        | FilterExpr::IsNotNull { .. } => {}
    }
}

/// Recursively fold catalog-dependent expressions to constants.
fn fold_expr(
    expr: SqlExpr,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> SqlExpr {
    match expr {
        // `'name'::regclass` or `'name'::regtype` → OID literal.
        // We match on both in a single arm to avoid partial-move issues.
        SqlExpr::Cast {
            expr: inner_expr,
            to_type,
        } => {
            let upper = to_type.to_ascii_uppercase();
            if upper == "REGCLASS" {
                if let SqlExpr::Literal(SqlValue::String(ref name)) = *inner_expr
                    && let Some(oid) = catalog.resolve_regclass(database_id, tenant_id, name)
                {
                    return SqlExpr::Literal(SqlValue::Int(oid));
                }
            } else if upper == "REGTYPE"
                && let SqlExpr::Literal(SqlValue::String(ref name)) = *inner_expr
                && let Some(oid) = catalog.resolve_regtype(name)
            {
                return SqlExpr::Literal(SqlValue::Int(oid));
            }
            // Non-catalog cast: recurse into inner expression.
            SqlExpr::Cast {
                expr: Box::new(fold_expr(*inner_expr, catalog, database_id, tenant_id)),
                to_type,
            }
        }

        // Recurse into BinaryOp children.
        SqlExpr::BinaryOp { left, op, right } => SqlExpr::BinaryOp {
            left: Box::new(fold_expr(*left, catalog, database_id, tenant_id)),
            op,
            right: Box::new(fold_expr(*right, catalog, database_id, tenant_id)),
        },

        // Recurse into InList.
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => SqlExpr::InList {
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
            list: list
                .into_iter()
                .map(|e| fold_expr(e, catalog, database_id, tenant_id))
                .collect(),
            negated,
        },

        // Recurse into IsNull.
        SqlExpr::IsNull { expr, negated } => SqlExpr::IsNull {
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
            negated,
        },

        // Recurse into UnaryOp.
        SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
            op,
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
        },

        // Recurse into ArrayLiteral elements.
        SqlExpr::ArrayLiteral(elems) => SqlExpr::ArrayLiteral(
            elems
                .into_iter()
                .map(|e| fold_expr(e, catalog, database_id, tenant_id))
                .collect(),
        ),

        // Recurse into Function args.
        SqlExpr::Function {
            name,
            args,
            distinct,
        } => SqlExpr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| fold_expr(a, catalog, database_id, tenant_id))
                .collect(),
            distinct,
        },

        // Recurse into Between bounds.
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => SqlExpr::Between {
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
            low: Box::new(fold_expr(*low, catalog, database_id, tenant_id)),
            high: Box::new(fold_expr(*high, catalog, database_id, tenant_id)),
            negated,
        },

        // Recurse into Like.
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => SqlExpr::Like {
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
            pattern: Box::new(fold_expr(*pattern, catalog, database_id, tenant_id)),
            negated,
            case_insensitive,
        },

        // Recurse into Case.
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => SqlExpr::Case {
            operand: operand.map(|e| Box::new(fold_expr(*e, catalog, database_id, tenant_id))),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| {
                    (
                        fold_expr(w, catalog, database_id, tenant_id),
                        fold_expr(t, catalog, database_id, tenant_id),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|e| Box::new(fold_expr(*e, catalog, database_id, tenant_id))),
        },

        // Leaf nodes: Column, Literal, Wildcard, Subquery — pass through.
        leaf => leaf,
    }
}
