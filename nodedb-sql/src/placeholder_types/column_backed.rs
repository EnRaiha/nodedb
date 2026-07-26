// SPDX-License-Identifier: Apache-2.0

//! The forms whose type comes from a catalog column: comparisons, `IN`,
//! `BETWEEN`, `UPDATE ... SET`, and `INSERT ... VALUES`.
//!
//! Unlike the catalog-free pass this is an explicit recursive descent rather
//! than a sqlparser `Visitor`, because every conclusion depends on which
//! relations are in scope at that point in the tree — and a `Visitor` has no
//! way to carry a scope down (or pop it back off) as it descends.
//!
//! Forms deliberately left unresolved: `ON CONFLICT DO UPDATE SET`,
//! `RETURNING`, function arguments, `CASE` branches, arithmetic operands,
//! `ANY` / `ALL`, array elements and window clauses. Each would need a type
//! rule of its own; guessing one is worse than the text-format fallback.

use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, Insert, ObjectName, Query, SetExpr,
    Statement, TableObject, Update, UpdateTableFromKind,
};

use super::scope::{Scope, column_of, lookup_relation};
use super::slots::{InferenceContext, InferredParamType, placeholder_index};
use crate::catalog::SqlCatalog;
use crate::types::ColumnInfo;

/// Resolve every catalog-backed placeholder position in `statement`.
pub(super) fn infer_from_statement(
    ctx: &mut InferenceContext,
    catalog: &dyn SqlCatalog,
    statement: &Statement,
) {
    match statement {
        Statement::Query(query) => walk_query(ctx, catalog, query),
        Statement::Insert(insert) => infer_insert(ctx, catalog, insert),
        Statement::Update(update) => infer_update(ctx, catalog, update),
        Statement::Delete(delete) => infer_delete(ctx, catalog, delete),
        // Every other statement kind carries no column-backed placeholder
        // position this pass types. Intentional: DDL, session commands and
        // the DSL statements all fall here.
        _ => {}
    }
}

fn walk_query(ctx: &mut InferenceContext, catalog: &dyn SqlCatalog, query: &Query) {
    if let Some(with) = &query.with {
        // A CTE body is its own scope. The CTE *name* is not resolvable
        // against the catalog, so any outer relation referencing it makes
        // that outer scope opaque — which is the safe outcome.
        for cte in &with.cte_tables {
            walk_query(ctx, catalog, &cte.query);
        }
    }
    walk_set_expr(ctx, catalog, &query.body);
}

fn walk_set_expr(ctx: &mut InferenceContext, catalog: &dyn SqlCatalog, body: &SetExpr) {
    match body {
        SetExpr::Select(select) => {
            let scope = Scope::from_tables(catalog, &select.from);
            if let Some(selection) = &select.selection {
                infer_predicate(ctx, catalog, &scope, selection);
            }
            if let Some(having) = &select.having {
                infer_predicate(ctx, catalog, &scope, having);
            }
        }
        SetExpr::Query(query) => walk_query(ctx, catalog, query),
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(ctx, catalog, left);
            walk_set_expr(ctx, catalog, right);
        }
        SetExpr::Insert(statement)
        | SetExpr::Update(statement)
        | SetExpr::Delete(statement)
        | SetExpr::Merge(statement) => infer_from_statement(ctx, catalog, statement),
        // A bare `VALUES` body has no target column list to map against, and
        // `TABLE t` has no expressions at all. Intentional: nothing to type.
        SetExpr::Values(_) | SetExpr::Table(_) => {}
    }
}

/// Walk a `WHERE` / `HAVING` predicate, typing every placeholder the scope
/// pins down and descending into nested subqueries with their own scope.
fn infer_predicate(
    ctx: &mut InferenceContext,
    catalog: &dyn SqlCatalog,
    scope: &Scope,
    expr: &Expr,
) {
    match expr {
        Expr::Nested(inner) => infer_predicate(ctx, catalog, scope, inner),
        Expr::UnaryOp { expr: inner, .. } => infer_predicate(ctx, catalog, scope, inner),
        Expr::BinaryOp { left, op, right } => {
            if is_comparison(op) {
                // Either operand may be the column and either the parameter;
                // `$1 = col` is as valid as `col = $1`.
                record_comparison(ctx, scope, left, right);
                record_comparison(ctx, scope, right, left);
            }
            // Non-comparison operators (`AND`, `OR`, arithmetic, ...) type
            // nothing themselves, but predicates and subqueries can still be
            // nested under them.
            infer_predicate(ctx, catalog, scope, left);
            infer_predicate(ctx, catalog, scope, right);
        }
        Expr::InList {
            expr: target, list, ..
        } => {
            if let Some(column) = column_ref(scope, target) {
                let ty = InferredParamType::from_column(column);
                for item in list {
                    ctx.record_if_placeholder(item, ty.clone());
                }
            }
        }
        Expr::Between {
            expr: target,
            low,
            high,
            ..
        } => {
            if let Some(column) = column_ref(scope, target) {
                let ty = InferredParamType::from_column(column);
                ctx.record_if_placeholder(low, ty.clone());
                ctx.record_if_placeholder(high, ty);
            }
        }
        Expr::InSubquery { subquery, .. } | Expr::Exists { subquery, .. } => {
            walk_query(ctx, catalog, subquery)
        }
        Expr::Subquery(query) => walk_query(ctx, catalog, query),
        // Every other predicate shape — `IS NULL`, `LIKE`, `ANY`/`ALL`,
        // function calls, `CASE` — is not a form this pass types. Intentional.
        _ => {}
    }
}

/// The comparison operators whose operands share a type.
///
/// `!=` and `<>` both parse to [`BinaryOperator::NotEq`].
fn is_comparison(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    )
}

/// Type `param_side` from `column_side` when the first is a bare `$N` and the
/// second a bare column reference the scope resolves unambiguously.
fn record_comparison(
    ctx: &mut InferenceContext,
    scope: &Scope,
    column_side: &Expr,
    param_side: &Expr,
) {
    let Some(index) = placeholder_index(param_side) else {
        return;
    };
    let Some(column) = column_ref(scope, column_side) else {
        return;
    };
    ctx.record(index, InferredParamType::from_column(column));
}

/// The catalog column `expr` names, when `expr` is a bare column reference.
///
/// Anything computed — `col + 1`, `lower(col)`, a subquery — is not a bare
/// column reference and does not carry the column's type, so it resolves to
/// `None`.
fn column_ref<'a>(scope: &'a Scope, expr: &Expr) -> Option<&'a ColumnInfo> {
    match expr {
        Expr::Nested(inner) => column_ref(scope, inner),
        Expr::Identifier(ident) => scope.resolve_column(&[ident.value.as_str()]),
        Expr::CompoundIdentifier(parts) => {
            let parts: Vec<&str> = parts.iter().map(|ident| ident.value.as_str()).collect();
            scope.resolve_column(&parts)
        }
        // Not a column reference. Intentional: leave the position unresolved.
        _ => None,
    }
}

fn infer_update(ctx: &mut InferenceContext, catalog: &dyn SqlCatalog, update: &Update) {
    let mut scope = Scope::from_tables(catalog, core::slice::from_ref(&update.table));
    match &update.from {
        Some(UpdateTableFromKind::BeforeSet(tables))
        | Some(UpdateTableFromKind::AfterSet(tables)) => scope.add_tables(catalog, tables),
        None => {}
    }

    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(name) = &assignment.target else {
            // `SET (a, b) = (...)` assigns a tuple; mapping its elements is
            // not a form this pass types. Intentional.
            continue;
        };
        let Some(parts) = object_name_parts(name) else {
            // A dialect-specific name-producing function in the target path:
            // not a form this pass types. Intentional.
            continue;
        };
        let Some(column) = scope.resolve_column(&parts) else {
            continue;
        };
        ctx.record_if_placeholder(&assignment.value, InferredParamType::from_column(column));
    }

    if let Some(selection) = &update.selection {
        infer_predicate(ctx, catalog, &scope, selection);
    }
}

/// The dotted parts of an assignment target, or `None` when any part is not
/// a plain identifier.
fn object_name_parts(name: &ObjectName) -> Option<Vec<&str>> {
    name.0
        .iter()
        .map(|part| part.as_ident().map(|ident| ident.value.as_str()))
        .collect()
}

fn infer_delete(ctx: &mut InferenceContext, catalog: &dyn SqlCatalog, delete: &Delete) {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let mut scope = Scope::from_tables(catalog, tables);
    if let Some(using) = &delete.using {
        scope.add_tables(catalog, using);
    }
    if let Some(selection) = &delete.selection {
        infer_predicate(ctx, catalog, &scope, selection);
    }
}

fn infer_insert(ctx: &mut InferenceContext, catalog: &dyn SqlCatalog, insert: &Insert) {
    let Some(source) = &insert.source else {
        return;
    };
    // `INSERT INTO ... SELECT` and any other query source: the projection
    // positions are not typed here, but predicates inside the source query
    // still are.
    walk_query(ctx, catalog, source);

    let TableObject::TableName(name) = &insert.table else {
        // `INSERT INTO TABLE FUNCTION ...`: no catalog relation behind it.
        return;
    };
    let Some(info) = lookup_relation(catalog, name) else {
        return;
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return;
    };

    // The target columns, in `VALUES` position order.
    let targets: Vec<&ColumnInfo> = if insert.columns.is_empty() {
        // No explicit column list: positional against the collection's
        // declared column order. A collection that declares none (a
        // schemaless document store) has no order to be positional against,
        // so nothing is typed.
        info.columns.iter().collect()
    } else {
        let mut targets = Vec::with_capacity(insert.columns.len());
        for ident in &insert.columns {
            let Some(column) = column_of(&info, &ident.value) else {
                // A named target the catalog does not declare makes the whole
                // positional mapping untrustworthy, not just this one slot.
                return;
            };
            targets.push(column);
        }
        targets
    };
    if targets.is_empty() {
        return;
    }

    for row in &values.rows {
        // An arity mismatch means the positional mapping is not the one the
        // planner will use (a synthesized key column, a malformed row), so
        // typing against it would be a guess.
        if row.len() != targets.len() {
            continue;
        }
        for (column, expr) in targets.iter().zip(row.iter()) {
            ctx.record_if_placeholder(expr, InferredParamType::from_column(column));
        }
    }
}
