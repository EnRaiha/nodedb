// SPDX-License-Identifier: Apache-2.0

//! Group-row column qualification.

use crate::types::SqlExpr;

/// `expr` with every column reference stripped of its table qualifier and
/// renamed to the key a finalized group row carries it under.
///
/// A finalized group row keys its columns by bare name, and an aliased GROUP
/// BY key by its alias: `renames` maps each aliased key's bare name to it.
pub(super) fn group_row_columns(expr: SqlExpr, renames: &[(String, String)]) -> SqlExpr {
    let go = |e: SqlExpr| group_row_columns(e, renames);
    let go_box = |e: Box<SqlExpr>| Box::new(group_row_columns(*e, renames));
    match expr {
        SqlExpr::Column { name, .. } => {
            let name = renames
                .iter()
                .find(|(from, _)| *from == name)
                .map_or(name, |(_, to)| to.clone());
            SqlExpr::Column { table: None, name }
        }
        SqlExpr::Function {
            name,
            args,
            distinct,
        } => SqlExpr::Function {
            name,
            args: args.into_iter().map(go).collect(),
            distinct,
        },
        SqlExpr::BinaryOp { left, op, right } => SqlExpr::BinaryOp {
            left: go_box(left),
            op,
            right: go_box(right),
        },
        SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
            op,
            expr: go_box(expr),
        },
        SqlExpr::Cast { expr, to_type } => SqlExpr::Cast {
            expr: go_box(expr),
            to_type,
        },
        SqlExpr::IsNull { expr, negated } => SqlExpr::IsNull {
            expr: go_box(expr),
            negated,
        },
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => SqlExpr::Case {
            operand: operand.map(go_box),
            when_then: when_then
                .into_iter()
                .map(|(when, then)| (go(when), go(then)))
                .collect(),
            else_expr: else_expr.map(go_box),
        },
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => SqlExpr::InList {
            expr: go_box(expr),
            list: list.into_iter().map(go).collect(),
            negated,
        },
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => SqlExpr::Between {
            expr: go_box(expr),
            low: go_box(low),
            high: go_box(high),
            negated,
        },
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => SqlExpr::Like {
            expr: go_box(expr),
            pattern: go_box(pattern),
            negated,
            case_insensitive,
        },
        SqlExpr::ArrayLiteral(items) => SqlExpr::ArrayLiteral(items.into_iter().map(go).collect()),
        SqlExpr::Literal(_) | SqlExpr::Subquery(_) | SqlExpr::Wildcard => expr,
    }
}
