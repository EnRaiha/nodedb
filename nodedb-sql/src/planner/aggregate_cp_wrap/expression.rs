// SPDX-License-Identifier: Apache-2.0

//! Group-row column qualification.

use crate::types::SqlExpr;

/// How an expression over the input relations reads a finalized group row.
pub(super) struct GroupRowNames {
    /// `(bare name, row name)` of each GROUP BY key the row carries under
    /// another name: an aliased key carries its alias.
    pub renames: Vec<(String, String)>,
    /// `(Debug rendering, row name)` of each computed GROUP BY key. The row
    /// carries the key's value, not the input columns it reads.
    pub computed_keys: Vec<(String, String)>,
}

/// `expr` read from a finalized group row.
///
/// A sub-expression equal to a computed GROUP BY key becomes a reference to
/// the key's row column. `SqlExpr` has no `PartialEq` (it can carry a subquery
/// plan), so the match compares the `Debug` rendering. Both sides pass through
/// `convert_expr`, so equal expressions render identically. Every other column
/// reference is stripped of its table qualifier and renamed to the name the
/// row carries it under.
pub(super) fn group_row_columns(expr: SqlExpr, names: &GroupRowNames) -> SqlExpr {
    if !names.computed_keys.is_empty() && !matches!(expr, SqlExpr::Column { .. }) {
        let rendered = format!("{expr:?}");
        if let Some((_, name)) = names.computed_keys.iter().find(|(key, _)| *key == rendered) {
            return SqlExpr::Column {
                table: None,
                name: name.clone(),
            };
        }
    }
    let go = |e: SqlExpr| group_row_columns(e, names);
    let go_box = |e: Box<SqlExpr>| Box::new(group_row_columns(*e, names));
    match expr {
        SqlExpr::Column { name, .. } => {
            let name = names
                .renames
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
