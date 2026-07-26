// SPDX-License-Identifier: Apache-2.0

//! AST extraction helpers shared by the DML planners: pulling a bare table
//! name out of a join tree, and pulling primary-key point-lookup keys out of
//! a `WHERE` clause.

use sqlparser::ast;

use super::value_convert::expr_to_sql_value;
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::types::*;

pub(crate) fn extract_table_name_from_table_with_joins(
    table: &ast::TableWithJoins,
) -> Result<String> {
    match &table.relation {
        ast::TableFactor::Table { name, .. } => Ok(normalize_object_name_checked(name)?),
        _ => Err(SqlError::Unsupported {
            detail: "non-table target in DML".into(),
        }),
    }
}

/// Extract point-operation keys from WHERE clause (WHERE pk = literal OR pk IN (...)).
pub fn extract_point_keys(selection: Option<&ast::Expr>, info: &CollectionInfo) -> Vec<SqlValue> {
    let pk = match &info.primary_key {
        Some(pk) => pk.clone(),
        None => return Vec::new(),
    };

    let expr = match selection {
        Some(e) => e,
        None => return Vec::new(),
    };

    let mut keys = Vec::new();
    collect_pk_equalities(expr, &pk, &mut keys);
    keys
}

fn collect_pk_equalities(expr: &ast::Expr, pk: &str, keys: &mut Vec<SqlValue>) {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if is_column(left, pk)
                && let Ok(v) = expr_to_sql_value(right)
            {
                keys.push(v);
            } else if is_column(right, pk)
                && let Ok(v) = expr_to_sql_value(left)
            {
                keys.push(v);
            }
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Or,
            right,
        } => {
            collect_pk_equalities(left, pk, keys);
            collect_pk_equalities(right, pk, keys);
        }
        ast::Expr::InList {
            expr: inner,
            list,
            negated: false,
        } if is_column(inner, pk) => {
            for item in list {
                if let Ok(v) = expr_to_sql_value(item) {
                    keys.push(v);
                }
            }
        }
        _ => {}
    }
}

fn is_column(expr: &ast::Expr, name: &str) -> bool {
    match expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident) == name,
        // Three or more parts: schema.table.col — never matches a plain pk name.
        ast::Expr::CompoundIdentifier(parts) if parts.len() >= 3 => false,
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            normalize_ident(&parts[1]) == name
        }
        _ => false,
    }
}
