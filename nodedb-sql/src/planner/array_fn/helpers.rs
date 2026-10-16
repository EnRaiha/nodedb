// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for ARRAY_* function planners: argument extraction
//! and validation against the array catalog.

use sqlparser::ast;

use nodedb_types::Value;

use crate::catalog::ArrayCatalogView;
use crate::error::{Result, SqlError};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::parser::object_literal::parse_object_literal_complete;
use crate::types::SqlCatalog;
use crate::types_array::{ArrayCoordLiteral, ArraySliceAst, NamedDimRange};

pub(super) fn collect_args(args: &[ast::FunctionArg]) -> Vec<ast::Expr> {
    args.iter()
        .filter_map(|a| match a {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
            _ => None,
        })
        .collect()
}

pub(super) fn require_array_name(
    args: &[ast::Expr],
    idx: usize,
    fn_name: &str,
    catalog: &dyn SqlCatalog,
) -> Result<String> {
    let expr = args.get(idx).ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: missing array name at arg {idx}"),
    })?;
    let name = match expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident),
        ast::Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualified: String = parts
                .iter()
                .map(normalize_ident)
                .collect::<Vec<_>>()
                .join(".");
            return Err(SqlError::Unsupported {
                detail: format!(
                    "schema-qualified array name '{qualified}': {SCHEMA_QUALIFIED_MSG}"
                ),
            });
        }
        _ => expect_string_literal(expr, &format!("{fn_name} array name"))?,
    };
    if !catalog.array_exists(&name) {
        return Err(SqlError::Unsupported {
            detail: format!("{fn_name}: array '{name}' not found"),
        });
    }
    Ok(name)
}

pub(super) fn expect_string_literal(expr: &ast::Expr, ctx: &str) -> Result<String> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) => Ok(s.clone()),
            other => Err(SqlError::Unsupported {
                detail: format!("{ctx}: expected string literal, got {other}"),
            }),
        },
        ast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
        _ => Err(SqlError::Unsupported {
            detail: format!("{ctx}: expected string literal, got {expr}"),
        }),
    }
}

pub(super) fn expect_u32(expr: &ast::Expr, ctx: &str) -> Result<u32> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<u32>().map_err(|_| SqlError::TypeMismatch {
                detail: format!("{ctx}: expected u32, got {n}"),
            }),
            other => Err(SqlError::TypeMismatch {
                detail: format!("{ctx}: expected number, got {other}"),
            }),
        },
        _ => Err(SqlError::TypeMismatch {
            detail: format!("{ctx}: expected number literal, got {expr}"),
        }),
    }
}

pub(super) fn expect_string_array(expr: &ast::Expr, ctx: &str) -> Result<Vec<String>> {
    let elems = match expr {
        ast::Expr::Array(arr) => arr.elem.clone(),
        ast::Expr::Function(f)
            if matches!(
                crate::parser::normalize::normalize_object_name_checked(&f.name)
                    .as_deref()
                    .unwrap_or(""),
                "make_array" | "array"
            ) =>
        {
            match &f.args {
                ast::FunctionArguments::List(list) => collect_args(&list.args),
                _ => Vec::new(),
            }
        }
        _ => {
            return Err(SqlError::Unsupported {
                detail: format!("{ctx}: expected array literal, got {expr}"),
            });
        }
    };
    elems
        .iter()
        .map(|e| expect_string_literal(e, ctx))
        .collect()
}

pub(super) fn is_null_literal(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Value(v) if matches!(v.value, ast::Value::Null)
    )
}

/// Parse a slice-predicate argument of `fn_name` over array `name`.
///
/// The argument is a quoted string holding the brace-form object literal
/// `'{dim: [lo, hi], ...}'`: the PostgreSQL dialect does not accept a bare
/// `{...}` in expression position. Each named dim must exist on the array.
pub(super) fn parse_slice_arg(
    expr: &ast::Expr,
    fn_name: &str,
    name: &str,
    view: &ArrayCatalogView,
) -> Result<ArraySliceAst> {
    let slice_str = expect_string_literal(expr, &format!("{fn_name} slice predicate"))?;
    let parsed =
        parse_object_literal_complete(&slice_str).ok_or_else(|| SqlError::Unsupported {
            detail: format!("{fn_name}: slice predicate must be an object literal: {slice_str}"),
        })?;
    let map = parsed.map_err(|detail| SqlError::Unsupported {
        detail: format!("{fn_name}: slice parse: {detail}"),
    })?;
    let mut dim_ranges: Vec<NamedDimRange> = Vec::with_capacity(map.len());
    for (dim, val) in map {
        if !view.dims.iter().any(|d| d.name == dim) {
            return Err(SqlError::Unsupported {
                detail: format!("{fn_name}: array '{name}' has no dim '{dim}'"),
            });
        }
        let arr = match val {
            Value::Array(a) if a.len() == 2 => a,
            _ => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "{fn_name}: dim '{dim}' range must be a 2-element array [lo, hi]"
                    ),
                });
            }
        };
        let lo = value_to_coord_literal(&arr[0], &dim)?;
        let hi = value_to_coord_literal(&arr[1], &dim)?;
        dim_ranges.push(NamedDimRange { dim, lo, hi });
    }
    Ok(ArraySliceAst { dim_ranges })
}

pub(super) fn value_to_coord_literal(v: &Value, dim: &str) -> Result<ArrayCoordLiteral> {
    match v {
        Value::Integer(i) => Ok(ArrayCoordLiteral::Int64(*i)),
        Value::Float(f) => Ok(ArrayCoordLiteral::Float64(*f)),
        Value::String(s) => Ok(ArrayCoordLiteral::String(s.clone())),
        other => Err(SqlError::TypeMismatch {
            detail: format!("ARRAY_SLICE dim '{dim}' bound: unsupported value kind {other:?}"),
        }),
    }
}
