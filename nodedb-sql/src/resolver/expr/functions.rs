// SPDX-License-Identifier: Apache-2.0

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::types::*;

use super::convert::convert_expr_depth;

pub(super) fn convert_function_depth(func: &ast::Function, depth: &mut usize) -> Result<SqlExpr> {
    // Intercept PG FTS surface functions and lower them to pg_* internal names
    // before the generic path runs.
    if func.name.0.len() == 1 {
        let raw_name = match &func.name.0[0] {
            ast::ObjectNamePart::Identifier(ident) => ident.value.to_ascii_lowercase(),
            _ => String::new(),
        };
        if let Some(expr) = intercept_fts_function(&raw_name, func, depth)? {
            return Ok(expr);
        }
    }

    if func.name.0.len() > 1 {
        let qualified: String = func
            .name
            .0
            .iter()
            .map(|p| match p {
                ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(".");
        return Err(SqlError::Unsupported {
            detail: format!("schema-qualified function name '{qualified}': {SCHEMA_QUALIFIED_MSG}"),
        });
    }
    let name = func
        .name
        .0
        .iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".");

    // `current_schemas(bool)` — PostgreSQL catalog function returning TEXT[].
    // Fold to a literal array at parse time so `= ANY(current_schemas(...))` works
    // without threading session context into the data-plane evaluator.
    if (name == "current_schemas" || name == "current_schema")
        && let Some(expr) = intercept_catalog_function(&name, func, depth)?
    {
        return Ok(expr);
    }

    let args = match &func.args {
        ast::FunctionArguments::None => Vec::new(),
        ast::FunctionArguments::Subquery(_) => {
            return Err(SqlError::Unsupported {
                detail: "subquery in function args".into(),
            });
        }
        ast::FunctionArguments::List(arg_list) => arg_list
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    Some(convert_expr_depth(e, depth))
                }
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                    Some(Ok(SqlExpr::Wildcard))
                }
                ast::FunctionArg::Named {
                    arg: ast::FunctionArgExpr::Expr(e),
                    ..
                } => Some(convert_expr_depth(e, depth)),
                _ => None,
            })
            .collect::<Result<Vec<_>>>()?,
    };

    let distinct = match &func.args {
        ast::FunctionArguments::List(arg_list) => {
            matches!(
                arg_list.duplicate_treatment,
                Some(ast::DuplicateTreatment::Distinct)
            )
        }
        _ => false,
    };

    Ok(SqlExpr::Function {
        name,
        args,
        distinct,
    })
}

/// Intercept PG FTS surface functions and lower them to `pg_*` internal
/// function-call `SqlExpr` nodes.  Returns `Ok(None)` for non-FTS names.
///
/// `ts_rank_cd` is rejected with `SqlError::Unsupported` as per the
/// approved design.
fn intercept_fts_function(
    name: &str,
    func: &ast::Function,
    depth: &mut usize,
) -> Result<Option<SqlExpr>> {
    use crate::functions::fts_ops::pg_fts_funcs;

    let args = collect_function_args(func, depth)?;
    match name {
        "to_tsvector" => Ok(Some(SqlExpr::Function {
            name: "pg_to_tsvector".into(),
            args,
            distinct: false,
        })),
        "to_tsquery" => Ok(Some(pg_fts_funcs::lower_pg_to_tsquery(args))),
        "plainto_tsquery" => Ok(Some(pg_fts_funcs::lower_pg_plainto_tsquery(args))),
        "phraseto_tsquery" => Ok(Some(pg_fts_funcs::lower_phraseto_tsquery(args))),
        "websearch_to_tsquery" => Ok(Some(pg_fts_funcs::lower_pg_websearch_to_tsquery(args))),
        "ts_rank" => Ok(Some(SqlExpr::Function {
            name: "pg_ts_rank".into(),
            args,
            distinct: false,
        })),
        "ts_rank_cd" => Err(SqlError::Unsupported {
            detail: "ts_rank_cd is not supported; use ts_rank or bm25_score instead".into(),
        }),
        "ts_headline" => Ok(Some(SqlExpr::Function {
            name: "pg_ts_headline".into(),
            args,
            distinct: false,
        })),
        _ => Ok(None),
    }
}

/// Intercept PostgreSQL catalog functions and fold them to literal arrays.
///
/// `current_schemas(true)` → `ARRAY['pg_catalog', 'public']`
/// `current_schemas(false)` → `ARRAY['public']`
/// `current_schema()` → `'public'`
///
/// Returns `Ok(Some(expr))` when the function is folded, `Ok(None)` otherwise.
fn intercept_catalog_function(
    name: &str,
    func: &ast::Function,
    depth: &mut usize,
) -> Result<Option<SqlExpr>> {
    let args = collect_function_args(func, depth)?;
    match name {
        "current_schemas" => {
            // Determine whether to include implicit schemas (pg_catalog).
            let include_implicit = match args.first() {
                Some(SqlExpr::Literal(SqlValue::Bool(b))) => *b,
                // Default to false when arg is missing or non-literal.
                _ => false,
            };
            let mut schemas: Vec<SqlExpr> = Vec::new();
            if include_implicit {
                schemas.push(SqlExpr::Literal(SqlValue::String("pg_catalog".into())));
            }
            schemas.push(SqlExpr::Literal(SqlValue::String("public".into())));
            Ok(Some(SqlExpr::ArrayLiteral(schemas)))
        }
        "current_schema" => Ok(Some(SqlExpr::Literal(SqlValue::String("public".into())))),
        _ => Ok(None),
    }
}

/// Collect function call arguments, converting each `Expr` to `SqlExpr`.
fn collect_function_args(func: &ast::Function, depth: &mut usize) -> Result<Vec<SqlExpr>> {
    match &func.args {
        ast::FunctionArguments::None => Ok(Vec::new()),
        ast::FunctionArguments::Subquery(_) => Err(SqlError::Unsupported {
            detail: "subquery in function args".into(),
        }),
        ast::FunctionArguments::List(arg_list) => arg_list
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    Some(convert_expr_depth(e, depth))
                }
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                    Some(Ok(SqlExpr::Wildcard))
                }
                ast::FunctionArg::Named {
                    arg: ast::FunctionArgExpr::Expr(e),
                    ..
                } => Some(convert_expr_depth(e, depth)),
                _ => None,
            })
            .collect::<Result<Vec<_>>>(),
    }
}
