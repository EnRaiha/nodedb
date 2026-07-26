// SPDX-License-Identifier: Apache-2.0

//! Convert sqlparser AST expressions to our SqlExpr IR.

use sqlparser::ast::{self, Expr, UnaryOperator, Value};

use crate::error::{Result, SqlError};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::types::*;

use super::binary_ops::{convert_binary_op, convert_unary_op};
use super::functions::convert_function_depth;
use super::value::{convert_value, parse_interval_to_micros};

/// Maximum AST nesting depth accepted by `convert_expr`.
/// Exceeding this limit returns `Err` instead of overflowing the stack.
const MAX_CONVERT_DEPTH: usize = 128;

/// SQL-standard niladic functions: written without parentheses. Parsers
/// emit them as bare identifiers; we promote them to function calls so
/// they fold to a value at plan time instead of resolving to a column.
fn is_zero_arg_keyword_function(name: &str) -> bool {
    matches!(
        name,
        "current_timestamp"
            | "current_date"
            | "current_time"
            | "localtime"
            | "localtimestamp"
            | "current_user"
            | "current_role"
            | "current_schema"
            | "session_user"
            | "user"
            | "version"
    )
}

/// Convert a sqlparser `Expr` to our `SqlExpr`.
pub fn convert_expr(expr: &Expr) -> Result<SqlExpr> {
    convert_expr_depth(expr, &mut 0)
}

/// Internal recursive helper that carries a depth counter to enforce
/// `MAX_CONVERT_DEPTH` and prevent stack overflow on malformed ASTs.
pub(super) fn convert_expr_depth(expr: &Expr, depth: &mut usize) -> Result<SqlExpr> {
    *depth += 1;
    if *depth > MAX_CONVERT_DEPTH {
        return Err(SqlError::Unsupported {
            detail: format!("expression nesting depth exceeds maximum of {MAX_CONVERT_DEPTH}"),
        });
    }
    let result = convert_expr_inner(expr, depth);
    *depth -= 1;
    result
}

fn convert_expr_inner(expr: &Expr, depth: &mut usize) -> Result<SqlExpr> {
    match expr {
        Expr::Identifier(ident) => {
            let name = normalize_ident(ident);
            // SQL-standard zero-arg keyword functions parse as bare
            // identifiers (no parentheses): `SELECT current_timestamp`,
            // `SELECT current_user`, etc. Promote them to function calls
            // so const folding evaluates them like the parenthesised form.
            if is_zero_arg_keyword_function(&name) {
                return Ok(SqlExpr::Function {
                    name,
                    args: vec![],
                    distinct: false,
                });
            }
            Ok(SqlExpr::Column { table: None, name })
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 3 => {
            let qualified: String = parts
                .iter()
                .map(normalize_ident)
                .collect::<Vec<_>>()
                .join(".");
            Err(SqlError::Unsupported {
                detail: format!(
                    "schema-qualified column reference '{qualified}': {SCHEMA_QUALIFIED_MSG}"
                ),
            })
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Ok(SqlExpr::Column {
            table: Some(normalize_ident(&parts[0])),
            name: normalize_ident(&parts[1]),
        }),
        Expr::Value(val) => Ok(SqlExpr::Literal(convert_value(&val.value)?)),
        Expr::BinaryOp { left, op, right } => {
            // JSON and FTS operators are lowered to function calls before the
            // generic binary-op path so they are never passed to
            // convert_binary_op.
            use ast::BinaryOperator;
            let json_fn: Option<&str> = match op {
                BinaryOperator::Arrow => Some("pg_json_get"),
                BinaryOperator::LongArrow => Some("pg_json_get_text"),
                BinaryOperator::HashArrow => Some("pg_json_path_get"),
                BinaryOperator::HashLongArrow => Some("pg_json_path_get_text"),
                BinaryOperator::AtArrow => Some("pg_json_contains"),
                BinaryOperator::ArrowAt => Some("pg_json_contained_by"),
                BinaryOperator::Question => Some("pg_json_has_key"),
                BinaryOperator::QuestionAnd => Some("pg_json_has_all_keys"),
                BinaryOperator::QuestionPipe => Some("pg_json_has_any_key"),
                _ => None,
            };
            if let Some(name) = json_fn {
                return Ok(SqlExpr::Function {
                    name: name.into(),
                    args: vec![
                        convert_expr_depth(left, depth)?,
                        convert_expr_depth(right, depth)?,
                    ],
                    distinct: false,
                });
            }
            // `col @@ query` → pg_fts_match(col, query)
            if matches!(op, BinaryOperator::AtAt) {
                let col_expr = convert_expr_depth(left, depth)?;
                let query_expr = convert_expr_depth(right, depth)?;
                return Ok(crate::functions::fts_ops::pg_fts_funcs::lower_pg_fts_match(
                    col_expr, query_expr,
                ));
            }
            Ok(SqlExpr::BinaryOp {
                left: Box::new(convert_expr_depth(left, depth)?),
                op: convert_binary_op(op)?,
                right: Box::new(convert_expr_depth(right, depth)?),
            })
        }
        // A negative integer literal reaches sqlparser as unary minus applied
        // to a *positive* number, so the most negative `BIGINT` arrives as
        // `-(9223372036854775808)` — and that operand does not fit an `i64`.
        // Converting the operand on its own therefore falls back to `Float`
        // and silently turns an exact integer into an approximate one. Folding
        // the sign into the literal before parsing keeps the whole `i64` range
        // exact; anything that still does not fit falls through to the general
        // path below and is handled as before.
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } if matches!(
            inner.as_ref(),
            Expr::Value(v) if matches!(&v.value, Value::Number(..))
        ) =>
        {
            let Expr::Value(v) = inner.as_ref() else {
                unreachable!("guarded by the `matches!` above")
            };
            let Value::Number(n, _) = &v.value else {
                unreachable!("guarded by the `matches!` above")
            };
            match format!("-{n}").parse::<i64>() {
                Ok(i) => Ok(SqlExpr::Literal(SqlValue::Int(i))),
                Err(_) => Ok(SqlExpr::UnaryOp {
                    op: UnaryOp::Neg,
                    expr: Box::new(convert_expr_depth(inner, depth)?),
                }),
            }
        }
        Expr::UnaryOp { op, expr } => Ok(SqlExpr::UnaryOp {
            op: convert_unary_op(op)?,
            expr: Box::new(convert_expr_depth(expr, depth)?),
        }),
        Expr::Function(func) => convert_function_depth(func, depth),
        Expr::Nested(inner) => convert_expr_depth(inner, depth),
        Expr::IsNull(inner) => Ok(SqlExpr::IsNull {
            expr: Box::new(convert_expr_depth(inner, depth)?),
            negated: false,
        }),
        Expr::IsNotNull(inner) => Ok(SqlExpr::IsNull {
            expr: Box::new(convert_expr_depth(inner, depth)?),
            negated: true,
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(SqlExpr::InList {
            expr: Box::new(convert_expr_depth(expr, depth)?),
            list: list
                .iter()
                .map(|e| convert_expr_depth(e, depth))
                .collect::<Result<_>>()?,
            negated: *negated,
        }),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(SqlExpr::Between {
            expr: Box::new(convert_expr_depth(expr, depth)?),
            low: Box::new(convert_expr_depth(low, depth)?),
            high: Box::new(convert_expr_depth(high, depth)?),
            negated: *negated,
        }),
        Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => Ok(SqlExpr::Like {
            expr: Box::new(convert_expr_depth(expr, depth)?),
            pattern: Box::new(convert_expr_depth(pattern, depth)?),
            negated: *negated,
            case_insensitive: false,
        }),
        Expr::ILike {
            expr,
            pattern,
            negated,
            ..
        } => Ok(SqlExpr::Like {
            expr: Box::new(convert_expr_depth(expr, depth)?),
            pattern: Box::new(convert_expr_depth(pattern, depth)?),
            negated: *negated,
            case_insensitive: true,
        }),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let when_then = conditions
                .iter()
                .map(|cw| {
                    Ok((
                        convert_expr_depth(&cw.condition, depth)?,
                        convert_expr_depth(&cw.result, depth)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlExpr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| convert_expr_depth(e, depth).map(Box::new))
                    .transpose()?,
                when_then,
                else_expr: else_result
                    .as_ref()
                    .map(|e| convert_expr_depth(e, depth).map(Box::new))
                    .transpose()?,
            })
        }
        Expr::TypedString(ts) => {
            // TIMESTAMP '...' and TIMESTAMPTZ '...' typed string literals.
            let type_str = format!("{}", ts.data_type).to_ascii_uppercase();
            let raw = match &ts.value.value {
                Value::SingleQuotedString(s) => s.clone(),
                other => {
                    return Err(SqlError::Unsupported {
                        detail: format!("typed string value: {other}"),
                    });
                }
            };
            match type_str.as_str() {
                "TIMESTAMP" => {
                    let dt =
                        nodedb_types::NdbDateTime::parse(&raw).ok_or_else(|| SqlError::Parse {
                            detail: format!("cannot parse TIMESTAMP literal: '{raw}'"),
                        })?;
                    return Ok(SqlExpr::Literal(SqlValue::Timestamp(dt)));
                }
                "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
                    let dt =
                        nodedb_types::NdbDateTime::parse(&raw).ok_or_else(|| SqlError::Parse {
                            detail: format!("cannot parse TIMESTAMPTZ literal: '{raw}'"),
                        })?;
                    return Ok(SqlExpr::Literal(SqlValue::Timestamptz(dt)));
                }
                _ => {}
            }
            // Fall through: return as a generic literal string.
            Ok(SqlExpr::Literal(SqlValue::String(raw)))
        }
        Expr::Cast {
            expr, data_type, ..
        } => {
            // `::tsvector` and `::tsquery` casts are PG surface notation; the
            // inner expression is the actual text value.  Elide the cast and
            // return the inner expression directly — no runtime type change is
            // needed since we operate on plain strings internally.
            let type_str = format!("{data_type}").to_ascii_lowercase();
            if type_str == "tsvector" || type_str == "tsquery" {
                return convert_expr_depth(expr, depth);
            }
            // `'...'::TIMESTAMP` and `'...'::TIMESTAMPTZ` — promote string literals
            // to typed SqlValue when the inner expression is a string literal.
            let upper = type_str.to_uppercase();
            if (upper == "TIMESTAMP"
                || upper == "TIMESTAMPTZ"
                || upper == "TIMESTAMP WITH TIME ZONE")
                && let Expr::Value(v) = expr.as_ref()
                && let Value::SingleQuotedString(s) = &v.value
            {
                let dt = nodedb_types::NdbDateTime::parse(s).ok_or_else(|| SqlError::Parse {
                    detail: format!("cannot parse timestamp cast: '{s}'"),
                })?;
                return Ok(SqlExpr::Literal(if upper == "TIMESTAMP" {
                    SqlValue::Timestamp(dt)
                } else {
                    SqlValue::Timestamptz(dt)
                }));
            }
            Ok(SqlExpr::Cast {
                expr: Box::new(convert_expr_depth(expr, depth)?),
                to_type: format!("{data_type}"),
            })
        }
        Expr::Array(ast::Array { elem, .. }) => {
            let elems = elem
                .iter()
                .map(|e| convert_expr_depth(e, depth))
                .collect::<Result<_>>()?;
            Ok(SqlExpr::ArrayLiteral(elems))
        }
        Expr::Wildcard(_) => Ok(SqlExpr::Wildcard),
        // TRIM([BOTH|LEADING|TRAILING] [what FROM] expr)
        Expr::Trim { expr, .. } => Ok(SqlExpr::Function {
            name: "trim".into(),
            args: vec![convert_expr_depth(expr, depth)?],
            distinct: false,
        }),
        // CEIL(expr) / FLOOR(expr)
        Expr::Ceil { expr, .. } => Ok(SqlExpr::Function {
            name: "ceil".into(),
            args: vec![convert_expr_depth(expr, depth)?],
            distinct: false,
        }),
        Expr::Floor { expr, .. } => Ok(SqlExpr::Function {
            name: "floor".into(),
            args: vec![convert_expr_depth(expr, depth)?],
            distinct: false,
        }),
        // SUBSTRING(expr FROM start FOR len)
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![convert_expr_depth(expr, depth)?];
            if let Some(from) = substring_from {
                args.push(convert_expr_depth(from, depth)?);
            }
            if let Some(len) = substring_for {
                args.push(convert_expr_depth(len, depth)?);
            }
            Ok(SqlExpr::Function {
                name: "substring".into(),
                args,
                distinct: false,
            })
        }
        Expr::Interval(interval) => {
            // INTERVAL '1 hour' → microseconds as i64 literal.
            // The interval value is typically a string literal.
            let interval_str = match interval.value.as_ref() {
                Expr::Value(v) => match &v.value {
                    Value::SingleQuotedString(s) => s.clone(),
                    Value::Number(n, _) => {
                        // INTERVAL 5 HOUR → combine number with leading_field.
                        if let Some(ref field) = interval.leading_field {
                            format!("{n} {field}")
                        } else {
                            n.clone()
                        }
                    }
                    _ => {
                        return Err(SqlError::Unsupported {
                            detail: format!("INTERVAL value: {}", interval.value),
                        });
                    }
                },
                _ => {
                    return Err(SqlError::Unsupported {
                        detail: format!("INTERVAL expression: {}", interval.value),
                    });
                }
            };

            // If leading_field is specified, append it: INTERVAL '5' HOUR → "5 HOUR"
            let full_str = if interval_str.chars().all(|c| c.is_ascii_digit())
                && let Some(ref field) = interval.leading_field
            {
                format!("{interval_str} {field}")
            } else {
                interval_str
            };

            let micros = parse_interval_to_micros(&full_str).ok_or_else(|| SqlError::Parse {
                detail: format!("cannot parse INTERVAL '{full_str}'"),
            })?;

            Ok(SqlExpr::Literal(SqlValue::Int(micros)))
        }
        // `left = ANY(right)` — desugar into InList over array elements.
        // When `right` resolves to an ArrayLiteral (or a function call that
        // the bridge/evaluator will fold to an array), emit InList so the
        // downstream scan filter path handles it natively.
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => {
            // Only support `=` comparison for now; reject other operators
            // with a clear, non-AST-leaking message.
            use ast::BinaryOperator;
            if !matches!(compare_op, BinaryOperator::Eq) {
                return Err(SqlError::Unsupported {
                    detail: "ANY operator with non-equality comparison is not supported".into(),
                });
            }
            let left_expr = convert_expr_depth(left, depth)?;
            let right_expr = convert_expr_depth(right, depth)?;
            // Expand the right-hand side into a list if it is an array literal;
            // otherwise wrap as a single-element list so InList still evaluates.
            let list = match right_expr {
                SqlExpr::ArrayLiteral(elems) => elems,
                other => vec![other],
            };
            Ok(SqlExpr::InList {
                expr: Box::new(left_expr),
                list,
                negated: false,
            })
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("expression: {expr}"),
        }),
    }
}
