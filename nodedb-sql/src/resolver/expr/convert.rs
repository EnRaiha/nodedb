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

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Expr, SelectItem, Statement, Value};

    use super::convert_expr;
    use crate::error::SqlError;
    use crate::parser::statement::parse_sql;
    use crate::resolver::expr::value::convert_value;
    use crate::types::*;

    /// Extract the first SELECT item expression from a simple `SELECT <expr> FROM <tbl>`.
    fn first_select_expr(sql: &str) -> Expr {
        let stmts = parse_sql(sql).expect("parse failed");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        match &sel.projection[0] {
            SelectItem::UnnamedExpr(e) => e.clone(),
            SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
            other => panic!("unexpected projection item: {other:?}"),
        }
    }

    #[test]
    fn compound_identifier_two_parts_is_column() {
        let expr = first_select_expr("SELECT t.col FROM t");
        let result = convert_expr(&expr).expect("should succeed");
        match result {
            SqlExpr::Column {
                table: Some(t),
                name,
            } => {
                assert_eq!(t, "t");
                assert_eq!(name, "col");
            }
            other => panic!("expected Column with table, got {other:?}"),
        }
    }

    #[test]
    fn compound_identifier_three_parts_rejected() {
        // schema.table.col — should be rejected.
        use sqlparser::ast::Ident;
        let parts = vec![Ident::new("schema"), Ident::new("table"), Ident::new("col")];
        let expr = Expr::CompoundIdentifier(parts);
        let err = convert_expr(&expr).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("schema.table.col") || msg.contains("schema-qualified"),
            "error should mention the qualified name: {msg}"
        );
    }

    #[test]
    fn compound_identifier_four_parts_rejected() {
        use sqlparser::ast::Ident;
        let parts = vec![
            Ident::new("a"),
            Ident::new("b"),
            Ident::new("c"),
            Ident::new("d"),
        ];
        let expr = Expr::CompoundIdentifier(parts);
        let err = convert_expr(&expr).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    }

    /// `"userId"` with the PostgreSQL dialect is an identifier (quoted,
    /// case-preserved), not a string literal.
    #[test]
    fn double_quoted_is_identifier_not_literal() {
        let expr = first_select_expr(r#"SELECT "userId" FROM users"#);
        match expr {
            Expr::Identifier(ident) => {
                assert_eq!(ident.value, "userId");
                assert_eq!(ident.quote_style, Some('"'));
            }
            other => panic!("expected Identifier, got {other:?}"),
        }
    }

    /// `'userId'` is a single-quoted string literal.
    #[test]
    fn single_quoted_is_string_literal() {
        let expr = first_select_expr("SELECT 'userId' FROM users");
        match &expr {
            Expr::Value(v) => match &v.value {
                Value::SingleQuotedString(s) => assert_eq!(s, "userId"),
                other => panic!("expected SingleQuotedString, got {other:?}"),
            },
            other => panic!("expected Value, got {other:?}"),
        }
        // And convert_value maps it to SqlValue::String.
        let Expr::Value(v) = expr else { unreachable!() };
        assert!(matches!(
            convert_value(&v.value),
            Ok(SqlValue::String(s)) if s == "userId"
        ));
    }

    /// `Value::DoubleQuotedString` (non-Postgres dialect) falls through
    /// `convert_value` to `SqlError::Unsupported`. With PostgreSQL dialect
    /// this variant is never produced, but constructing it directly verifies
    /// the arm is absent and not silently accepted.
    #[test]
    fn double_quoted_string_value_unsupported() {
        // Construct the variant directly — it cannot be produced by parsing
        // with PostgreSqlDialect, which is exactly why the arm was dead code.
        let val = Value::DoubleQuotedString("userId".into());
        assert!(
            matches!(convert_value(&val), Err(SqlError::Unsupported { .. })),
            "DoubleQuotedString should be Unsupported, not silently accepted"
        );
    }

    /// `"col" = 'literal'` — double-quoted identifier on the left, single-quoted
    /// string literal on the right — must lower to `BinaryOp(Column("col"), Eq,
    /// Literal(String("literal")))`.  This is the canonical mixed-quotation form
    /// used in WHERE clauses (e.g. WHERE "userId" = 'alice').
    #[test]
    fn double_quoted_col_eq_single_quoted_literal() {
        let expr = where_sql_expr(r#"SELECT * FROM t WHERE "col" = 'literal'"#);
        match expr {
            SqlExpr::BinaryOp { left, right, .. } => {
                assert!(
                    matches!(*left, SqlExpr::Column { ref name, .. } if name == "col"),
                    "left should be Column(col), got {left:?}"
                );
                assert!(
                    matches!(*right, SqlExpr::Literal(SqlValue::String(ref s)) if s == "literal"),
                    "right should be Literal(String(\"literal\")), got {right:?}"
                );
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    /// `"colA" = "colB"` — both sides are double-quoted identifiers; both must
    /// resolve as column references, not string literals.
    #[test]
    fn double_quoted_col_eq_double_quoted_col() {
        let expr = where_sql_expr(r#"SELECT * FROM t WHERE "colA" = "colB""#);
        match expr {
            SqlExpr::BinaryOp { left, right, .. } => {
                assert!(
                    matches!(*left, SqlExpr::Column { ref name, .. } if name == "colA"),
                    "left should be Column(colA), got {left:?}"
                );
                assert!(
                    matches!(*right, SqlExpr::Column { ref name, .. } if name == "colB"),
                    "right should be Column(colB), got {right:?}"
                );
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    /// A double-quoted identifier in the SELECT list resolves as `SqlExpr::Column`
    /// with the exact case preserved (not lowercased, because it was quoted).
    #[test]
    fn double_quoted_select_col_case_preserved() {
        let expr = first_select_expr(r#"SELECT "userId" FROM users"#);
        let sql_expr = convert_expr(&expr).expect("convert_expr should succeed");
        match sql_expr {
            SqlExpr::Column { name, table } => {
                assert_eq!(
                    name, "userId",
                    "case must be preserved for quoted identifier"
                );
                assert_eq!(table, None, "no table qualifier expected");
            }
            other => panic!("expected Column, got {other:?}"),
        }
    }

    /// Extract and convert the WHERE predicate from a simple
    /// `SELECT * FROM tbl WHERE <expr>` statement.
    fn where_sql_expr(sql: &str) -> SqlExpr {
        let stmts = parse_sql(sql).expect("parse failed");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        let raw = sel.selection.as_ref().expect("expected WHERE clause");
        convert_expr(raw).expect("convert_expr failed")
    }

    #[test]
    fn like_is_case_sensitive() {
        let expr = where_sql_expr("SELECT * FROM t WHERE name LIKE 'foo%'");
        match expr {
            SqlExpr::Like {
                negated,
                case_insensitive,
                ..
            } => {
                assert!(!negated, "LIKE should not be negated");
                assert!(!case_insensitive, "LIKE should be case-sensitive");
            }
            other => panic!("expected SqlExpr::Like, got {other:?}"),
        }
    }

    #[test]
    fn ilike_is_case_insensitive() {
        let expr = where_sql_expr("SELECT * FROM t WHERE name ILIKE 'foo%'");
        match expr {
            SqlExpr::Like {
                negated,
                case_insensitive,
                ..
            } => {
                assert!(!negated, "ILIKE should not be negated");
                assert!(case_insensitive, "ILIKE should be case-insensitive");
            }
            other => panic!("expected SqlExpr::Like, got {other:?}"),
        }
    }

    #[test]
    fn not_like_is_negated_case_sensitive() {
        let expr = where_sql_expr("SELECT * FROM t WHERE name NOT LIKE 'foo%'");
        match expr {
            SqlExpr::Like {
                negated,
                case_insensitive,
                ..
            } => {
                assert!(negated, "NOT LIKE should be negated");
                assert!(!case_insensitive, "NOT LIKE should be case-sensitive");
            }
            other => panic!("expected SqlExpr::Like, got {other:?}"),
        }
    }

    #[test]
    fn not_ilike_is_negated_case_insensitive() {
        let expr = where_sql_expr("SELECT * FROM t WHERE name NOT ILIKE 'foo%'");
        match expr {
            SqlExpr::Like {
                negated,
                case_insensitive,
                ..
            } => {
                assert!(negated, "NOT ILIKE should be negated");
                assert!(case_insensitive, "NOT ILIKE should be case-insensitive");
            }
            other => panic!("expected SqlExpr::Like, got {other:?}"),
        }
    }

    // ── JSON operator lowering tests ───────────────────────────────────────

    /// Parses `SELECT <expr> FROM t` and returns the lowered `SqlExpr` for `<expr>`.
    fn select_expr_lowered(sql: &str) -> SqlExpr {
        let stmts = parse_sql(sql).expect("parse failed");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        let raw = &sel.projection[0];
        let raw_expr = match raw {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            other => panic!("unexpected projection: {other:?}"),
        };
        convert_expr(raw_expr).expect("convert_expr failed")
    }

    fn assert_json_fn(sql: &str, expected_fn: &str) {
        let expr = select_expr_lowered(sql);
        match expr {
            SqlExpr::Function { name, args, .. } => {
                assert_eq!(name, expected_fn, "wrong function name");
                assert_eq!(args.len(), 2, "expected 2 args");
            }
            other => panic!("expected Function, got {other:?}"),
        }
    }

    #[test]
    fn arrow_lowers_to_pg_json_get() {
        assert_json_fn("SELECT data->'key' FROM t", "pg_json_get");
    }

    #[test]
    fn long_arrow_lowers_to_pg_json_get_text() {
        assert_json_fn("SELECT data->>'key' FROM t", "pg_json_get_text");
    }

    #[test]
    fn hash_arrow_lowers_to_pg_json_path_get() {
        assert_json_fn("SELECT data#>'{a,b}' FROM t", "pg_json_path_get");
    }

    #[test]
    fn hash_long_arrow_lowers_to_pg_json_path_get_text() {
        assert_json_fn("SELECT data#>>'{a,b}' FROM t", "pg_json_path_get_text");
    }

    #[test]
    fn at_arrow_lowers_to_pg_json_contains() {
        assert_json_fn("SELECT data @> '{\"a\":1}' FROM t", "pg_json_contains");
    }

    #[test]
    fn arrow_at_lowers_to_pg_json_contained_by() {
        assert_json_fn("SELECT '{\"a\":1}' <@ data FROM t", "pg_json_contained_by");
    }

    #[test]
    fn question_lowers_to_pg_json_has_key() {
        assert_json_fn("SELECT data ? 'key' FROM t", "pg_json_has_key");
    }

    #[test]
    fn question_and_lowers_to_pg_json_has_all_keys() {
        assert_json_fn(
            "SELECT data ?& ARRAY['a','b'] FROM t",
            "pg_json_has_all_keys",
        );
    }

    #[test]
    fn question_pipe_lowers_to_pg_json_has_any_key() {
        assert_json_fn(
            "SELECT data ?| ARRAY['a','b'] FROM t",
            "pg_json_has_any_key",
        );
    }

    #[test]
    fn chained_arrow_lowers_nested() {
        // data->'a'->'b' → pg_json_get(pg_json_get(data, 'a'), 'b')
        let expr = select_expr_lowered("SELECT data->'a'->'b' FROM t");
        match expr {
            SqlExpr::Function { name, ref args, .. } => {
                assert_eq!(name, "pg_json_get", "outer fn should be pg_json_get");
                match &args[0] {
                    SqlExpr::Function {
                        name: inner_name, ..
                    } => {
                        assert_eq!(inner_name, "pg_json_get", "inner fn should be pg_json_get");
                    }
                    other => panic!("expected inner pg_json_get, got {other:?}"),
                }
            }
            other => panic!("expected outer pg_json_get, got {other:?}"),
        }
    }

    // ── FTS operator / function lowering tests ────────────────────────────────

    fn where_fn(sql: &str) -> SqlExpr {
        where_sql_expr(sql)
    }

    #[test]
    fn at_at_lowers_to_pg_fts_match() {
        // col @@ to_tsquery('rust & lang') → pg_fts_match(col, pg_to_tsquery('rust & lang'))
        let expr = where_fn("SELECT * FROM t WHERE body @@ to_tsquery('rust & lang')");
        match expr {
            SqlExpr::Function {
                ref name, ref args, ..
            } => {
                assert_eq!(
                    name, "pg_fts_match",
                    "operator @@ should lower to pg_fts_match"
                );
                assert_eq!(args.len(), 2, "expected 2 args");
                match &args[1] {
                    SqlExpr::Function { name: inner, .. } => {
                        assert_eq!(inner, "pg_to_tsquery");
                    }
                    other => panic!("expected pg_to_tsquery as right arg, got {other:?}"),
                }
            }
            other => panic!("expected pg_fts_match Function, got {other:?}"),
        }
    }

    #[test]
    fn at_at_with_plainto_tsquery() {
        // col @@ plainto_tsquery('rust lang') → pg_fts_match(col, pg_plainto_tsquery(...))
        let expr = where_fn("SELECT * FROM t WHERE body @@ plainto_tsquery('rust lang')");
        match expr {
            SqlExpr::Function {
                ref name, ref args, ..
            } => {
                assert_eq!(name, "pg_fts_match");
                match &args[1] {
                    SqlExpr::Function { name: inner, .. } => {
                        assert_eq!(inner, "pg_plainto_tsquery");
                    }
                    other => panic!("expected pg_plainto_tsquery, got {other:?}"),
                }
            }
            other => panic!("expected pg_fts_match, got {other:?}"),
        }
    }

    #[test]
    fn tsvector_cast_elided() {
        // 'foo'::tsvector → Literal("foo")
        let expr = select_expr_lowered("SELECT 'foo'::tsvector FROM t");
        assert!(
            matches!(expr, SqlExpr::Literal(SqlValue::String(ref s)) if s == "foo"),
            "expected Literal(\"foo\"), got {expr:?}"
        );
    }

    #[test]
    fn tsquery_cast_elided() {
        // 'rust'::tsquery → Literal("rust")
        let expr = select_expr_lowered("SELECT 'rust'::tsquery FROM t");
        assert!(
            matches!(expr, SqlExpr::Literal(SqlValue::String(ref s)) if s == "rust"),
            "expected Literal(\"rust\"), got {expr:?}"
        );
    }

    #[test]
    fn ts_rank_cd_is_unsupported() {
        use crate::parser::statement::parse_sql;
        let sql = "SELECT ts_rank_cd(body, to_tsquery('rust')) FROM t";
        let stmts = parse_sql(sql).expect("parse ok");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        let raw = match &sel.projection[0] {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            other => panic!("unexpected projection: {other:?}"),
        };
        let err = convert_expr(raw).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "ts_rank_cd should be Unsupported, got {err:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("ts_rank_cd"),
            "error should mention ts_rank_cd: {msg}"
        );
    }

    #[test]
    fn to_tsquery_lowers_to_pg_to_tsquery() {
        let expr = select_expr_lowered("SELECT to_tsquery('rust & lang') FROM t");
        match expr {
            SqlExpr::Function { ref name, .. } => {
                assert_eq!(name, "pg_to_tsquery");
            }
            other => panic!("expected pg_to_tsquery Function, got {other:?}"),
        }
    }

    #[test]
    fn plainto_tsquery_lowers_correctly() {
        let expr = select_expr_lowered("SELECT plainto_tsquery('rust lang') FROM t");
        match expr {
            SqlExpr::Function { ref name, .. } => {
                assert_eq!(name, "pg_plainto_tsquery");
            }
            other => panic!("expected pg_plainto_tsquery, got {other:?}"),
        }
    }

    #[test]
    fn ts_rank_lowers_to_pg_ts_rank() {
        let expr = select_expr_lowered("SELECT ts_rank(body, to_tsquery('rust')) FROM t");
        match expr {
            SqlExpr::Function { ref name, .. } => {
                assert_eq!(name, "pg_ts_rank");
            }
            other => panic!("expected pg_ts_rank, got {other:?}"),
        }
    }
}
