// SPDX-License-Identifier: BUSL-1.1

//! Expression evaluation for value-generating recursive CTEs.
//!
//! Every failure is a distinct, named outcome. The evaluator never returns an
//! "absent" value that a caller reads as a terminating condition. A recursive
//! step that fails to evaluate must abort the statement: a short result set is
//! indistinguishable from a correct one at the client.

use std::collections::HashMap;

use crate::bridge::envelope::ErrorCode;

/// Why an expression in a value-generating recursive CTE could not be reduced
/// to a value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvalError {
    /// A column reference that names nothing in the working row.
    #[error("column \"{column}\" does not exist")]
    UndefinedColumn { column: String },
    /// A well-formed expression the in-memory evaluator does not implement.
    #[error("{detail}")]
    Unsupported { detail: String },
    /// Integer arithmetic left the `i64` range, or a float result was not
    /// finite.
    #[error("{op}: numeric result is out of range")]
    Overflow { op: &'static str },
    /// Division or modulo by zero.
    #[error("division by zero")]
    DivisionByZero,
    /// A `WHERE` condition produced a value that is not a boolean.
    #[error("WHERE condition did not evaluate to a boolean")]
    NonBooleanCondition,
}

impl From<EvalError> for ErrorCode {
    fn from(err: EvalError) -> Self {
        match err {
            EvalError::UndefinedColumn { column } => ErrorCode::UndefinedColumn { column },
            EvalError::DivisionByZero => ErrorCode::DivisionByZero,
            other => ErrorCode::Unsupported {
                detail: other.to_string(),
            },
        }
    }
}

pub type EvalResult<T> = std::result::Result<T, EvalError>;

/// A row context: column name (lowercased) to value.
pub type Ctx = HashMap<String, nodedb_types::Value>;

/// Evaluate a slice of SQL expression strings against a row context.
pub fn eval_row_exprs(exprs: &[String], ctx: &Ctx) -> EvalResult<Vec<nodedb_types::Value>> {
    exprs.iter().map(|e| eval_sql_expr(e, ctx)).collect()
}

/// Evaluate a single SQL expression string against a row context.
pub fn eval_sql_expr(sql_text: &str, ctx: &Ctx) -> EvalResult<nodedb_types::Value> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let full_sql = format!("SELECT {sql_text}");
    // reconstructed-sql: parser-only evaluates an internal recursive expression AST
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, &full_sql).map_err(|e| {
        EvalError::Unsupported {
            detail: format!("could not parse '{sql_text}': {e}"),
        }
    })?;
    let unsupported = || EvalError::Unsupported {
        detail: format!("'{sql_text}' is not an expression this CTE can evaluate"),
    };
    let stmt = stmts.into_iter().next().ok_or_else(unsupported)?;
    let sqlparser::ast::Statement::Query(query) = stmt else {
        return Err(unsupported());
    };
    let sqlparser::ast::SetExpr::Select(select) = &*query.body else {
        return Err(unsupported());
    };
    let item = select.projection.first().ok_or_else(unsupported)?;
    let expr = match item {
        sqlparser::ast::SelectItem::UnnamedExpr(e)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. }
        | sqlparser::ast::SelectItem::ExprWithAliases { expr: e, .. } => e,
        sqlparser::ast::SelectItem::Wildcard(_)
        | sqlparser::ast::SelectItem::QualifiedWildcard(..) => return Err(unsupported()),
    };
    eval_ast_expr(expr, ctx)
}

/// Evaluate a `WHERE` condition.
///
/// `Ok(false)` is the only outcome that terminates a recursion; every failure
/// is an error, never a silent stop.
pub fn eval_condition(sql_text: &str, ctx: &Ctx) -> EvalResult<bool> {
    match eval_sql_expr(sql_text, ctx)? {
        nodedb_types::Value::Bool(b) => Ok(b),
        // SQL treats an unknown (NULL) predicate as not-true, which for a
        // recursive step means the row does not participate. That is a real
        // termination, not a failure to evaluate.
        nodedb_types::Value::Null => Ok(false),
        _ => Err(EvalError::NonBooleanCondition),
    }
}

/// Evaluate a sqlparser expression against a row context.
pub fn eval_ast_expr(expr: &sqlparser::ast::Expr, ctx: &Ctx) -> EvalResult<nodedb_types::Value> {
    use nodedb_types::Value;
    use sqlparser::ast::{Expr, UnaryOperator};

    match expr {
        Expr::Value(v) => eval_ast_literal(&v.value),

        // Unqualified column: `n`
        Expr::Identifier(ident) => {
            let name = ident.value.to_lowercase();
            ctx.get(&name)
                .cloned()
                .ok_or(EvalError::UndefinedColumn { column: name })
        }

        // Qualified: `c.n` — strip qualifier
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let col = parts[1].value.to_lowercase();
            ctx.get(&col)
                .cloned()
                .ok_or(EvalError::UndefinedColumn { column: col })
        }

        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match eval_ast_expr(inner, ctx)? {
            Value::Integer(i) => i
                .checked_neg()
                .map(Value::Integer)
                .ok_or(EvalError::Overflow { op: "negation" }),
            Value::Float(f) => Ok(Value::Float(-f)),
            other => Err(unsupported_operand("unary -", &other)),
        },

        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => match eval_ast_expr(inner, ctx)? {
            Value::Bool(b) => Ok(Value::Bool(!b)),
            other => Err(unsupported_operand("NOT", &other)),
        },

        Expr::BinaryOp { left, op, right } => {
            let l = eval_ast_expr(left, ctx)?;
            let r = eval_ast_expr(right, ctx)?;
            eval_binary_op(&l, op, &r)
        }

        Expr::Nested(inner) => eval_ast_expr(inner, ctx),

        other => Err(EvalError::Unsupported {
            detail: format!(
                "'{other}' is not supported in a value-generating WITH RECURSIVE; \
                 only literals, column references and arithmetic are"
            ),
        }),
    }
}

fn unsupported_operand(op: &str, value: &nodedb_types::Value) -> EvalError {
    EvalError::Unsupported {
        detail: format!("operator '{op}' does not apply to {}", type_name(value)),
    }
}

fn type_name(value: &nodedb_types::Value) -> &'static str {
    use nodedb_types::Value;
    match value {
        Value::Null => "NULL",
        Value::Bool(_) => "a boolean",
        Value::Integer(_) => "an integer",
        Value::Float(_) => "a float",
        Value::String(_) => "a string",
        _ => "this value",
    }
}

fn eval_ast_literal(v: &sqlparser::ast::Value) -> EvalResult<nodedb_types::Value> {
    use sqlparser::ast::Value;
    match v {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(nodedb_types::Value::Integer(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(nodedb_types::Value::Float(f))
            } else {
                Err(EvalError::Unsupported {
                    detail: format!("numeric literal '{n}' is out of range"),
                })
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(nodedb_types::Value::String(s.clone()))
        }
        Value::Boolean(b) => Ok(nodedb_types::Value::Bool(*b)),
        Value::Null => Ok(nodedb_types::Value::Null),
        other => Err(EvalError::Unsupported {
            detail: format!("literal '{other}' is not supported in a value-generating CTE"),
        }),
    }
}

fn eval_binary_op(
    l: &nodedb_types::Value,
    op: &sqlparser::ast::BinaryOperator,
    r: &nodedb_types::Value,
) -> EvalResult<nodedb_types::Value> {
    use nodedb_types::Value;
    use sqlparser::ast::BinaryOperator::*;

    let unsupported = || EvalError::Unsupported {
        detail: format!(
            "operator '{op}' does not apply to {} and {}",
            type_name(l),
            type_name(r)
        ),
    };

    match (l, r) {
        (Value::Integer(a), Value::Integer(b)) => match op {
            Plus => a
                .checked_add(*b)
                .map(Value::Integer)
                .ok_or(EvalError::Overflow { op: "addition" }),
            Minus => a
                .checked_sub(*b)
                .map(Value::Integer)
                .ok_or(EvalError::Overflow { op: "subtraction" }),
            Multiply => a
                .checked_mul(*b)
                .map(Value::Integer)
                .ok_or(EvalError::Overflow {
                    op: "multiplication",
                }),
            Divide if *b == 0 => Err(EvalError::DivisionByZero),
            Divide => a
                .checked_div(*b)
                .map(Value::Integer)
                .ok_or(EvalError::Overflow { op: "division" }),
            Modulo if *b == 0 => Err(EvalError::DivisionByZero),
            Modulo => a
                .checked_rem(*b)
                .map(Value::Integer)
                .ok_or(EvalError::Overflow { op: "modulo" }),
            Gt => Ok(Value::Bool(a > b)),
            GtEq => Ok(Value::Bool(a >= b)),
            Lt => Ok(Value::Bool(a < b)),
            LtEq => Ok(Value::Bool(a <= b)),
            Eq => Ok(Value::Bool(a == b)),
            NotEq => Ok(Value::Bool(a != b)),
            _ => Err(unsupported()),
        },
        (Value::Float(a), Value::Float(b)) => match op {
            Plus => finite(a + b, "addition"),
            Minus => finite(a - b, "subtraction"),
            Multiply => finite(a * b, "multiplication"),
            Divide if *b == 0.0 => Err(EvalError::DivisionByZero),
            Divide => finite(a / b, "division"),
            Gt => Ok(Value::Bool(a > b)),
            GtEq => Ok(Value::Bool(a >= b)),
            Lt => Ok(Value::Bool(a < b)),
            LtEq => Ok(Value::Bool(a <= b)),
            Eq => Ok(Value::Bool(a == b)),
            NotEq => Ok(Value::Bool(a != b)),
            _ => Err(unsupported()),
        },
        (Value::Integer(a), Value::Float(b)) => {
            eval_binary_op(&Value::Float(*a as f64), op, &Value::Float(*b))
        }
        (Value::Float(a), Value::Integer(b)) => {
            eval_binary_op(&Value::Float(*a), op, &Value::Float(*b as f64))
        }
        (Value::Bool(a), Value::Bool(b)) => match op {
            And => Ok(Value::Bool(*a && *b)),
            Or => Ok(Value::Bool(*a || *b)),
            Eq => Ok(Value::Bool(a == b)),
            NotEq => Ok(Value::Bool(a != b)),
            _ => Err(unsupported()),
        },
        (Value::String(a), Value::String(b)) => match op {
            StringConcat => Ok(Value::String(format!("{a}{b}"))),
            Eq => Ok(Value::Bool(a == b)),
            NotEq => Ok(Value::Bool(a != b)),
            Gt => Ok(Value::Bool(a > b)),
            GtEq => Ok(Value::Bool(a >= b)),
            Lt => Ok(Value::Bool(a < b)),
            LtEq => Ok(Value::Bool(a <= b)),
            _ => Err(unsupported()),
        },
        // A NULL operand makes the whole expression unknown, which is a value,
        // not a failure.
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(unsupported()),
    }
}

fn finite(result: f64, op: &'static str) -> EvalResult<nodedb_types::Value> {
    if result.is_finite() {
        Ok(nodedb_types::Value::Float(result))
    } else {
        Err(EvalError::Overflow { op })
    }
}
