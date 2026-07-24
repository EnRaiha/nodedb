// SPDX-License-Identifier: BUSL-1.1

//! Value-generating recursive CTE handler.
//!
//! Evaluates `WITH RECURSIVE name(cols) AS (anchor UNION [ALL] step WHERE cond)`
//! entirely in memory — no collection scan, no storage I/O.
//!
//! Algorithm:
//! 1. Parse and evaluate `init_exprs` against an empty context → row 0.
//! 2. Loop: evaluate `step_exprs` against the previous row → new row.
//!    Evaluate `condition` (if present) against the new row; stop if false.
//!    Stop on fixed point (UNION dedup) or when `max_depth` is exceeded
//!    (returns a typed error).
//! 3. Serialise all rows as a msgpack array and return.

use std::collections::{HashMap, HashSet};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_recursive_value`].
pub(in crate::data::executor) struct RecursiveValueParams<'a> {
    pub cte_name: &'a str,
    pub columns: &'a [String],
    pub init_exprs: &'a [String],
    pub step_exprs: &'a [String],
    pub condition: Option<&'a str>,
    pub max_depth: usize,
    pub distinct: bool,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_recursive_value(
        &mut self,
        task: &ExecutionTask,
        params: RecursiveValueParams<'_>,
    ) -> Response {
        let RecursiveValueParams {
            cte_name,
            columns,
            init_exprs,
            step_exprs,
            condition,
            max_depth,
            distinct,
        } = params;
        // ── Anchor row ────────────────────────────────────────────────────────
        let init_values = match eval_row_exprs(init_exprs, &HashMap::new()) {
            Some(v) => v,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Unsupported {
                        detail: format!(
                            "WITH RECURSIVE '{cte_name}': failed to evaluate anchor expressions; \
                             only literals and arithmetic are supported in value-generating CTEs"
                        ),
                    },
                );
            }
        };

        let mut results: Vec<nodedb_types::Value> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        let init_obj = build_object(columns, init_values);
        if should_keep_obj(&init_obj, distinct, &mut seen) {
            results.push(init_obj.clone());
        }

        // ── Iterative step ────────────────────────────────────────────────────
        let mut current = obj_to_ctx(&init_obj);

        for depth in 0..max_depth {
            // Evaluate the WHERE condition against the CURRENT row before
            // computing the next step — this matches SQL semantics where
            // `WHERE n < 5` filters which rows from the working set participate
            // in the recursive step.
            if let Some(cond_sql) = condition {
                match eval_condition(cond_sql, &current) {
                    Some(true) => {}
                    Some(false) | None => break,
                }
            }

            let step_values = match eval_row_exprs(step_exprs, &current) {
                Some(v) => v,
                None => break,
            };
            let new_obj = build_object(columns, step_values);

            if distinct {
                let key = obj_dedup_key(&new_obj);
                if !seen.insert(key) {
                    break; // Duplicate → fixed point.
                }
            }

            results.push(new_obj.clone());
            current = obj_to_ctx(&new_obj);

            // Depth limit: exceeded after recording depth+1 rows beyond anchor.
            if depth + 1 == max_depth {
                return self.response_error(
                    task,
                    ErrorCode::RecursionDepthExceeded {
                        cte_name: cte_name.to_owned(),
                        max_depth,
                    },
                );
            }
        }

        // ── Serialise to msgpack array ─────────────────────────────────────────
        // Use value_to_msgpack (standard msgpack, not the zerompk tagged format)
        // so the response passes through decode_payload_to_json correctly.
        let mut payload: Vec<u8> = Vec::new();
        nodedb_query::msgpack_scan::write_array_header(&mut payload, results.len());
        for obj in &results {
            match nodedb_types::value_to_msgpack(obj) {
                Ok(mp) => payload.extend_from_slice(&mp),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("recursive value serialisation failed: {e}"),
                        },
                    );
                }
            }
        }
        self.response_with_payload(task, payload)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a `Value::Object` from ordered column names and values.
fn build_object(columns: &[String], values: Vec<nodedb_types::Value>) -> nodedb_types::Value {
    let map: HashMap<String, nodedb_types::Value> = columns
        .iter()
        .zip(values)
        .map(|(k, v)| (k.clone(), v))
        .collect();
    nodedb_types::Value::Object(map)
}

/// Extract a column→value map from a `Value::Object` for use as an eval context.
fn obj_to_ctx(obj: &nodedb_types::Value) -> HashMap<String, nodedb_types::Value> {
    match obj {
        nodedb_types::Value::Object(m) => m.clone(),
        _ => HashMap::new(),
    }
}

/// Produce a stable deduplication key for a `Value::Object`.
fn obj_dedup_key(obj: &nodedb_types::Value) -> String {
    match obj {
        nodedb_types::Value::Object(m) => {
            let mut pairs: Vec<(&String, &nodedb_types::Value)> = m.iter().collect();
            pairs.sort_by_key(|(k, _)| k.as_str());
            pairs
                .iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect::<Vec<_>>()
                .join(",")
        }
        other => format!("{other:?}"),
    }
}

/// Returns `true` if the row should be added to results.
fn should_keep_obj(obj: &nodedb_types::Value, distinct: bool, seen: &mut HashSet<String>) -> bool {
    if !distinct {
        return true;
    }
    seen.insert(obj_dedup_key(obj))
}

/// Evaluate a slice of SQL expression strings against a row context.
/// Returns `None` if any expression fails to evaluate.
fn eval_row_exprs(
    exprs: &[String],
    ctx: &HashMap<String, nodedb_types::Value>,
) -> Option<Vec<nodedb_types::Value>> {
    exprs.iter().map(|e| eval_sql_expr(e, ctx)).collect()
}

/// Evaluate a single SQL expression string with an optional row context.
fn eval_sql_expr(
    sql_text: &str,
    ctx: &HashMap<String, nodedb_types::Value>,
) -> Option<nodedb_types::Value> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let full_sql = format!("SELECT {sql_text}");
    // reconstructed-sql: parser-only evaluates an internal recursive expression AST
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, &full_sql).ok()?;
    let stmt = stmts.into_iter().next()?;

    if let sqlparser::ast::Statement::Query(query) = stmt
        && let sqlparser::ast::SetExpr::Select(select) = &*query.body
        && let Some(item) = select.projection.first()
    {
        let expr = match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
            sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => return None,
        };
        return eval_ast_expr(expr, ctx);
    }
    None
}

/// Evaluate a WHERE condition expression; returns `None` on failure (treated as false).
fn eval_condition(sql_text: &str, ctx: &HashMap<String, nodedb_types::Value>) -> Option<bool> {
    match eval_sql_expr(sql_text, ctx)? {
        nodedb_types::Value::Bool(b) => Some(b),
        _ => None,
    }
}

/// Recursively evaluate a sqlparser AST expression with column reference support.
fn eval_ast_expr(
    expr: &sqlparser::ast::Expr,
    ctx: &HashMap<String, nodedb_types::Value>,
) -> Option<nodedb_types::Value> {
    use nodedb_types::Value;
    use sqlparser::ast::{Expr, UnaryOperator};

    match expr {
        Expr::Value(v) => eval_ast_literal(&v.value),

        // Unqualified column: `n`
        Expr::Identifier(ident) => {
            let name = ident.value.to_lowercase();
            ctx.get(&name).cloned()
        }

        // Qualified: `c.n` — strip qualifier
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let col = parts[1].value.to_lowercase();
            ctx.get(&col).cloned()
        }

        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match eval_ast_expr(inner, ctx)? {
            Value::Integer(i) => Some(Value::Integer(-i)),
            Value::Float(f) => Some(Value::Float(-f)),
            _ => None,
        },

        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => match eval_ast_expr(inner, ctx)? {
            Value::Bool(b) => Some(Value::Bool(!b)),
            _ => None,
        },

        Expr::BinaryOp { left, op, right } => {
            let l = eval_ast_expr(left, ctx)?;
            let r = eval_ast_expr(right, ctx)?;
            eval_binary_op(&l, op, &r)
        }

        Expr::Nested(inner) => eval_ast_expr(inner, ctx),

        _ => None,
    }
}

fn eval_ast_literal(v: &sqlparser::ast::Value) -> Option<nodedb_types::Value> {
    use sqlparser::ast::Value;
    match v {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Some(nodedb_types::Value::Integer(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Some(nodedb_types::Value::Float(f))
            } else {
                None
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Some(nodedb_types::Value::String(s.clone()))
        }
        Value::Boolean(b) => Some(nodedb_types::Value::Bool(*b)),
        Value::Null => Some(nodedb_types::Value::Null),
        _ => None,
    }
}

fn eval_binary_op(
    l: &nodedb_types::Value,
    op: &sqlparser::ast::BinaryOperator,
    r: &nodedb_types::Value,
) -> Option<nodedb_types::Value> {
    use nodedb_types::Value;
    use sqlparser::ast::BinaryOperator::*;

    match (l, r) {
        (Value::Integer(a), Value::Integer(b)) => match op {
            Plus => Some(Value::Integer(a.checked_add(*b)?)),
            Minus => Some(Value::Integer(a.checked_sub(*b)?)),
            Multiply => Some(Value::Integer(a.checked_mul(*b)?)),
            Divide if *b != 0 => Some(Value::Integer(a.checked_div(*b)?)),
            Modulo if *b != 0 => Some(Value::Integer(a.checked_rem(*b)?)),
            Gt => Some(Value::Bool(a > b)),
            GtEq => Some(Value::Bool(a >= b)),
            Lt => Some(Value::Bool(a < b)),
            LtEq => Some(Value::Bool(a <= b)),
            Eq => Some(Value::Bool(a == b)),
            NotEq => Some(Value::Bool(a != b)),
            _ => None,
        },
        (Value::Float(a), Value::Float(b)) => match op {
            Plus => Some(Value::Float(a + b)),
            Minus => Some(Value::Float(a - b)),
            Multiply => Some(Value::Float(a * b)),
            Divide if *b != 0.0 => {
                let res = a / b;
                if res.is_finite() {
                    Some(Value::Float(res))
                } else {
                    None
                }
            }
            Gt => Some(Value::Bool(a > b)),
            GtEq => Some(Value::Bool(a >= b)),
            Lt => Some(Value::Bool(a < b)),
            LtEq => Some(Value::Bool(a <= b)),
            _ => None,
        },
        (Value::Integer(a), Value::Float(b)) => {
            eval_binary_op(&Value::Float(*a as f64), op, &Value::Float(*b))
        }
        (Value::Float(a), Value::Integer(b)) => {
            eval_binary_op(&Value::Float(*a), op, &Value::Float(*b as f64))
        }
        (Value::Bool(a), Value::Bool(b)) => match op {
            And => Some(Value::Bool(*a && *b)),
            Or => Some(Value::Bool(*a || *b)),
            Eq => Some(Value::Bool(a == b)),
            NotEq => Some(Value::Bool(a != b)),
            _ => None,
        },
        (Value::String(a), Value::String(b)) => match op {
            StringConcat => Some(Value::String(format!("{a}{b}"))),
            Eq => Some(Value::Bool(a == b)),
            NotEq => Some(Value::Bool(a != b)),
            Gt => Some(Value::Bool(a > b)),
            Lt => Some(Value::Bool(a < b)),
            _ => None,
        },
        _ => None,
    }
}
