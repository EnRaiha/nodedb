// SPDX-License-Identifier: Apache-2.0

//! Expression evaluation against a JSON row.
//!
//! Scan handlers materialize rows as `serde_json::Value` before applying
//! ORDER BY and window functions. Both evaluate arbitrary expressions per row
//! and both must surface an evaluation failure — a zero divisor in
//! `ORDER BY 100 / weight` fails the statement with SQLSTATE `22012` rather
//! than sorting the row under NULL.

use crate::expr::{EvalError, SqlExpr};

/// Evaluate `expr` against a JSON row.
///
/// Column and literal references short-circuit; anything else goes through the
/// shared row evaluator, so arithmetic, function calls, and `CASE` all behave
/// exactly as they do in a projection or WHERE clause.
pub fn eval_expr_on_json(
    expr: &SqlExpr,
    doc: &serde_json::Value,
) -> Result<serde_json::Value, EvalError> {
    match expr {
        SqlExpr::Column(name) => Ok(doc.get(name).cloned().unwrap_or(serde_json::Value::Null)),
        SqlExpr::Literal(v) => Ok(serde_json::Value::from(v.clone())),
        other => {
            let row = nodedb_types::Value::from(doc.clone());
            Ok(serde_json::Value::from(other.eval(&row)?))
        }
    }
}

/// Total order over JSON values for sorting.
///
/// NULLs sort first; numbers compare numerically; strings lexicographically;
/// mixed or composite values fall back to their rendered form so the ordering
/// stays deterministic.
pub fn compare_json(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value;
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => x
            .as_f64()
            .partial_cmp(&y.as_f64())
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => a.to_string().cmp(&b.to_string()),
    }
}
