// SPDX-License-Identifier: Apache-2.0

//! Window-function argument evaluation.
//!
//! A window function's argument is an arbitrary expression — `SUM(price * qty)`,
//! `LAG(10 / denom)`, `SUM(1)` — not merely a column reference. Every argument
//! is evaluated per row through the shared row evaluator, so a
//! division/modulo-by-zero inside an argument raises
//! `EvalError::DivisionByZero` (SQLSTATE `22012`) exactly as it does in a
//! projection or WHERE clause, and a non-column argument yields its computed
//! value rather than a NULL placeholder.

use super::helpers::eval_expr_on_json;
use super::spec::WindowFuncSpec;
use crate::expr::{EvalError, SqlExpr};

/// Evaluated values of one window-function argument, aligned to the caller's
/// partition `indices` (position `p` holds the value for row `indices[p]`).
///
/// `None` means the function was called without that argument — `COUNT(*)`,
/// `ROW_NUMBER()` — which is distinct from an argument that evaluated to NULL.
pub(super) type ArgValues = Option<Vec<serde_json::Value>>;

/// Evaluate argument `idx` of `spec` once for every row in the partition.
///
/// Evaluating up front keeps each row's argument evaluated exactly once even
/// when the frame evaluator revisits rows, and gives every aggregate the same
/// values the frame bounds are computed against.
pub(super) fn eval_arg_values(
    rows: &[(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    idx: usize,
) -> Result<ArgValues, EvalError> {
    let Some(expr) = spec.args.get(idx) else {
        return Ok(None);
    };
    let values = indices
        .iter()
        .map(|&i| eval_expr_on_json(expr, &rows[i].1))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(values))
}

/// Value of argument `idx` at partition position `pos`, or NULL when the
/// function was called without that argument.
pub(super) fn arg_at(values: &ArgValues, pos: usize) -> serde_json::Value {
    values
        .as_ref()
        .and_then(|v| v.get(pos).cloned())
        .unwrap_or(serde_json::Value::Null)
}

/// Resolve a constant integer argument — the `LAG`/`LEAD` offset, `NTILE`
/// bucket count, and `NTH_VALUE` position.
///
/// The planner rejects a non-constant in these positions before the spec ever
/// reaches the evaluator, so the only way to observe `default` here is an
/// argument the caller omitted (`LAG(x)` → offset 1).
pub(super) fn const_usize_arg(spec: &WindowFuncSpec, idx: usize, default: usize) -> usize {
    spec.args
        .get(idx)
        .and_then(|e| match e {
            SqlExpr::Literal(v) => v.as_f64().map(|n| n as usize),
            _ => None,
        })
        .unwrap_or(default)
}

/// Resolve the constant `default` argument of `LAG`/`LEAD` — the value
/// returned when the offset falls outside the partition.
pub(super) fn const_default_arg(spec: &WindowFuncSpec, idx: usize) -> serde_json::Value {
    spec.args
        .get(idx)
        .and_then(|e| match e {
            SqlExpr::Literal(v) => Some(serde_json::Value::from(v.clone())),
            _ => None,
        })
        .unwrap_or(serde_json::Value::Null)
}
