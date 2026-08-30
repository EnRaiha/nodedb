//! AST dispatch: evaluates one `Expr` node, recursing into its children.

use std::collections::BTreeMap;

use super::super::ast::*;
use super::super::error::PromqlError;
use super::super::types::*;
use super::context::EvalContext;
use super::{aggregate, binary, call, helpers, selector};

pub(crate) fn eval(ctx: &EvalContext, expr: &Expr) -> Result<Value, PromqlError> {
    match expr {
        Expr::Scalar(v) => Ok(Value::Scalar(*v, ctx.timestamp_ms)),
        Expr::StringLiteral(_) => Err(PromqlError::TypeError {
            context: "evaluation".to_string(),
            detail: "string literals not supported in evaluation".to_string(),
        }),
        Expr::VectorSelector {
            name,
            matchers,
            offset,
        } => selector::eval_vector_selector(ctx, name.as_deref(), matchers, *offset),
        Expr::MatrixSelector { selector, range } => {
            selector::eval_matrix_selector(ctx, selector, *range)
        }
        Expr::Paren(inner) => eval(ctx, inner),
        Expr::Negate(inner) => {
            let val = eval(ctx, inner)?;
            Ok(helpers::negate_value(val, ctx.timestamp_ms))
        }
        Expr::BinaryOp {
            op,
            lhs,
            rhs,
            return_bool,
            ..
        } => {
            let l = eval(ctx, lhs)?;
            let r = eval(ctx, rhs)?;
            binary::eval_binary_op(*op, l, r, *return_bool, ctx.timestamp_ms)
        }
        Expr::Aggregate {
            op,
            expr: inner,
            param,
            grouping,
        } => {
            let val = eval(ctx, inner)?;
            let p = match param {
                Some(p) => {
                    if let Value::Scalar(v, _) = eval(ctx, p)? {
                        Some(v)
                    } else {
                        None
                    }
                }
                None => None,
            };
            aggregate::eval_aggregation(*op, val, p, grouping, ctx.timestamp_ms)
        }
        Expr::Call { func, args } => call::eval_call(ctx, func, args),
        Expr::Subquery {
            expr: inner,
            range,
            step,
        } => eval_subquery(ctx, inner, *range, *step),
    }
}

/// Evaluate a subquery: `expr[range:step]`.
///
/// Evaluates the inner expression at each step within the range,
/// collecting results into a range vector (matrix).
fn eval_subquery(
    ctx: &EvalContext,
    inner: &Expr,
    range: Duration,
    step: Option<Duration>,
) -> Result<Value, PromqlError> {
    let end_ms = ctx.timestamp_ms;
    let start_ms = end_ms - range.ms();
    // Default step: evaluation interval, or 1 minute if unset.
    let step_ms = step.map_or(60_000, |d| d.ms()).max(1);

    let mut result_series: BTreeMap<String, RangeSeries> = BTreeMap::new();

    let mut ts = start_ms;
    while ts <= end_ms {
        let step_ctx = EvalContext {
            series: ctx.series.clone(),
            timestamp_ms: ts,
            lookback_ms: ctx.lookback_ms,
        };
        let val = eval(&step_ctx, inner)?;

        if let Value::Vector(samples) = val {
            for s in samples {
                let key = helpers::labels_key(&s.labels);
                let entry = result_series.entry(key).or_insert_with(|| RangeSeries {
                    labels: s.labels.clone(),
                    samples: vec![],
                });
                entry.samples.push(Sample {
                    timestamp_ms: ts,
                    value: s.value,
                });
            }
        }

        ts += step_ms;
    }

    Ok(Value::Matrix(result_series.into_values().collect()))
}
