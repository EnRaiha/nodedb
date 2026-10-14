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
            offset,
        } => eval_subquery(ctx, inner, *range, *step, *offset),
    }
}

/// Step of a `[range:]` subquery: Prometheus' default evaluation interval.
const DEFAULT_SUBQUERY_STEP_MS: i64 = 60_000;

/// Evaluate a subquery: `expr[range:step] offset o`.
///
/// The inner expression must yield an instant vector. It is evaluated at
/// every timestamp that is a multiple of the step inside the left-open
/// window `(t - o - range, t - o]`, as Prometheus aligns subquery steps to
/// absolute time. The results form a range vector.
fn eval_subquery(
    ctx: &EvalContext,
    inner: &Expr,
    range: Duration,
    step: Option<Duration>,
    offset: Option<Duration>,
) -> Result<Value, PromqlError> {
    let end_ms = ctx.timestamp_ms - offset.map_or(0, |d| d.ms());
    let window_start = end_ms - range.ms();
    let step_ms = step.map_or(DEFAULT_SUBQUERY_STEP_MS, |d| d.ms()).max(1);
    let mut ts = window_start.div_euclid(step_ms) * step_ms;
    if ts <= window_start {
        ts += step_ms;
    }

    let mut result_series: BTreeMap<String, RangeSeries> = BTreeMap::new();
    while ts <= end_ms {
        let step_ctx = EvalContext {
            series: std::sync::Arc::clone(&ctx.series),
            timestamp_ms: ts,
            lookback_ms: ctx.lookback_ms,
        };
        let samples = match eval(&step_ctx, inner)? {
            Value::Vector(samples) => samples,
            other => {
                return Err(PromqlError::TypeError {
                    context: "subquery".to_string(),
                    detail: format!(
                        "subquery needs an instant vector expression, got {}",
                        other.result_type()
                    ),
                });
            }
        };
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
        ts += step_ms;
    }

    Ok(Value::Matrix(result_series.into_values().collect()))
}
