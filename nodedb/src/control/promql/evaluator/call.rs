// SPDX-License-Identifier: BUSL-1.1

//! Function call evaluation.

use super::super::ast::Expr;
use super::super::error::PromqlError;
use super::super::functions;
use super::super::types::*;
use super::{EvalContext, eval};

pub fn eval_call(ctx: &EvalContext, func: &str, args: &[Expr]) -> Result<Value, PromqlError> {
    // Range-vector functions.
    if functions::is_range_func(func) {
        if args.is_empty() {
            return Err(PromqlError::WrongArgCount {
                func: func.to_string(),
                expected: 1,
                got: 0,
            });
        }
        // `quantile_over_time(φ, v)` takes its scalar first; every other
        // range function takes the range vector first.
        let (matrix_idx, scalar_idx) = if func == "quantile_over_time" {
            (1, 0)
        } else {
            (0, 1)
        };
        if args.len() <= matrix_idx {
            return Err(PromqlError::WrongArgCount {
                func: func.to_string(),
                expected: matrix_idx + 1,
                got: args.len(),
            });
        }
        let matrix = eval(ctx, &args[matrix_idx])?;
        let scalar_arg = if args.len() > 1
            && let Value::Scalar(v, _) = eval(ctx, &args[scalar_idx])?
        {
            Some(v)
        } else {
            None
        };
        let scalar_arg2 = if args.len() > 2
            && let Value::Scalar(v, _) = eval(ctx, &args[2])?
        {
            Some(v)
        } else {
            None
        };

        let Value::Matrix(range_series) = matrix else {
            return Err(PromqlError::TypeError {
                context: func.to_string(),
                detail: "requires a range vector argument".to_string(),
            });
        };

        let mut result = Vec::new();
        for rs in &range_series {
            let val = if func == "holt_winters" {
                functions::call_holt_winters(&rs.samples, scalar_arg, scalar_arg2)
            } else {
                functions::call_range_func(func, &rs.samples, scalar_arg)
            };
            if let Some(v) = val {
                result.push(InstantSample {
                    labels: rs.labels.clone(),
                    value: v,
                    timestamp_ms: ctx.timestamp_ms,
                });
            }
        }
        return Ok(Value::Vector(result));
    }

    // Scalar math functions.
    match func {
        "abs" => unary_scalar_fn(ctx, args, f64::abs),
        "ceil" => unary_scalar_fn(ctx, args, f64::ceil),
        "floor" => unary_scalar_fn(ctx, args, f64::floor),
        "round" => unary_scalar_fn(ctx, args, f64::round),
        "sqrt" => unary_scalar_fn(ctx, args, f64::sqrt),
        "ln" => unary_scalar_fn(ctx, args, f64::ln),
        "log2" => unary_scalar_fn(ctx, args, f64::log2),
        "log10" => unary_scalar_fn(ctx, args, f64::log10),
        "exp" => unary_scalar_fn(ctx, args, f64::exp),
        "scalar" => {
            let val = eval(
                ctx,
                args.first().ok_or(PromqlError::WrongArgCount {
                    func: "scalar".to_string(),
                    expected: 1,
                    got: 0,
                })?,
            )?;
            match val {
                Value::Vector(v) if v.len() == 1 => Ok(Value::Scalar(v[0].value, ctx.timestamp_ms)),
                _ => Ok(Value::Scalar(f64::NAN, ctx.timestamp_ms)),
            }
        }
        "vector" => {
            let val = eval(
                ctx,
                args.first().ok_or(PromqlError::WrongArgCount {
                    func: "vector".to_string(),
                    expected: 1,
                    got: 0,
                })?,
            )?;
            match val {
                Value::Scalar(v, _) => Ok(Value::Vector(vec![InstantSample {
                    labels: Labels::new(),
                    value: v,
                    timestamp_ms: ctx.timestamp_ms,
                }])),
                other => Ok(other),
            }
        }
        "time" => Ok(Value::Scalar(
            ctx.timestamp_ms as f64 / 1000.0,
            ctx.timestamp_ms,
        )),

        // Trig functions.
        "acos" => unary_scalar_fn(ctx, args, f64::acos),
        "asin" => unary_scalar_fn(ctx, args, f64::asin),
        "atan" => unary_scalar_fn(ctx, args, f64::atan),
        "cos" => unary_scalar_fn(ctx, args, f64::cos),
        "sin" => unary_scalar_fn(ctx, args, f64::sin),
        "tan" => unary_scalar_fn(ctx, args, f64::tan),
        "deg" => unary_scalar_fn(ctx, args, f64::to_degrees),
        "rad" => unary_scalar_fn(ctx, args, f64::to_radians),
        "sgn" => unary_scalar_fn(ctx, args, f64::signum),

        // Clamp functions.
        "clamp" => eval_clamp(ctx, args),
        "clamp_min" => eval_clamp_min(ctx, args),
        "clamp_max" => eval_clamp_max(ctx, args),

        // absent: returns 1-element vector if input is empty, else empty vector.
        "absent" => eval_absent(ctx, args),

        // Label manipulation.
        "label_replace" => eval_label_replace(ctx, args),
        "label_join" => eval_label_join(ctx, args),

        // histogram_quantile(φ, buckets).
        "histogram_quantile" => eval_histogram_quantile(ctx, args),

        // atan2 is a binary function.
        "atan2" => {
            if args.len() < 2 {
                return Err(PromqlError::WrongArgCount {
                    func: "atan2".to_string(),
                    expected: 2,
                    got: args.len(),
                });
            }
            let a = eval(ctx, &args[0])?;
            let b = eval(ctx, &args[1])?;
            match (a, b) {
                (Value::Scalar(y, _), Value::Scalar(x, _)) => {
                    Ok(Value::Scalar(y.atan2(x), ctx.timestamp_ms))
                }
                _ => Err(PromqlError::TypeError {
                    context: "atan2".to_string(),
                    detail: "requires scalar arguments".to_string(),
                }),
            }
        }

        _ => Err(PromqlError::UnknownFunction {
            name: func.to_string(),
        }),
    }
}

fn eval_clamp(ctx: &EvalContext, args: &[Expr]) -> Result<Value, PromqlError> {
    if args.len() < 3 {
        return Err(PromqlError::WrongArgCount {
            func: "clamp".to_string(),
            expected: 3,
            got: args.len(),
        });
    }
    let val = eval(ctx, &args[0])?;
    let Value::Scalar(min_val, _) = eval(ctx, &args[1])? else {
        return Err(PromqlError::TypeError {
            context: "clamp".to_string(),
            detail: "min must be scalar".to_string(),
        });
    };
    let Value::Scalar(max_val, _) = eval(ctx, &args[2])? else {
        return Err(PromqlError::TypeError {
            context: "clamp".to_string(),
            detail: "max must be scalar".to_string(),
        });
    };
    apply_to_vector(val, ctx.timestamp_ms, |v| v.clamp(min_val, max_val))
}

fn eval_clamp_min(ctx: &EvalContext, args: &[Expr]) -> Result<Value, PromqlError> {
    if args.len() < 2 {
        return Err(PromqlError::WrongArgCount {
            func: "clamp_min".to_string(),
            expected: 2,
            got: args.len(),
        });
    }
    let val = eval(ctx, &args[0])?;
    let Value::Scalar(min_val, _) = eval(ctx, &args[1])? else {
        return Err(PromqlError::TypeError {
            context: "clamp_min".to_string(),
            detail: "min must be scalar".to_string(),
        });
    };
    apply_to_vector(val, ctx.timestamp_ms, |v| v.max(min_val))
}

fn eval_clamp_max(ctx: &EvalContext, args: &[Expr]) -> Result<Value, PromqlError> {
    if args.len() < 2 {
        return Err(PromqlError::WrongArgCount {
            func: "clamp_max".to_string(),
            expected: 2,
            got: args.len(),
        });
    }
    let val = eval(ctx, &args[0])?;
    let Value::Scalar(max_val, _) = eval(ctx, &args[1])? else {
        return Err(PromqlError::TypeError {
            context: "clamp_max".to_string(),
            detail: "max must be scalar".to_string(),
        });
    };
    apply_to_vector(val, ctx.timestamp_ms, |v| v.min(max_val))
}

fn eval_absent(ctx: &EvalContext, args: &[Expr]) -> Result<Value, PromqlError> {
    let val = eval(
        ctx,
        args.first().ok_or(PromqlError::WrongArgCount {
            func: "absent".to_string(),
            expected: 1,
            got: 0,
        })?,
    )?;
    match val {
        Value::Vector(v) if v.is_empty() => Ok(Value::Vector(vec![InstantSample {
            labels: Labels::new(),
            value: 1.0,
            timestamp_ms: ctx.timestamp_ms,
        }])),
        Value::Vector(_) => Ok(Value::Vector(vec![])),
        _ => Ok(Value::Vector(vec![])),
    }
}

fn eval_label_replace(ctx: &EvalContext, args: &[Expr]) -> Result<Value, PromqlError> {
    if args.len() < 5 {
        return Err(PromqlError::WrongArgCount {
            func: "label_replace".to_string(),
            expected: 5,
            got: args.len(),
        });
    }
    let val = eval(ctx, &args[0])?;
    let Value::Vector(samples) = val else {
        return Err(PromqlError::TypeError {
            context: "label_replace".to_string(),
            detail: "requires instant vector".to_string(),
        });
    };
    let dst = eval_string_arg(ctx, &args[1])?;
    let replacement = eval_string_arg(ctx, &args[2])?;
    let src = eval_string_arg(ctx, &args[3])?;
    let regex_str = eval_string_arg(ctx, &args[4])?;
    let re = regex::Regex::new(&format!("^(?:{regex_str})$")).map_err(|e| {
        PromqlError::InvalidString {
            detail: format!("label_replace: invalid regex: {e}"),
        }
    })?;

    let result: Vec<InstantSample> = samples
        .into_iter()
        .map(|mut s| {
            let src_val = s.labels.get(&src).cloned().unwrap_or_default();
            if let Some(caps) = re.captures(&src_val) {
                let mut replaced = replacement.clone();
                // Replace $1, $2, etc. with capture groups.
                for i in 0..caps.len() {
                    if let Some(m) = caps.get(i) {
                        replaced = replaced.replace(&format!("${i}"), m.as_str());
                    }
                }
                if replaced.is_empty() {
                    s.labels.remove(&dst);
                } else {
                    s.labels.insert(dst.clone(), replaced);
                }
            }
            s
        })
        .collect();
    Ok(Value::Vector(result))
}

fn eval_label_join(ctx: &EvalContext, args: &[Expr]) -> Result<Value, PromqlError> {
    if args.len() < 3 {
        return Err(PromqlError::WrongArgCount {
            func: "label_join".to_string(),
            expected: 3,
            got: args.len(),
        });
    }
    let val = eval(ctx, &args[0])?;
    let Value::Vector(samples) = val else {
        return Err(PromqlError::TypeError {
            context: "label_join".to_string(),
            detail: "requires instant vector".to_string(),
        });
    };
    let dst = eval_string_arg(ctx, &args[1])?;
    let separator = eval_string_arg(ctx, &args[2])?;
    let src_labels: Vec<String> = args[3..]
        .iter()
        .filter_map(|a| eval_string_arg(ctx, a).ok())
        .collect();

    let result: Vec<InstantSample> = samples
        .into_iter()
        .map(|mut s| {
            let joined: Vec<&str> = src_labels
                .iter()
                .filter_map(|l| s.labels.get(l).map(|v| v.as_str()))
                .collect();
            let val = joined.join(&separator);
            if val.is_empty() {
                s.labels.remove(&dst);
            } else {
                s.labels.insert(dst.clone(), val);
            }
            s
        })
        .collect();
    Ok(Value::Vector(result))
}

fn eval_histogram_quantile(ctx: &EvalContext, args: &[Expr]) -> Result<Value, PromqlError> {
    if args.len() < 2 {
        return Err(PromqlError::WrongArgCount {
            func: "histogram_quantile".to_string(),
            expected: 2,
            got: args.len(),
        });
    }
    let Value::Scalar(phi, _) = eval(ctx, &args[0])? else {
        return Err(PromqlError::TypeError {
            context: "histogram_quantile".to_string(),
            detail: "first arg must be scalar".to_string(),
        });
    };
    let val = eval(ctx, &args[1])?;
    let Value::Vector(samples) = val else {
        return Err(PromqlError::TypeError {
            context: "histogram_quantile".to_string(),
            detail: "second arg must be instant vector".to_string(),
        });
    };

    // Group by labels excluding "le".
    let mut groups: std::collections::BTreeMap<String, Vec<(f64, f64)>> =
        std::collections::BTreeMap::new();
    let mut group_labels_map: std::collections::BTreeMap<String, Labels> =
        std::collections::BTreeMap::new();

    for s in &samples {
        let le_str = s.labels.get("le").cloned().unwrap_or_default();
        let le: f64 = le_str.parse().unwrap_or(f64::INFINITY);
        let mut key_labels = s.labels.clone();
        key_labels.remove("le");
        key_labels.remove("__name__");
        let key = super::helpers::labels_key(&key_labels);
        group_labels_map.entry(key.clone()).or_insert(key_labels);
        groups.entry(key).or_default().push((le, s.value));
    }

    let mut result = Vec::new();
    for (key, mut buckets) in groups {
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let quantile_val = histogram_quantile_from_buckets(phi, &buckets);
        if let Some(labels) = group_labels_map.get(&key) {
            result.push(InstantSample {
                labels: labels.clone(),
                value: quantile_val,
                timestamp_ms: ctx.timestamp_ms,
            });
        }
    }
    Ok(Value::Vector(result))
}

/// Compute quantile from sorted histogram buckets (le, count) pairs.
fn histogram_quantile_from_buckets(phi: f64, buckets: &[(f64, f64)]) -> f64 {
    if buckets.is_empty() || phi.is_nan() {
        return f64::NAN;
    }
    let phi = phi.clamp(0.0, 1.0);
    let total = buckets.last().map_or(0.0, |b| b.1);
    if total == 0.0 {
        return f64::NAN;
    }
    let rank = phi * total;

    let mut prev_count = 0.0;
    let mut prev_le = 0.0;
    for &(le, count) in buckets {
        if count >= rank {
            // Linear interpolation within this bucket.
            let bucket_count = count - prev_count;
            if bucket_count <= 0.0 {
                return le;
            }
            let fraction = (rank - prev_count) / bucket_count;
            return prev_le + fraction * (le - prev_le);
        }
        prev_count = count;
        prev_le = le;
    }
    buckets.last().map_or(f64::NAN, |b| b.0)
}

fn eval_string_arg(ctx: &EvalContext, arg: &Expr) -> Result<String, PromqlError> {
    match arg {
        Expr::StringLiteral(s) => Ok(s.clone()),
        other => {
            let val = eval(ctx, other)?;
            match val {
                Value::Scalar(v, _) => Ok(format!("{v}")),
                _ => Err(PromqlError::TypeError {
                    context: "string argument".to_string(),
                    detail: "expected string or scalar".to_string(),
                }),
            }
        }
    }
}

fn apply_to_vector(val: Value, ts: i64, f: impl Fn(f64) -> f64) -> Result<Value, PromqlError> {
    match val {
        Value::Scalar(v, _) => Ok(Value::Scalar(f(v), ts)),
        Value::Vector(samples) => Ok(Value::Vector(
            samples
                .into_iter()
                .map(|s| InstantSample {
                    value: f(s.value),
                    ..s
                })
                .collect(),
        )),
        _ => Err(PromqlError::TypeError {
            context: "apply_to_vector".to_string(),
            detail: "expected scalar or instant vector".to_string(),
        }),
    }
}

fn unary_scalar_fn(
    ctx: &EvalContext,
    args: &[Expr],
    f: fn(f64) -> f64,
) -> Result<Value, super::super::PromqlError> {
    let val = eval(
        ctx,
        args.first()
            .ok_or(super::super::PromqlError::WrongArgCount {
                func: "unary_scalar_fn".to_string(),
                expected: 1,
                got: 0,
            })?,
    )?;
    match val {
        Value::Scalar(v, ts) => Ok(Value::Scalar(f(v), ts)),
        Value::Vector(samples) => {
            let mapped: Vec<InstantSample> = samples
                .into_iter()
                .map(|s| InstantSample {
                    labels: s.labels,
                    value: f(s.value),
                    timestamp_ms: s.timestamp_ms,
                })
                .collect();
            Ok(Value::Vector(mapped))
        }
        _ => Err(super::super::PromqlError::TypeError {
            context: "unary_scalar_fn".to_string(),
            detail: "argument must be scalar or instant vector".to_string(),
        }),
    }
}
