// SPDX-License-Identifier: BUSL-1.1

//! Vector and matrix selector evaluation.

use super::super::ast::*;
use super::super::error::PromqlError;
use super::super::label::matches_all;
use super::super::types::*;
use super::EvalContext;

pub fn eval_vector_selector(
    ctx: &EvalContext,
    name: Option<&str>,
    matchers: &[super::super::label::LabelMatcher],
    offset: Option<Duration>,
) -> Result<Value, PromqlError> {
    let eval_ts = ctx.timestamp_ms - offset.map_or(0, |d| d.ms());
    let min_ts = eval_ts - ctx.lookback_ms;

    let mut result = Vec::new();
    for series in ctx.series.iter() {
        if let Some(n) = name
            && series.metric_name() != n
        {
            continue;
        }
        if !matches_all(matchers, &series.labels) {
            continue;
        }

        // Samples are ascending by timestamp: the latest one at or before
        // `eval_ts` is the last one of the prefix that ends there.
        let end = series
            .samples
            .partition_point(|s| s.timestamp_ms <= eval_ts);
        let sample = series.samples[..end]
            .last()
            .filter(|s| s.timestamp_ms >= min_ts);

        if let Some(s) = sample {
            result.push(InstantSample {
                labels: series.labels.clone(),
                value: s.value,
                timestamp_ms: eval_ts,
            });
        }
    }
    Ok(Value::Vector(result))
}

pub fn eval_matrix_selector(
    ctx: &EvalContext,
    selector: &Expr,
    range: Duration,
) -> Result<Value, PromqlError> {
    let Expr::VectorSelector {
        name,
        matchers,
        offset,
    } = selector
    else {
        return Err(PromqlError::Selector {
            detail: "matrix selector requires vector selector".to_string(),
        });
    };

    let eval_ts = ctx.timestamp_ms - offset.map_or(0, |d| d.ms());
    let range_start = eval_ts - range.ms();

    let mut result = Vec::new();
    for series in ctx.series.iter() {
        if let Some(n) = name
            && series.metric_name() != n
        {
            continue;
        }
        if !matches_all(matchers, &series.labels) {
            continue;
        }

        // Samples are ascending by timestamp: the window is the slice in
        // `(range_start, eval_ts]`.
        let lo = series
            .samples
            .partition_point(|s| s.timestamp_ms <= range_start);
        let hi = series
            .samples
            .partition_point(|s| s.timestamp_ms <= eval_ts);
        let window: Vec<Sample> = series.samples[lo..hi].to_vec();

        if !window.is_empty() {
            result.push(RangeSeries {
                labels: series.labels.clone(),
                samples: window,
            });
        }
    }
    Ok(Value::Matrix(result))
}
