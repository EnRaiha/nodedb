// SPDX-License-Identifier: BUSL-1.1

//! The sample window each selector of a PromQL expression reads.
//!
//! The evaluator works on pre-fetched series. This walk tells the fetch
//! which metric, which matchers and which inclusive time range every
//! selector can touch while the expression is evaluated at each timestamp
//! of `[eval_start, eval_end]`.

use crate::control::promql::ast::Expr;
use crate::control::promql::{LabelMatcher, PromqlError};

/// One selector's read: its metric name, its matchers, and the inclusive
/// time window its evaluation can touch.
#[derive(Debug, Clone)]
pub(crate) struct SelectorRead {
    pub name: Option<String>,
    pub matchers: Vec<LabelMatcher>,
    pub min_ts: i64,
    pub max_ts: i64,
}

impl SelectorRead {
    /// Whether a series with `labels` belongs to this selector.
    pub(crate) fn selects(&self, labels: &crate::control::promql::types::Labels) -> bool {
        if let Some(name) = &self.name
            && labels.get("__name__").map(String::as_str) != Some(name.as_str())
        {
            return false;
        }
        crate::control::promql::label::matches_all(&self.matchers, labels)
    }

    /// Refuse a selector that matches every series, as Prometheus does. Such
    /// a selector would read every metric collection in the database.
    pub(crate) fn require_selective(&self) -> Result<(), PromqlError> {
        if self.name.is_some() || self.matchers.iter().any(|m| !m.matches("")) {
            Ok(())
        } else {
            Err(PromqlError::UnselectiveSelector)
        }
    }
}

/// Collect the read of every selector in `expr`, evaluated at each
/// timestamp in `[eval_start, eval_end]`.
pub(crate) fn selector_reads(
    expr: &Expr,
    eval_start: i64,
    eval_end: i64,
    lookback_ms: i64,
) -> Vec<SelectorRead> {
    let mut out = Vec::new();
    walk(expr, eval_start, eval_end, lookback_ms, &mut out);
    out
}

fn walk(expr: &Expr, start: i64, end: i64, lookback_ms: i64, out: &mut Vec<SelectorRead>) {
    match expr {
        Expr::Scalar(_) | Expr::StringLiteral(_) => {}
        Expr::VectorSelector {
            name,
            matchers,
            offset,
        } => {
            let off = offset.map_or(0, |d| d.ms());
            out.push(SelectorRead {
                name: name.clone(),
                matchers: matchers.clone(),
                min_ts: start.saturating_sub(off).saturating_sub(lookback_ms),
                max_ts: end.saturating_sub(off),
            });
        }
        Expr::MatrixSelector { selector, range } => {
            // A matrix over anything but a vector selector is refused by the
            // evaluator; it reads nothing here.
            if let Expr::VectorSelector {
                name,
                matchers,
                offset,
            } = selector.as_ref()
            {
                let off = offset.map_or(0, |d| d.ms());
                out.push(SelectorRead {
                    name: name.clone(),
                    matchers: matchers.clone(),
                    min_ts: start.saturating_sub(off).saturating_sub(range.ms()),
                    max_ts: end.saturating_sub(off),
                });
            }
        }
        Expr::Subquery {
            expr,
            range,
            offset,
            ..
        } => {
            // The inner expression runs at steps inside
            // `(t - offset - range, t - offset]`.
            let off = offset.map_or(0, |d| d.ms());
            walk(
                expr,
                start.saturating_sub(off).saturating_sub(range.ms()),
                end.saturating_sub(off),
                lookback_ms,
                out,
            )
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            walk(lhs, start, end, lookback_ms, out);
            walk(rhs, start, end, lookback_ms, out);
        }
        Expr::Negate(inner) | Expr::Paren(inner) => walk(inner, start, end, lookback_ms, out),
        Expr::Aggregate { expr, param, .. } => {
            walk(expr, start, end, lookback_ms, out);
            if let Some(param) = param {
                walk(param, start, end, lookback_ms, out);
            }
        }
        Expr::Call { args, .. } => {
            for arg in args {
                walk(arg, start, end, lookback_ms, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::promql::lexer::tokenize;
    use crate::control::promql::parse;

    const MIN: i64 = 60_000;

    fn reads(q: &str, start: i64, end: i64) -> Vec<SelectorRead> {
        let expr = parse(&tokenize(q).unwrap()).unwrap();
        selector_reads(&expr, start, end, 5 * MIN)
    }

    #[test]
    fn vector_selector_reads_the_lookback_window() {
        let r = reads("up", 100 * MIN, 110 * MIN);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].min_ts, 95 * MIN);
        assert_eq!(r[0].max_ts, 110 * MIN);
    }

    #[test]
    fn matrix_selector_with_offset_shifts_the_range() {
        let r = reads("rate(x[5m] offset 1m)", 100 * MIN, 110 * MIN);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].min_ts, 94 * MIN);
        assert_eq!(r[0].max_ts, 109 * MIN);
    }

    #[test]
    fn subquery_widens_the_inner_window() {
        let r = reads("max_over_time(x[10m:1m])", 100 * MIN, 110 * MIN);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].min_ts, 85 * MIN);
        assert_eq!(r[0].max_ts, 110 * MIN);
    }

    #[test]
    fn subquery_offset_shifts_the_inner_window() {
        let r = reads("max_over_time(x[10m:1m] offset 2m)", 100 * MIN, 110 * MIN);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].min_ts, 83 * MIN);
        assert_eq!(r[0].max_ts, 108 * MIN);
    }

    #[test]
    fn binary_op_reads_both_sides() {
        let r = reads("a + b", 0, 0);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn selector_matching_only_empty_values_is_refused() {
        let r = reads(r#"{job=~".*"}"#, 0, 0);
        assert!(matches!(
            r[0].require_selective(),
            Err(PromqlError::UnselectiveSelector)
        ));
        let r = reads(r#"{job="api"}"#, 0, 0);
        assert!(r[0].require_selective().is_ok());
        let r = reads("up", 0, 0);
        assert!(r[0].require_selective().is_ok());
    }
}
