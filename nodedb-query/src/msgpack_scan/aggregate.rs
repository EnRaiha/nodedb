// SPDX-License-Identifier: Apache-2.0

//! Zero-deserialization aggregate computation on raw MessagePack documents.
//!
//! Replaces `compute_aggregate(op, field, docs: &[serde_json::Value])` with
//! direct binary field extraction. Each document is `&[u8]` (MessagePack map).
//! When an expression is provided, decodes msgpack → `nodedb_types::Value`
//! directly (no JSON intermediate) and evaluates the expression once per document.

use std::cmp::Ordering;
use std::collections::HashSet;

use nodedb_types::Value;

use crate::msgpack_scan::compare::compare_field_bytes;
use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::reader::{read_f64, read_null, read_str};
use crate::value_ops;

/// Compute an aggregate function over raw MessagePack documents.
///
/// Each entry in `docs` is a complete MessagePack map (the raw bytes from storage).
/// Returns the result as `Value` — conversion to JSON happens at the
/// response boundary only.
pub fn compute_aggregate_binary(
    op: &str,
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
    docs: &[&[u8]],
) -> Value {
    match op {
        "count" => {
            if field == "*" && expr.is_none() {
                Value::Integer(docs.len() as i64)
            } else {
                let count = docs
                    .iter()
                    .filter_map(|d| extract_as_value(d, field, expr))
                    .filter(|v| !v.is_null())
                    .count();
                Value::Integer(count as i64)
            }
        }

        "sum" => {
            let total: f64 = docs
                .iter()
                .filter_map(|d| extract_f64_val(d, field, expr))
                .sum();
            Value::Float(total)
        }

        "avg" => {
            let (sum, count) = docs
                .iter()
                .filter_map(|d| extract_f64_val(d, field, expr))
                .fold((0.0f64, 0u64), |(s, c), v| (s + v, c + 1));
            if count == 0 {
                Value::Null
            } else {
                Value::Float(sum / count as f64)
            }
        }

        "min" => find_minmax(docs, field, expr, false),
        "max" => find_minmax(docs, field, expr, true),

        "count_distinct" => {
            let mut seen = HashSet::new();
            for doc in docs {
                if let Some(bytes) = extract_value_bytes(doc, field, expr)
                    && !value_bytes_are_null(&bytes)
                {
                    seen.insert(bytes);
                }
            }
            Value::Integer(seen.len() as i64)
        }

        "stddev" | "stddev_pop" => {
            stat_aggregate(docs, field, expr, |variance, _n| variance.sqrt(), true)
        }

        "stddev_samp" => stat_aggregate(docs, field, expr, |variance, _n| variance.sqrt(), false),

        "variance" | "var_pop" => stat_aggregate(docs, field, expr, |variance, _n| variance, true),

        "var_samp" => stat_aggregate(docs, field, expr, |variance, _n| variance, false),

        "array_agg" => {
            let values: Vec<Value> = docs
                .iter()
                .filter_map(|d| extract_as_value(d, field, expr))
                .filter(|v| !v.is_null())
                .collect();
            Value::Array(values)
        }

        "array_agg_distinct" => {
            let mut seen_bytes = HashSet::new();
            let mut values = Vec::new();
            for doc in docs {
                // When expr is present, evaluate once and derive both bytes and value
                // from the result to avoid double-decoding the document.
                if let Some(expr) = expr {
                    let Some(val) = eval_expr_on_doc(doc, expr) else {
                        continue;
                    };
                    if val.is_null() {
                        continue;
                    }
                    let bytes = zerompk::to_msgpack_vec(&val).unwrap_or_default();
                    if seen_bytes.insert(bytes) {
                        values.push(val);
                    }
                } else if let Some(bytes) = extract_value_bytes(doc, field, None)
                    && !value_bytes_are_null(&bytes)
                    && seen_bytes.insert(bytes)
                    && let Some(v) = value_from_field(doc, field)
                {
                    values.push(v);
                }
            }
            Value::Array(values)
        }

        "string_agg" | "group_concat" => {
            let values: Vec<String> = docs
                .iter()
                .filter_map(|d| extract_str_val(d, field, expr))
                .collect();
            Value::String(values.join(","))
        }

        "approx_count_distinct" => {
            let mut hll = nodedb_types::approx::HyperLogLog::new();
            for doc in docs {
                if let Some(bytes) = extract_value_bytes(doc, field, expr)
                    && !value_bytes_are_null(&bytes)
                {
                    // Hash the raw bytes for HLL.
                    let hash = hash_bytes(&bytes);
                    hll.add(hash);
                }
            }
            Value::Integer(hll.estimate().round() as i64)
        }

        "approx_percentile" => {
            // Format: field is "quantile:actual_field" (e.g. "0.95:latency").
            let (pct, actual_field) = if let Some(idx) = field.find(':') {
                match field[..idx].parse::<f64>() {
                    Ok(p) => (p, &field[idx + 1..]),
                    Err(_) => return Value::Null, // invalid quantile
                }
            } else {
                (0.5, field)
            };
            let mut digest = nodedb_types::approx::TDigest::new();
            for doc in docs {
                if let Some(v) = extract_f64_val(doc, actual_field, expr) {
                    digest.add(v);
                }
            }
            let result = digest.quantile(pct);
            if result.is_nan() {
                Value::Null
            } else {
                Value::Float(result)
            }
        }

        "approx_topk" => {
            // Format: field is "k:actual_field" (e.g. "10:region").
            let (k, actual_field) = if let Some(idx) = field.find(':') {
                match field[..idx].parse::<usize>() {
                    Ok(k) => (k, &field[idx + 1..]),
                    Err(_) => return Value::Null, // invalid k
                }
            } else {
                (10, field)
            };
            let mut ss = nodedb_types::approx::SpaceSaving::new(k);
            for doc in docs {
                if let Some(bytes) = extract_value_bytes(doc, actual_field, expr)
                    && !value_bytes_are_null(&bytes)
                {
                    ss.add(hash_bytes(&bytes));
                }
            }
            // Return as array of [hash, count, error] tuples.
            let top = ss.top_k();
            let arr: Vec<Value> = top
                .into_iter()
                .map(|(item, count, error)| {
                    Value::Object(
                        [
                            ("item".to_string(), Value::Integer(item as i64)),
                            ("count".to_string(), Value::Integer(count as i64)),
                            ("error".to_string(), Value::Integer(error as i64)),
                        ]
                        .into_iter()
                        .collect(),
                    )
                })
                .collect();
            Value::Array(arr)
        }

        "percentile_cont" => {
            let (pct, actual_field) = if let Some(idx) = field.find(':') {
                match field[..idx].parse::<f64>() {
                    Ok(p) => (p, &field[idx + 1..]),
                    Err(_) => return Value::Null, // invalid quantile
                }
            } else {
                (0.5, field)
            };
            let mut values: Vec<f64> = docs
                .iter()
                .filter_map(|d| extract_f64_val(d, actual_field, expr))
                .collect();
            if values.is_empty() {
                return Value::Null;
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let idx = (pct * (values.len() - 1) as f64).clamp(0.0, (values.len() - 1) as f64);
            let lower = idx.floor() as usize;
            let upper = idx.ceil() as usize;
            let frac = idx - lower as f64;
            let result = values[lower] * (1.0 - frac) + values[upper] * frac;
            Value::Float(result)
        }

        _ => Value::Null,
    }
}

// ── Internal helpers ───────────────────────────────────────────────────

/// Decode a msgpack document directly to `nodedb_types::Value` and evaluate
/// the expression. No JSON intermediate — msgpack → Value → eval → Value.
///
/// `compute_aggregate_binary` (this module's only public entry point) has
/// no caller anywhere in the workspace — the live aggregate pipeline goes
/// through `nodedb/src/data/executor/handlers/accum` instead. A
/// division/modulo-by-zero (nodedb issue #216) here folds to `None`
/// exactly like the pre-existing msgpack-decode-failure case on the line
/// above, rather than threading a `Result` through an otherwise-dead
/// `Option`-shaped API.
#[inline]
fn eval_expr_on_doc(doc: &[u8], expr: &crate::expr::SqlExpr) -> Option<Value> {
    let doc_val = nodedb_types::json_msgpack::value_from_msgpack(doc).ok()?;
    expr.eval(&doc_val).ok()
}

/// Extract a numeric value from a field or expression result.
#[inline]
fn extract_f64_val(doc: &[u8], field: &str, expr: Option<&crate::expr::SqlExpr>) -> Option<f64> {
    if let Some(expr) = expr {
        return value_ops::value_to_f64(&eval_expr_on_doc(doc, expr)?, false);
    }
    let (start, _end) = extract_field(doc, 0, field)?;
    read_f64(doc, start)
}

/// Extract a string from a field or expression result.
fn extract_str_val(doc: &[u8], field: &str, expr: Option<&crate::expr::SqlExpr>) -> Option<String> {
    if let Some(expr) = expr {
        return Some(value_ops::value_to_display_string(&eval_expr_on_doc(
            doc, expr,
        )?));
    }
    let (start, _end) = extract_field(doc, 0, field)?;
    read_str(doc, start).map(|s| s.to_string())
}

/// Extract a field as `Value`. Uses direct msgpack→Value for scalars;
/// falls back to full decode only for complex types.
fn extract_as_value(doc: &[u8], field: &str, expr: Option<&crate::expr::SqlExpr>) -> Option<Value> {
    if let Some(expr) = expr {
        return eval_expr_on_doc(doc, expr);
    }
    value_from_field(doc, field)
}

#[inline]
fn value_from_field(doc: &[u8], field: &str) -> Option<Value> {
    let (start, end) = extract_field(doc, 0, field)?;
    // Fast path: scalar types (null, bool, int, float, string).
    if let Some(v) = crate::msgpack_scan::reader::read_value(doc, start) {
        return Some(v);
    }
    // Slow path: complex types (array, map, bin) — decode field bytes directly.
    let field_bytes = &doc[start..end];
    nodedb_types::json_msgpack::value_from_msgpack(field_bytes).ok()
}

/// Find min or max across docs by comparing raw field bytes.
fn find_minmax(
    docs: &[&[u8]],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
    want_max: bool,
) -> Value {
    if let Some(expr) = expr {
        // Evaluate expression once per doc; compare on Value
        // since the result may be any type (not a raw field).
        let mut best: Option<Value> = None;
        for doc in docs {
            let Some(value) = eval_expr_on_doc(doc, expr) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            let replace = match &best {
                None => true,
                Some(current) => {
                    let ord = value_ops::compare_values(&value, current);
                    if want_max {
                        ord == Ordering::Greater
                    } else {
                        ord == Ordering::Less
                    }
                }
            };
            if replace {
                best = Some(value);
            }
        }
        return best.unwrap_or(Value::Null);
    }

    let mut best_doc: Option<&[u8]> = None;
    let mut best_range: Option<(usize, usize)> = None;

    for doc in docs {
        if let Some(range) = extract_field(doc, 0, field) {
            if read_null(doc, range.0) {
                continue;
            }
            match best_range {
                None => {
                    best_doc = Some(doc);
                    best_range = Some(range);
                }
                Some(br) => {
                    let Some(bd) = best_doc else { continue };
                    let cmp = compare_field_bytes(doc, range, bd, br);
                    let replace = if want_max {
                        cmp == Ordering::Greater
                    } else {
                        cmp == Ordering::Less
                    };
                    if replace {
                        best_doc = Some(doc);
                        best_range = Some(range);
                    }
                }
            }
        }
    }

    match (best_doc, best_range) {
        (Some(doc), Some((start, end))) => {
            if let Some(v) = crate::msgpack_scan::reader::read_value(doc, start) {
                return v;
            }
            let bytes = &doc[start..end];
            nodedb_types::json_msgpack::value_from_msgpack(bytes).unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

/// Compute stddev or variance. `population` = true for population variant.
/// `finalize` transforms the variance into the final result.
fn stat_aggregate(
    docs: &[&[u8]],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
    finalize: fn(f64, usize) -> f64,
    population: bool,
) -> Value {
    let values: Vec<f64> = docs
        .iter()
        .filter_map(|d| extract_f64_val(d, field, expr))
        .collect();
    if values.len() < 2 {
        return Value::Null;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let divisor = if population {
        values.len() as f64
    } else {
        (values.len() - 1) as f64
    };
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / divisor;
    Value::Float(finalize(variance, values.len()))
}

fn extract_value_bytes(
    doc: &[u8],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
) -> Option<Vec<u8>> {
    if let Some(expr) = expr {
        let val = eval_expr_on_doc(doc, expr)?;
        return nodedb_types::json_msgpack::value_to_msgpack(&val).ok();
    }
    let (start, end) = extract_field(doc, 0, field)?;
    Some(doc[start..end].to_vec())
}

/// Check if msgpack bytes represent null. Msgpack null is the single byte 0xc0.
fn value_bytes_are_null(bytes: &[u8]) -> bool {
    bytes == [0xc0]
}

/// FNV-1a hash for raw bytes (used by approx aggregates to feed HLL/SpaceSaving).
fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    #[test]
    fn count() {
        let d1 = encode(&json!({"x": 1}));
        let d2 = encode(&json!({"x": 2}));
        let d3 = encode(&json!({"x": 3}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("count", "x", None, &docs),
            Value::Integer(3)
        );
    }

    #[test]
    fn sum() {
        let d1 = encode(&json!({"v": 10}));
        let d2 = encode(&json!({"v": 20}));
        let d3 = encode(&json!({"v": 30}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("sum", "v", None, &docs),
            Value::Float(60.0)
        );
    }

    #[test]
    fn avg() {
        let d1 = encode(&json!({"v": 10}));
        let d2 = encode(&json!({"v": 20}));
        let docs: Vec<&[u8]> = vec![&d1, &d2];
        assert_eq!(
            compute_aggregate_binary("avg", "v", None, &docs),
            Value::Float(15.0)
        );
    }

    #[test]
    fn avg_empty() {
        let d1 = encode(&json!({"other": 1}));
        let docs: Vec<&[u8]> = vec![&d1];
        assert_eq!(
            compute_aggregate_binary("avg", "v", None, &docs),
            Value::Null
        );
    }

    #[test]
    fn min_max() {
        let d1 = encode(&json!({"v": 5}));
        let d2 = encode(&json!({"v": 1}));
        let d3 = encode(&json!({"v": 9}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];

        let min = compute_aggregate_binary("min", "v", None, &docs);
        let max = compute_aggregate_binary("max", "v", None, &docs);
        assert_eq!(min, Value::Integer(1));
        assert_eq!(max, Value::Integer(9));
    }

    #[test]
    fn count_distinct() {
        let d1 = encode(&json!({"v": "a"}));
        let d2 = encode(&json!({"v": "b"}));
        let d3 = encode(&json!({"v": "a"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("count_distinct", "v", None, &docs),
            Value::Integer(2)
        );
    }

    #[test]
    fn string_agg() {
        let d1 = encode(&json!({"n": "alice"}));
        let d2 = encode(&json!({"n": "bob"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2];
        assert_eq!(
            compute_aggregate_binary("string_agg", "n", None, &docs),
            Value::String("alice,bob".into())
        );
    }

    #[test]
    fn array_agg() {
        let d1 = encode(&json!({"v": 1}));
        let d2 = encode(&json!({"v": 2}));
        let docs: Vec<&[u8]> = vec![&d1, &d2];
        let result = compute_aggregate_binary("array_agg", "v", None, &docs);
        assert_eq!(
            result,
            Value::Array(vec![Value::Integer(1), Value::Integer(2),])
        );
    }

    #[test]
    fn stddev_pop() {
        let d1 = encode(&json!({"v": 2.0}));
        let d2 = encode(&json!({"v": 4.0}));
        let d3 = encode(&json!({"v": 4.0}));
        let d4 = encode(&json!({"v": 4.0}));
        let d5 = encode(&json!({"v": 5.0}));
        let d6 = encode(&json!({"v": 5.0}));
        let d7 = encode(&json!({"v": 7.0}));
        let d8 = encode(&json!({"v": 9.0}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3, &d4, &d5, &d6, &d7, &d8];
        let result = compute_aggregate_binary("stddev_pop", "v", None, &docs);
        if let Value::Float(v) = result {
            assert!((v - 2.0).abs() < 0.01);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn percentile_cont_median() {
        let d1 = encode(&json!({"v": 1.0}));
        let d2 = encode(&json!({"v": 2.0}));
        let d3 = encode(&json!({"v": 3.0}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("percentile_cont", "v", None, &docs),
            Value::Float(2.0)
        );
    }

    #[test]
    fn missing_field_skipped() {
        let d1 = encode(&json!({"v": 10}));
        let d2 = encode(&json!({"other": 99}));
        let d3 = encode(&json!({"v": 30}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("sum", "v", None, &docs),
            Value::Float(40.0)
        );
    }

    #[test]
    fn null_field_skipped_in_count_distinct() {
        let d1 = encode(&json!({"v": "a"}));
        let d2 = encode(&json!({"v": null}));
        let d3 = encode(&json!({"v": "a"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("count_distinct", "v", None, &docs),
            Value::Integer(1)
        );
    }

    #[test]
    fn array_agg_distinct() {
        let d1 = encode(&json!({"v": 1}));
        let d2 = encode(&json!({"v": 2}));
        let d3 = encode(&json!({"v": 1}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        let result = compute_aggregate_binary("array_agg_distinct", "v", None, &docs);
        assert_eq!(
            result,
            Value::Array(vec![Value::Integer(1), Value::Integer(2),])
        );
    }

    #[test]
    fn sum_case_when_expression() {
        let d1 = encode(&json!({"category": "tools"}));
        let d2 = encode(&json!({"category": "books"}));
        let d3 = encode(&json!({"category": "tools"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        let expr = crate::expr::SqlExpr::Case {
            operand: None,
            when_thens: vec![(
                crate::expr::SqlExpr::BinaryOp {
                    left: Box::new(crate::expr::SqlExpr::Column("category".into())),
                    op: crate::expr::BinaryOp::Eq,
                    right: Box::new(crate::expr::SqlExpr::Literal(Value::String("tools".into()))),
                },
                crate::expr::SqlExpr::Literal(Value::Integer(1)),
            )],
            else_expr: Some(Box::new(crate::expr::SqlExpr::Literal(Value::Integer(0)))),
        };

        assert_eq!(
            compute_aggregate_binary("sum", "*", Some(&expr), &docs),
            Value::Float(2.0)
        );
    }

    #[test]
    fn approx_count_distinct_basic() {
        let docs: Vec<Vec<u8>> = vec![
            encode(&json!({"region": "us"})),
            encode(&json!({"region": "eu"})),
            encode(&json!({"region": "us"})),
            encode(&json!({"region": "ap"})),
        ];
        let refs: Vec<&[u8]> = docs.iter().map(|d| d.as_slice()).collect();
        let result = compute_aggregate_binary("approx_count_distinct", "region", None, &refs);
        // HLL may not be exactly 3 but should be close.
        if let Value::Integer(n) = result {
            assert!((2..=4).contains(&n), "expected ~3 distinct, got {n}");
        } else {
            panic!("expected Integer, got {result:?}");
        }
    }

    #[test]
    fn approx_percentile_basic() {
        let docs: Vec<Vec<u8>> = (1..=100).map(|i| encode(&json!({"val": i}))).collect();
        let refs: Vec<&[u8]> = docs.iter().map(|d| d.as_slice()).collect();
        let result = compute_aggregate_binary("approx_percentile", "0.5:val", None, &refs);
        if let Value::Float(f) = result {
            assert!(
                (f - 50.0).abs() < 10.0,
                "p50 of 1..100 should be ~50, got {f}"
            );
        } else {
            panic!("expected Float, got {result:?}");
        }
    }

    #[test]
    fn approx_topk_basic() {
        let mut docs: Vec<Vec<u8>> = Vec::new();
        for _ in 0..10 {
            docs.push(encode(&json!({"cat": "a"})));
        }
        for _ in 0..5 {
            docs.push(encode(&json!({"cat": "b"})));
        }
        for _ in 0..1 {
            docs.push(encode(&json!({"cat": "c"})));
        }
        let refs: Vec<&[u8]> = docs.iter().map(|d| d.as_slice()).collect();
        let result = compute_aggregate_binary("approx_topk", "3:cat", None, &refs);
        if let Value::Array(arr) = result {
            assert!(!arr.is_empty(), "should have top-k results");
        } else {
            panic!("expected Array, got {result:?}");
        }
    }
}
