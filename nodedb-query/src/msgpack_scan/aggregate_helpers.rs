// SPDX-License-Identifier: Apache-2.0

//! Public helpers for streaming aggregate accumulators.
//!
//! These thin wrappers expose field-extraction primitives used by the
//! `handlers/aggregate.rs` streaming accumulator path in the `nodedb` crate.
//! Each function operates on a single raw MessagePack document byte slice and
//! returns only the scalar value needed by the calling accumulator — no
//! document bytes are retained after the call returns.

use nodedb_types::Value;

use crate::expr::SqlExpr;
use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::reader::{read_f64, read_str, read_value};
use crate::value_ops;

// ── Expression evaluator ───────────────────────────────────────────────────

/// Evaluate `expr` against a decoded document.
///
/// These helpers feed `AggAccum::feed` (`nodedb/.../handlers/accum/feed.rs`),
/// the per-document hot path shared by every streaming aggregate
/// (`SUM`/`AVG`/`MIN`/`MAX`/`COUNT`/... over an expression argument), which
/// is itself infallible (`fn feed(&mut self, ..)`) and is called from
/// several independent aggregation pipelines (columnar, spill-to-disk
/// group-by, grouping sets, shuffle-merge). Threading a `Result` up through
/// `feed` and all of those call sites is a substantially larger, separate
/// change; for now a division/modulo-by-zero (nodedb issue #216) in an
/// aggregate argument folds to `None` here, matching the pre-existing
/// "value can't be produced for this row" handling every caller already
/// has (the row is simply excluded from the aggregate) rather than
/// silently coercing to a wrong answer. See the unit-2 completion report
/// for the follow-up needed to give aggregate arguments the same `22012`
/// treatment as WHERE/projection expressions.
#[inline]
fn eval_expr(doc: &[u8], expr: &SqlExpr) -> Option<Value> {
    let doc_val = nodedb_types::json_msgpack::value_from_msgpack(doc).ok()?;
    expr.eval(&doc_val).ok()
}

// ── Public extraction helpers ──────────────────────────────────────────────

/// Extract a numeric (f64) value from `field`, or evaluate `expr` if provided.
/// Returns `None` when the field is absent or cannot be converted to f64.
#[inline]
pub fn extract_f64(doc: &[u8], field: &str, expr: Option<&SqlExpr>) -> Option<f64> {
    if let Some(expr) = expr {
        return value_ops::value_to_f64(&eval_expr(doc, expr)?, false);
    }
    let (start, _end) = extract_field(doc, 0, field)?;
    read_f64(doc, start)
}

/// Extract a display string from `field`, or evaluate `expr` if provided.
/// Returns `None` when the field is absent.
pub fn extract_str(doc: &[u8], field: &str, expr: Option<&SqlExpr>) -> Option<String> {
    if let Some(expr) = expr {
        return Some(value_ops::value_to_display_string(&eval_expr(doc, expr)?));
    }
    let (start, _end) = extract_field(doc, 0, field)?;
    read_str(doc, start).map(|s| s.to_string())
}

/// Extract a field as `Value`.  Uses direct msgpack→Value for scalars;
/// falls back to full document decode only for complex types.
pub fn extract_value(doc: &[u8], field: &str, expr: Option<&SqlExpr>) -> Option<Value> {
    if let Some(expr) = expr {
        return eval_expr(doc, expr);
    }
    let (start, end) = extract_field(doc, 0, field)?;
    if let Some(v) = read_value(doc, start) {
        return Some(v);
    }
    let field_bytes = &doc[start..end];
    nodedb_types::json_msgpack::value_from_msgpack(field_bytes).ok()
}

/// Extract a field or expression result as raw msgpack bytes.
/// Used by `count_distinct`, `approx_count_distinct`, `approx_topk`, etc.
pub fn extract_bytes(doc: &[u8], field: &str, expr: Option<&SqlExpr>) -> Option<Vec<u8>> {
    if let Some(expr) = expr {
        let val = eval_expr(doc, expr)?;
        return nodedb_types::json_msgpack::value_to_msgpack(&val).ok();
    }
    let (start, end) = extract_field(doc, 0, field)?;
    Some(doc[start..end].to_vec())
}

/// Returns `Some(())` when the field is present and non-null.
/// Used by `count(field)` accumulator to count non-null values.
#[inline]
pub fn extract_non_null(doc: &[u8], field: &str, expr: Option<&SqlExpr>) -> Option<()> {
    let v = extract_value(doc, field, expr)?;
    if v.is_null() { None } else { Some(()) }
}
