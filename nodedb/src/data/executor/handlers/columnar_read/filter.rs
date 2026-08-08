// SPDX-License-Identifier: BUSL-1.1

//! Row-level WHERE predicate evaluation for memtable scans.

use nodedb_query::EvalError;
use nodedb_query::scan_filter::{FilterOp, ScanFilter};

/// Check whether a memtable row satisfies all filter predicates.
///
/// Returns `Ok(true)` if every filter passes (AND semantics). Uses the full
/// `ScanFilter::matches_value` path which handles `FilterOp::Expr` predicates
/// (scalar functions, JSON operators, column arithmetic) in addition to simple
/// comparison operators.
///
/// Returns `Err(EvalError::DivisionByZero)` when a `FilterOp::Expr`
/// predicate divides or takes a modulus by zero — this is the columnar
/// engine's WHERE predicate, so the behavior-flip rule applies: the query
/// fails instead of the row being silently excluded.
pub(in crate::data::executor) fn row_matches_filters(
    row: &[nodedb_types::value::Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    filters: &[ScanFilter],
) -> Result<bool, EvalError> {
    // Build a Value::Object so that ScanFilter::matches_value can navigate
    // field paths and expression predicates (e.g. pg_json_get_text).
    let mut map = std::collections::HashMap::with_capacity(schema.columns.len());
    for (i, col_def) in schema.columns.iter().enumerate() {
        if i < row.len() {
            map.insert(col_def.name.clone(), row[i].clone());
        }
    }
    value_matches_filters(&nodedb_types::Value::Object(map), filters)
}

/// Check whether an already-assembled row object satisfies all predicates.
///
/// The single evaluator both the columnar WHERE path above and the write gate
/// share, so a row-level-security predicate can never mean one thing when it
/// decides which rows a query returns and another when it decides which rows a
/// statement may write.
pub(in crate::data::executor) fn value_matches_filters(
    doc: &nodedb_types::Value,
    filters: &[ScanFilter],
) -> Result<bool, EvalError> {
    for filter in filters {
        if filter.op == FilterOp::MatchAll {
            continue;
        }
        if !filter.matches_value(doc)? {
            return Ok(false);
        }
    }
    Ok(true)
}
