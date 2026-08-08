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
    // Build the row image through the ONE builder every columnar path uses —
    // the write gate, the `ON CONFLICT` merge, and a `RETURNING` projection all
    // call it too. A private copy here is how the same schema-ordered row came
    // to mean two different objects on the read and write sides.
    let doc = crate::data::executor::handlers::columnar_write::row_values_to_object(schema, row);
    value_matches_filters(&doc, filters)
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
