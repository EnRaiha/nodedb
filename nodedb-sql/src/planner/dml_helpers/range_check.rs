// SPDX-License-Identifier: Apache-2.0

//! Declared-width validation for `VALUES` rows and `SET` assignments.
//!
//! Coercion (turning a literal into a typed value) and range-checking
//! (rejecting a typed value that overflows its column's declared width) are
//! deliberately two separate steps run in a fixed order — see
//! [`coerce_and_check_rows`].

use crate::error::{Result, SqlError};
use crate::planner::declared_type_coerce::coerce_rows_to_declared_types;
use crate::types::*;

/// Apply the collection's declared column types to a `VALUES` row set, then
/// range-check the result.
///
/// The single entry point every non-KV `VALUES` path uses, so no engine can
/// acquire one half of the contract without the other. The order is
/// load-bearing: coercion is what turns a literal into an integer in the first
/// place, so range-checking before it would let a value that only becomes an
/// `i64` through coercion skip its declared-width check entirely.
///
/// It is applied for every engine rather than only the ones that need it.
/// Engines with a typed write path (strict, columnar, timeseries, spatial)
/// already re-type each field against their declared schema on write and are
/// unaffected by a value arriving pre-typed; the document-schemaless and
/// key-value engines store the planner's value verbatim and are corrected by
/// it. Branching on engine here would put a routing decision outside
/// `EngineRules` for no behavioural gain — see `declared_type_coerce`.
pub(crate) fn coerce_and_check_rows(
    info: &CollectionInfo,
    rows: &mut [Vec<(String, SqlValue)>],
) -> Result<()> {
    coerce_rows_to_declared_types(&info.columns, rows, info.primary_key.as_deref())?;
    check_declared_int_ranges(&info.columns, rows)?;
    coerce_rows_to_declared_types(&info.columns, rows, info.primary_key.as_deref())?;
    check_declared_int_ranges(&info.columns, rows)
}

/// Reject any integer value that does not fit its column's declared width.
///
/// nodedb stores every integer as an `i64`, so this is not a storage limit.
/// It is the constraint that makes the column's advertised wire type honest:
/// a column declared `INTEGER` reports OID 23 in `RowDescription`, and a
/// pgwire client reading it in binary format decodes exactly four bytes.
/// Accepting a wider value would force a later choice between truncating it on
/// read and lying about the column's type — so the value is refused at the
/// point it enters, exactly as PostgreSQL refuses it.
///
/// This runs in the planner rather than in each engine because the declared
/// width is engine-independent (the same `IntWidth` drives the wire type for
/// schemaless, columnar, strict, and kv alike), and because parameters are
/// bound into the AST before planning — so one check here covers both literal
/// `VALUES` and `$1` placeholders, for every engine, on every DML path.
///
/// Non-integer values and columns with no declared width pass through: this
/// checks range only, never type.
pub(crate) fn check_declared_int_ranges(
    columns: &[ColumnInfo],
    rows: &[Vec<(String, SqlValue)>],
) -> Result<()> {
    // Overwhelmingly the common case — skip the per-cell name lookup entirely
    // when the collection declares no narrowed integer column.
    if !columns.iter().any(|c| {
        matches!(
            c.int_width,
            Some(nodedb_types::columnar::IntWidth::I16 | nodedb_types::columnar::IntWidth::I32)
        )
    }) {
        return Ok(());
    }

    for row in rows {
        for (name, value) in row {
            let SqlValue::Int(v) = value else { continue };
            let Some(width) = columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
                .and_then(|c| c.int_width)
            else {
                continue;
            };
            if !width.contains(*v) {
                return Err(SqlError::IntegerOutOfRange {
                    column: name.clone(),
                    value: *v,
                    declared_type: width.pg_type_name(),
                });
            }
        }
    }
    Ok(())
}

/// [`check_declared_int_ranges`] for `UPDATE ... SET col = <literal>`.
///
/// Only literal assignments are checkable at plan time; a computed assignment
/// (`SET n = n + 1`) has no value until the Data Plane evaluates it. Those are
/// caught on the read path instead, where the encoder refuses to transmit a
/// value that does not fit the column's advertised width — so an out-of-range
/// value can never reach a client silently by either route.
pub(crate) fn check_declared_int_ranges_in_assignments(
    columns: &[ColumnInfo],
    assignments: &[(String, SqlExpr)],
) -> Result<()> {
    let literals: Vec<(String, SqlValue)> = assignments
        .iter()
        .filter_map(|(col, expr)| match expr {
            SqlExpr::Literal(v @ SqlValue::Int(_)) => Some((col.clone(), v.clone())),
            _ => None,
        })
        .collect();
    if literals.is_empty() {
        return Ok(());
    }
    check_declared_int_ranges(columns, std::slice::from_ref(&literals))
}

