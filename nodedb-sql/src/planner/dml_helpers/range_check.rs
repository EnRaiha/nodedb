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
    check_declared_float_ranges(&info.columns, rows)
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

/// Reject a finite float value that overflows to infinity when narrowed to
/// its column's declared `REAL` width.
///
/// This is deliberately *not* the float mirror of
/// [`check_declared_int_ranges`]'s full-range check: narrowing an `f64` to
/// `f32` normally **rounds**, and `1.1` becoming `1.10000002` is correct
/// PostgreSQL `real` behaviour — accepted, never rejected. The one narrowing
/// that is not value-preserving is a finite value beyond `f32`'s range, which
/// would otherwise silently become `Infinity` on read; that is refused here at
/// write time, exactly as PostgreSQL refuses it (`value out of range: real`).
/// A value that is *already* infinite or `NaN` in the source is legitimately
/// representable in both widths and passes through unchanged — it is not an
/// overflow.
///
/// This runs in the planner, on the same coerce-then-check path as
/// [`check_declared_int_ranges`], for the same reason: the declared width is
/// engine-independent, and parameters are bound into the AST before planning,
/// so one check here covers literal `VALUES` and `$1` placeholders alike, for
/// every engine, on every DML path.
///
/// See `nodedb::control::server::pgwire::numeric_narrow::checked_narrow_f32`
/// for the read-side backstop this check is layered with: rows written before
/// a column's width was declared, or written via non-SQL ingest, are not
/// covered by this planner-time check, so the read-side guard stays in place
/// as the last line of defence. Neither is redundant with the other.
pub(crate) fn check_declared_float_ranges(
    columns: &[ColumnInfo],
    rows: &[Vec<(String, SqlValue)>],
) -> Result<()> {
    // Overwhelmingly the common case — skip the per-cell name lookup entirely
    // when the collection declares no `REAL`-width column.
    if !columns
        .iter()
        .any(|c| c.float_width == Some(nodedb_types::columnar::FloatWidth::F32))
    {
        return Ok(());
    }

    for row in rows {
        for (name, value) in row {
            let SqlValue::Float(v) = value else { continue };
            let Some(width) = columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
                .and_then(|c| c.float_width)
            else {
                continue;
            };
            if width != nodedb_types::columnar::FloatWidth::F32 {
                continue;
            }
            if v.is_finite() && !(*v as f32).is_finite() {
                return Err(SqlError::FloatOutOfRange {
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

/// [`check_declared_float_ranges`] for `UPDATE ... SET col = <literal>`.
///
/// Same literal-only scope and same computed-assignment carve-out as
/// [`check_declared_int_ranges_in_assignments`] — see its docs.
pub(crate) fn check_declared_float_ranges_in_assignments(
    columns: &[ColumnInfo],
    assignments: &[(String, SqlExpr)],
) -> Result<()> {
    let literals: Vec<(String, SqlValue)> = assignments
        .iter()
        .filter_map(|(col, expr)| match expr {
            SqlExpr::Literal(v @ SqlValue::Float(_)) => Some((col.clone(), v.clone())),
            _ => None,
        })
        .collect();
    if literals.is_empty() {
        return Ok(());
    }
    check_declared_float_ranges(columns, std::slice::from_ref(&literals))
}

#[cfg(test)]
mod float_range_tests {
    use super::*;
    use nodedb_types::columnar::FloatWidth;

    fn float_column(name: &str, width: Option<FloatWidth>) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: SqlDataType::Float64,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: width,
        }
    }

    fn row(name: &str, value: f64) -> Vec<Vec<(String, SqlValue)>> {
        vec![vec![(name.to_string(), SqlValue::Float(value))]]
    }

    #[test]
    fn out_of_f32_range_literal_into_real_column_is_rejected() {
        let columns = [float_column("r", Some(FloatWidth::F32))];
        let rows = row("r", 1e300);
        let err = check_declared_float_ranges(&columns, &rows).expect_err("must overflow f32");
        assert!(matches!(err, SqlError::FloatOutOfRange { .. }));
    }

    #[test]
    fn same_literal_into_double_column_is_accepted() {
        let columns = [float_column("r", Some(FloatWidth::F64))];
        let rows = row("r", 1e300);
        check_declared_float_ranges(&columns, &rows).expect("f64 has no narrower width to check");
    }

    #[test]
    fn merely_rounding_value_into_real_column_is_accepted() {
        // The load-bearing case: `1.1` narrows to `1.10000002` under `f32`,
        // which is correct rounding, never an error.
        let columns = [float_column("r", Some(FloatWidth::F32))];
        let rows = row("r", 1.1);
        check_declared_float_ranges(&columns, &rows).expect("rounding must not be rejected");
    }

    #[test]
    fn f32_max_itself_is_accepted() {
        let columns = [float_column("r", Some(FloatWidth::F32))];
        let rows = row("r", f32::MAX as f64);
        check_declared_float_ranges(&columns, &rows).expect("f32::MAX fits f32 exactly");
    }

    #[test]
    fn already_infinite_or_nan_literal_is_accepted() {
        let columns = [float_column("r", Some(FloatWidth::F32))];
        for v in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let rows = row("r", v);
            check_declared_float_ranges(&columns, &rows)
                .expect("already-infinite/NaN source values are not an overflow");
        }
    }

    #[test]
    fn no_declared_float_width_skips_check() {
        let columns = [float_column("r", None)];
        let rows = row("r", 1e300);
        check_declared_float_ranges(&columns, &rows).expect("untyped float column is unchecked");
    }
}
