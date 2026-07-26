// SPDX-License-Identifier: BUSL-1.1

//! Guards for narrowing a stored numeric cell to the width its column
//! advertises on the wire.
//!
//! nodedb stores every integer as a full `i64` and every float as a full
//! `f64`; the declared width (`SMALLINT`, `REAL`, …) lives only in the
//! `RowDescription` OID and, under the binary result format, in the number of
//! bytes transmitted. Narrowing therefore happens at encode time, in every
//! pgwire encoder — and a narrowing that silently changes the value is
//! indistinguishable from a correct one at the far end. These helpers are the
//! single place that decides which narrowings are value-preserving, so no
//! encoder can disagree with another.
//!
//! The two families fail differently, and the difference is deliberate:
//!
//! * **Integers wrap.** A value outside the declared width has no faithful
//!   encoding at all, so it is a hard error — and, because that is unusable as
//!   a last line of defence, writes are range-checked up front
//!   (`nodedb_sql::planner::dml_helpers::check_declared_int_ranges`).
//! * **Floats round.** `1.1` read back from a `REAL` column as `1.10000002` is
//!   exactly what PostgreSQL does, so rounding is never an error here. The
//!   single non-value-preserving case is overflow: a finite `f64` beyond
//!   `f32`'s range narrows to an infinity, replacing a real number with
//!   `Infinity` and signalling nothing — writes are range-checked for exactly
//!   this case up front
//!   (`nodedb_sql::planner::dml_helpers::check_declared_float_ranges`).

use nodedb_types::columnar::{FloatWidth, IntWidth};
use pgwire::error::PgWireResult;

/// Range-check an integer cell against the width its column advertises, before
/// it is narrowed for transmission.
///
/// `Ok(None)` means the cell is absent or not an integer and encodes as SQL
/// NULL, matching the wider `Int8` arm. `Ok(Some(n))` guarantees `n` fits
/// `width`, so the caller's narrowing cast is lossless by construction.
///
/// An out-of-range value is a hard error rather than a truncation: the
/// column's `RowDescription` already told the client to read two or four
/// bytes, so no encoding of the true value is available here, and a wrapped
/// one is undetectable at the far end. SQLSTATE `22003`
/// (`numeric_value_out_of_range`) is what PostgreSQL raises for the same
/// condition, so drivers already classify it as a data error.
///
/// Writes through SQL are range-checked at plan time
/// (`nodedb_sql::planner::dml_helpers::check_declared_int_ranges`), which
/// makes this unreachable for data nodedb accepted itself. It still has to
/// exist: rows written before a column's width was declared, and rows
/// arriving over non-SQL ingest paths, are not covered by that check.
pub(in crate::control::server::pgwire) fn checked_narrow(
    v: &serde_json::Value,
    width: IntWidth,
) -> PgWireResult<Option<i64>> {
    let Some(n) = v.as_i64() else {
        return Ok(None);
    };
    if width.contains(n) {
        return Ok(Some(n));
    }
    Err(out_of_range(format!(
        "value {n} is out of range for type {}",
        width.pg_type_name()
    )))
}

/// Narrow a float cell to the `f32` a `real` column transmits, refusing the
/// one narrowing that is not value-preserving.
///
/// `Ok(None)` means the cell is absent or not a number and encodes as SQL
/// NULL, matching the wider `Float8` arm.
///
/// This is deliberately *not* the float mirror of [`checked_narrow`]'s range
/// constraint — see the module docs. Rounding is correct and never an error;
/// only a finite `f64` overflowing to infinity is refused, under the same
/// SQLSTATE `22003` PostgreSQL raises for the identical conversion. A source
/// value that is *already* infinite or NaN passes through untouched: those are
/// representable in both widths and are not an overflow.
///
/// Writes through SQL are range-checked at plan time
/// (`nodedb_sql::planner::dml_helpers::check_declared_float_ranges`), which
/// makes this unreachable for data nodedb accepted itself. It still has to
/// exist, layered underneath rather than replaced by it: rows written before a
/// column's width was declared, and rows arriving over non-SQL ingest paths,
/// are not covered by that check.
pub(in crate::control::server::pgwire) fn checked_narrow_f32(
    v: &serde_json::Value,
) -> PgWireResult<Option<f32>> {
    let Some(f) = v.as_f64() else {
        return Ok(None);
    };
    let narrowed = f as f32;
    if f.is_finite() && !narrowed.is_finite() {
        return Err(out_of_range(format!(
            "value {f} is out of range for type {}",
            FloatWidth::F32.pg_type_name()
        )));
    }
    Ok(Some(narrowed))
}

/// A pgwire `22003` (`numeric_value_out_of_range`) error — the SQLSTATE
/// PostgreSQL raises when a value cannot be represented in the type its column
/// advertises. Shared by both guards so they surface identically to a driver.
fn out_of_range(message: String) -> pgwire::error::PgWireError {
    pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".into(),
        "22003".into(),
        message,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The SQLSTATE a guard raised, or a panic naming what it raised instead.
    fn sqlstate_of(err: pgwire::error::PgWireError) -> String {
        let pgwire::error::PgWireError::UserError(info) = err else {
            panic!("expected a UserError carrying a SQLSTATE");
        };
        info.code.clone()
    }

    #[test]
    fn integer_inside_declared_width_passes_through() {
        assert_eq!(
            checked_narrow(&json!(i16::MAX as i64), IntWidth::I16).expect("in range"),
            Some(i16::MAX as i64)
        );
        assert_eq!(
            checked_narrow(&json!(-1), IntWidth::I32).expect("in range"),
            Some(-1)
        );
    }

    #[test]
    fn integer_outside_declared_width_is_rejected() {
        let err = checked_narrow(&json!(i16::MAX as i64 + 1), IntWidth::I16)
            .expect_err("one past the boundary must be refused");
        assert_eq!(sqlstate_of(err), "22003");
    }

    #[test]
    fn non_integer_cell_encodes_as_null() {
        assert_eq!(
            checked_narrow(&json!(null), IntWidth::I16).expect("null is not an error"),
            None
        );
        assert_eq!(
            checked_narrow(&json!("x"), IntWidth::I16).expect("text is not an error"),
            None
        );
    }

    /// Narrowing rounds, and rounding is not an error: PostgreSQL's `real`
    /// stores `1.1` as `1.10000002`. Rejecting that would make `REAL` columns
    /// unreadable for almost every value they hold.
    #[test]
    fn float_narrowing_rounds_without_erroring() {
        for v in [json!(1.1), json!(0.0), json!(-2.5), json!(3.4e38)] {
            let narrowed = checked_narrow_f32(&v)
                .expect("an in-range value must narrow without error")
                .expect("a JSON number must narrow to Some");
            assert_eq!(
                narrowed,
                v.as_f64().expect("test value is a number") as f32,
                "{v} must narrow by the ordinary rounding cast"
            );
        }
    }

    /// A finite `f64` beyond `f32`'s range would become `inf` — a real stored
    /// number silently replaced by infinity on the wire.
    #[test]
    fn finite_float_overflowing_f32_is_rejected() {
        for v in [json!(1e39), json!(-1e39), json!(f64::MAX), json!(f64::MIN)] {
            let err = checked_narrow_f32(&v)
                .expect_err("a finite value beyond f32 range must be refused");
            assert_eq!(sqlstate_of(err), "22003", "{v} must report 22003");
        }
    }

    /// The column advertises `real`, so the error must name `real`, not the
    /// `f64` nodedb actually stores.
    #[test]
    fn float_overflow_error_names_the_declared_type() {
        let err =
            checked_narrow_f32(&json!(1e300)).expect_err("1e300 must overflow single precision");
        let pgwire::error::PgWireError::UserError(info) = err else {
            panic!("expected a UserError carrying a SQLSTATE");
        };
        assert!(
            info.message.contains("real"),
            "error must name the advertised type, got: {}",
            info.message
        );
    }

    /// A cell that is not a number at all encodes as SQL NULL, exactly as the
    /// wider `Float8` arm does — never an error.
    #[test]
    fn non_numeric_float_cell_encodes_as_null() {
        assert_eq!(
            checked_narrow_f32(&json!(null)).expect("null is not an error"),
            None
        );
        assert_eq!(
            checked_narrow_f32(&json!("Infinity")).expect("string is not an error"),
            None
        );
    }
}
