// SPDX-License-Identifier: Apache-2.0

//! Coerce literal DML values into the representation their declared column
//! type requires, for engines that persist the planner's values verbatim.
//!
//! # Why this exists
//!
//! A fractional SQL literal resolves to [`SqlValue::Decimal`] (see
//! `resolver::expr::value::convert_value`, which prefers exact decimal
//! arithmetic over a lossy `f64`). Decimal has no msgpack scalar form, so the
//! Origin planner serializes it as a msgpack *string*. Engines with a typed
//! write path re-type each field against the declared column type before
//! storing it — the strict document encoder parses that string back into a
//! float — so the stored cell always matches what the column declared.
//!
//! Two engines have no typed write path — key-value and document-schemaless.
//! Both store the value bytes they are handed, and the declared schema lives
//! only in the catalog, in the Control Plane. Left uncoerced, a column declared
//! `REAL` / `DOUBLE` holds the string `"1.5"` while `RowDescription` advertises
//! float4 / float8, and the pgwire encoder — which correctly refuses to encode
//! a non-number under a numeric OID — transmits SQL NULL. The client asked for
//! four bytes of `real` and gets nothing: a stored value silently lost in
//! flight.
//!
//! Coercing here, where the declared column types are already in hand, is what
//! makes the declared type authoritative on those write paths too. It is
//! applied on every engine's `VALUES` and `SET` path rather than only theirs:
//! an engine that re-types on write is unaffected by a value arriving
//! pre-typed, and branching on engine here would put a routing decision outside
//! `EngineRules` for no behavioural gain.
//!
//! # Scope
//!
//! Only [`SqlDataType::Int64`] and [`SqlDataType::Float64`] columns are
//! coerced, and both are coerced symmetrically — this is not a float special
//! case. They are the two declared types whose stored representation is
//! decided by the declaration rather than by the value: nodedb keeps every
//! integer as an `i64` and every float as an `f64`, and the declared width
//! that drives the wire OID (see `ColumnInfo::int_width` /
//! `ColumnInfo::float_width`) is only honest if the stored cell is a number of
//! that family to begin with. Every other declared type either has one
//! unambiguous literal form already or (like `DECIMAL`) is deliberately
//! carried as text for exactness, and passes through untouched.
//!
//! The conversions mirror the strict document encoder's `coerce_value`, so the
//! two engines accept and reject exactly the same literals for a given
//! declared type.

use rust_decimal::prelude::ToPrimitive;

use crate::error::{Result, SqlError};
use crate::types::{ColumnInfo, SqlDataType, SqlExpr, SqlValue};

/// Coerce every value in one `(column, value)` row to its declared column
/// type, in place.
///
/// Columns absent from `declared_columns` (a KV collection's untyped
/// `key`/`value` convention, a `ttl` pseudo-column) are left untouched, as is
/// `exempt_column` — see [`coerce_rows_to_declared_types`] for why the primary
/// key is exempt.
pub(super) fn coerce_row_to_declared_types(
    declared_columns: &[ColumnInfo],
    row: &mut [(String, SqlValue)],
    exempt_column: Option<&str>,
) -> Result<()> {
    for (name, value) in row.iter_mut() {
        if is_exempt(exempt_column, name.as_str()) {
            continue;
        }
        let Some(column) = declared_columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name.as_str()))
        else {
            continue;
        };
        let taken = std::mem::replace(value, SqlValue::Null);
        *value = coerce_value(name.as_str(), taken, &column.data_type)?;
    }
    Ok(())
}

/// [`coerce_row_to_declared_types`] across every row of a `VALUES` clause.
///
/// # Why the primary key is exempt
///
/// A row's key is not stored as a typed cell: the KV engine keys on
/// `sql_value_to_bytes` of the literal and the document engines on
/// `sql_value_to_string` of it, and the *read* side renders its own
/// `WHERE pk = <literal>` through those same functions with no coercion in
/// between. Re-typing only the write side would let an inserted key and the
/// key that looks it up disagree byte-for-byte — `VALUES (5.0)` stored under
/// `"5"` but searched for as `"5.0"` — turning a wire-fidelity fix into
/// unfindable rows. The key column keeps its literal exactly as written on
/// both sides.
pub(super) fn coerce_rows_to_declared_types(
    declared_columns: &[ColumnInfo],
    rows: &mut [Vec<(String, SqlValue)>],
    exempt_column: Option<&str>,
) -> Result<()> {
    for row in rows.iter_mut() {
        coerce_row_to_declared_types(declared_columns, row, exempt_column)?;
    }
    Ok(())
}

/// [`coerce_row_to_declared_types`] for `SET col = <literal>` assignments.
///
/// Only literal assignments carry a value at plan time; a computed assignment
/// (`SET n = n + 1`) is evaluated by the engine and is not this pass's to
/// re-type. `exempt_column` carries the same primary-key exemption, for the
/// same reason: `SET pk = <literal>` rewrites the row's identity, which the
/// engines derive from the literal's own rendering.
pub(super) fn coerce_assignments_to_declared_types(
    declared_columns: &[ColumnInfo],
    assignments: &mut [(String, SqlExpr)],
    exempt_column: Option<&str>,
) -> Result<()> {
    for (name, expr) in assignments.iter_mut() {
        if is_exempt(exempt_column, name.as_str()) {
            continue;
        }
        let SqlExpr::Literal(value) = expr else {
            continue;
        };
        let Some(column) = declared_columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name.as_str()))
        else {
            continue;
        };
        let taken = std::mem::replace(value, SqlValue::Null);
        *value = coerce_value(name.as_str(), taken, &column.data_type)?;
    }
    Ok(())
}

/// Whether `column` is the exempted (primary-key) column, compared the same
/// case-insensitive way as every other column-name lookup here.
fn is_exempt(exempt_column: Option<&str>, column: &str) -> bool {
    exempt_column.is_some_and(|exempt| exempt.eq_ignore_ascii_case(column))
}

/// Coerce one literal to `declared`, returning it unchanged when the declared
/// type imposes no representation of its own.
fn coerce_value(column: &str, value: SqlValue, declared: &SqlDataType) -> Result<SqlValue> {
    match declared {
        SqlDataType::Int64 => coerce_to_int(column, value),
        SqlDataType::Float64 => coerce_to_float(column, value),
        SqlDataType::String
        | SqlDataType::Bool
        | SqlDataType::Bytes
        | SqlDataType::Timestamp
        | SqlDataType::Timestamptz
        | SqlDataType::Decimal
        | SqlDataType::Uuid
        | SqlDataType::Vector(_)
        | SqlDataType::Geometry => Ok(value),
    }
}

/// Integer column: an `i64` passes through; a float or decimal literal is
/// accepted only when it names a whole number, and a numeric string is parsed.
///
/// A fractional literal is refused rather than truncated — the column declared
/// an integer, and silently dropping the fraction is the same class of
/// invisible data change this module exists to prevent. `NULL` and non-numeric
/// values are left alone: nullability and type checking are enforced
/// elsewhere, and this pass only fixes representation.
fn coerce_to_int(column: &str, value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Float(f) => whole_f64(f)
            .map(SqlValue::Int)
            .ok_or_else(|| not_representable(column, &f.to_string(), "INT")),
        SqlValue::Decimal(d) => match d.is_integer().then(|| d.to_i64()).flatten() {
            Some(n) => Ok(SqlValue::Int(n)),
            None => Err(not_representable(column, &d.to_string(), "INT")),
        },
        SqlValue::String(s) => s
            .parse::<i64>()
            .map(SqlValue::Int)
            .map_err(|_| not_representable(column, &s, "INT")),
        other => Ok(other),
    }
}

/// Float column: an `f64` passes through; integer and decimal literals become
/// the `f64` the column stores, and a numeric string is parsed.
///
/// Narrowing a decimal to `f64` is the same rounding PostgreSQL performs
/// assigning `numeric` to `double precision`, so it is not an error — only a
/// decimal with no `f64` image at all is refused.
fn coerce_to_float(column: &str, value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Int(i) => Ok(SqlValue::Float(i as f64)),
        SqlValue::Decimal(d) => d
            .to_f64()
            .map(SqlValue::Float)
            .ok_or_else(|| not_representable(column, &d.to_string(), "FLOAT")),
        SqlValue::String(s) => s
            .parse::<f64>()
            .map(SqlValue::Float)
            .map_err(|_| not_representable(column, &s, "FLOAT")),
        other => Ok(other),
    }
}

/// `Some(n)` when `f` is a whole number inside `i64`'s range.
fn whole_f64(f: f64) -> Option<i64> {
    (f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64).then_some(f as i64)
}

fn not_representable(column: &str, value: &str, declared: &str) -> SqlError {
    SqlError::TypeMismatch {
        detail: format!("column '{column}': cannot store '{value}' as {declared}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, data_type: SqlDataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        }
    }

    fn coerced(columns: &[ColumnInfo], name: &str, value: SqlValue) -> Result<SqlValue> {
        coerced_with_exemption(columns, name, value, None)
    }

    fn coerced_with_exemption(
        columns: &[ColumnInfo],
        name: &str,
        value: SqlValue,
        exempt_column: Option<&str>,
    ) -> Result<SqlValue> {
        let mut rows = vec![vec![(name.to_string(), value)]];
        coerce_rows_to_declared_types(columns, &mut rows, exempt_column)?;
        Ok(rows.remove(0).remove(0).1)
    }

    fn decimal(text: &str) -> SqlValue {
        SqlValue::Decimal(
            rust_decimal::Decimal::from_str_exact(text).expect("test decimal literal parses"),
        )
    }

    /// The failing case this module exists for: a fractional literal bound for
    /// a declared float column must reach storage as a number, not as the
    /// decimal's text form.
    #[test]
    fn fractional_literal_becomes_a_float_for_a_float_column() {
        let columns = [column("r", SqlDataType::Float64)];
        assert_eq!(
            coerced(&columns, "r", decimal("1.5")).expect("1.5 fits a float column"),
            SqlValue::Float(1.5)
        );
    }

    /// Integers are coerced by the same rule, in both directions: an integer
    /// literal in a float column widens, and an integer column keeps its
    /// `i64` untouched.
    #[test]
    fn integer_literals_follow_the_declared_family() {
        let columns = [
            column("r", SqlDataType::Float64),
            column("n", SqlDataType::Int64),
        ];
        assert_eq!(
            coerced(&columns, "r", SqlValue::Int(5)).expect("an integer fits a float column"),
            SqlValue::Float(5.0)
        );
        assert_eq!(
            coerced(&columns, "n", SqlValue::Int(5)).expect("an integer fits an integer column"),
            SqlValue::Int(5)
        );
    }

    /// A whole-numbered decimal or float is a valid integer; a fractional one
    /// is refused rather than truncated.
    #[test]
    fn integer_column_accepts_whole_numbers_and_refuses_fractions() {
        let columns = [column("n", SqlDataType::Int64)];
        assert_eq!(
            coerced(&columns, "n", decimal("3.0")).expect("3.0 is a whole number"),
            SqlValue::Int(3)
        );
        assert_eq!(
            coerced(&columns, "n", SqlValue::Float(4.0)).expect("4.0 is a whole number"),
            SqlValue::Int(4)
        );
        assert!(
            coerced(&columns, "n", decimal("1.5")).is_err(),
            "a fractional value must not be silently truncated into an INT column"
        );
    }

    /// Numeric text is parsed into the declared family; text that is not a
    /// number at all is an error rather than a stored string under a numeric
    /// wire type.
    #[test]
    fn numeric_strings_are_parsed_and_non_numeric_text_is_refused() {
        let columns = [
            column("r", SqlDataType::Float64),
            column("n", SqlDataType::Int64),
        ];
        assert_eq!(
            coerced(&columns, "r", SqlValue::String("2.25".into())).expect("numeric text parses"),
            SqlValue::Float(2.25)
        );
        assert_eq!(
            coerced(&columns, "n", SqlValue::String("7".into())).expect("numeric text parses"),
            SqlValue::Int(7)
        );
        assert!(coerced(&columns, "r", SqlValue::String("abc".into())).is_err());
    }

    /// NULL is untouched — nullability is enforced elsewhere, and this pass
    /// only fixes representation.
    #[test]
    fn null_passes_through_every_declared_type() {
        let columns = [
            column("r", SqlDataType::Float64),
            column("n", SqlDataType::Int64),
        ];
        assert_eq!(
            coerced(&columns, "r", SqlValue::Null).expect("null is not an error"),
            SqlValue::Null
        );
        assert_eq!(
            coerced(&columns, "n", SqlValue::Null).expect("null is not an error"),
            SqlValue::Null
        );
    }

    /// Non-numeric declared types impose no representation of their own: a
    /// `DECIMAL` column keeps its exact decimal, and a `TEXT` column keeps
    /// whatever literal it was given.
    #[test]
    fn non_numeric_declared_types_pass_through_untouched() {
        let columns = [
            column("d", SqlDataType::Decimal),
            column("t", SqlDataType::String),
        ];
        assert_eq!(
            coerced(&columns, "d", decimal("1.5")).expect("decimal columns keep exact decimals"),
            decimal("1.5")
        );
        assert_eq!(
            coerced(&columns, "t", SqlValue::Int(9)).expect("text columns are untouched"),
            SqlValue::Int(9)
        );
    }

    /// The primary key keeps its literal exactly as written even when its
    /// declared type would otherwise re-type it: the engines derive a row's
    /// identity from the literal's own rendering on both the write and the
    /// read side, so coercing one side would make the row unfindable.
    #[test]
    fn the_primary_key_column_is_exempt() {
        let columns = [
            column("id", SqlDataType::Int64),
            column("n", SqlDataType::Int64),
        ];
        assert_eq!(
            coerced_with_exemption(&columns, "id", decimal("5.0"), Some("id"))
                .expect("an exempt key is never re-typed"),
            decimal("5.0")
        );
        // The exemption is by name and applies to that column only — a
        // non-key column of the same declared type still coerces.
        assert_eq!(
            coerced_with_exemption(&columns, "n", decimal("5.0"), Some("id"))
                .expect("5.0 is a whole number"),
            SqlValue::Int(5)
        );
    }

    /// `SET pk = <literal>` carries the same exemption, and a non-key
    /// assignment beside it is still coerced.
    #[test]
    fn assignments_honour_the_primary_key_exemption() {
        let columns = [
            column("id", SqlDataType::Float64),
            column("r", SqlDataType::Float64),
        ];
        let mut assignments = vec![
            ("id".to_string(), SqlExpr::Literal(decimal("1.5"))),
            ("r".to_string(), SqlExpr::Literal(decimal("1.5"))),
        ];
        coerce_assignments_to_declared_types(&columns, &mut assignments, Some("id"))
            .expect("both assignments are representable");
        let literal = |expr: &SqlExpr| match expr {
            SqlExpr::Literal(value) => value.clone(),
            other => panic!("expected a literal assignment, got {other:?}"),
        };
        assert_eq!(literal(&assignments[0].1), decimal("1.5"));
        assert_eq!(literal(&assignments[1].1), SqlValue::Float(1.5));
    }

    /// A column the catalog does not declare (the KV `key`/`value`
    /// convention, a `ttl` pseudo-column) is left exactly as written.
    #[test]
    fn undeclared_columns_are_left_alone() {
        let columns = [column("r", SqlDataType::Float64)];
        assert_eq!(
            coerced(&columns, "ttl", decimal("1.5")).expect("undeclared columns are untouched"),
            decimal("1.5")
        );
    }
}
