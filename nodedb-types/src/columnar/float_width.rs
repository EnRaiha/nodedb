// SPDX-License-Identifier: Apache-2.0

//! [`FloatWidth`] — the declared width of a floating-point column.
//!
//! nodedb stores every float as a full `f64` regardless of the width the DDL
//! author declared: `ColumnType::Float64` is the one storage variant for
//! `REAL`, `FLOAT4`, `DOUBLE`, and `FLOAT8` alike. The declared width is
//! therefore not a storage property — it is a *wire contract*: the column's
//! `RowDescription` OID is 700 or 701, and under the binary result format the
//! value is transmitted as 4 or 8 bytes. A client that declared `REAL` decodes
//! exactly four bytes; handing it eight is a decode failure, not a rounding
//! error.
//!
//! # How this differs from [`IntWidth`](super::IntWidth)
//!
//! The integer analogue bounds writes by *full range*: narrowing an
//! out-of-range `i64` wraps, so any value that does not fit a declared
//! `INTEGER` is rejected at write time exactly as PostgreSQL rejects it.
//! Floats are bounded differently. Narrowing an `f64` to an `f32` **rounds** —
//! `1.1` becoming `1.10000002` is correct PostgreSQL `real` behaviour, not an
//! error — so `check_declared_float_ranges` (the float counterpart of
//! `check_declared_int_ranges`) never rejects on rounding. The single failure
//! mode it does reject is *overflow to infinity*: a finite `f64` beyond
//! `f32`'s range becomes `inf`, which PostgreSQL raises `value out of range:
//! overflow` for. That check runs at write time in the planner, mirroring the
//! read-side guard the wire encoder applies at narrowing time — the two are
//! deliberately layered, not redundant: the write-time check is bypassed by
//! rows written before the column's width was declared or via non-SQL
//! ingest, so the read-side guard remains the backstop.
//!
//! Because the keyword set is shared by catalog introspection OIDs and
//! `RowDescription` OIDs alike, [`FloatWidth::from_declared_type`] is the
//! **single** place that maps a declared type string to a float width, so the
//! two can never drift apart.

use serde::{Deserialize, Serialize};

/// The precision at or below which PostgreSQL's `FLOAT(p)` is single-precision.
///
/// `FLOAT(1)`..`FLOAT(24)` is `real`; `FLOAT(25)`..`FLOAT(53)` is
/// `double precision`. See PostgreSQL's numeric-type documentation.
const MAX_SINGLE_PRECISION_P: u32 = 24;

/// The declared width of a floating-point column.
///
/// Ordered narrow → wide; `F64` is the storage width and the default for a
/// float column whose declared type carries no width information.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
#[repr(u8)]
pub enum FloatWidth {
    /// `REAL` / `FLOAT4` / `FLOAT32` / `FLOAT(p<=24)` — 4 bytes on the wire,
    /// PostgreSQL OID 700.
    F32 = 0,
    /// `DOUBLE PRECISION` / `FLOAT8` / `FLOAT64` / `FLOAT` / `FLOAT(p>=25)` —
    /// 8 bytes on the wire, PostgreSQL OID 701.
    F64 = 1,
}

impl FloatWidth {
    /// Resolve a declared SQL type string to a float width, or `None` when the
    /// string does not name a floating-point type.
    ///
    /// Matching is case-insensitive and boundary-aware, using the same idiom as
    /// [`IntWidth::from_declared_type`](super::IntWidth::from_declared_type): a
    /// keyword matches when the (trimmed, lowercased) input either equals it
    /// exactly or is followed by `(` or whitespace, so trailing modifiers
    /// (`"REAL NOT NULL"`) and parameterised spellings still resolve. The
    /// boundary check is also what keeps bare `float` from swallowing
    /// `float4`/`float8`/`float32`/`float64` as prefixes — the next character
    /// is a digit, which is neither `(` nor whitespace — so the arm order below
    /// is not load-bearing.
    ///
    /// # Keyword mapping
    ///
    /// Bare `FLOAT` is **double precision**, matching PostgreSQL (and the SQL
    /// standard), not single. `REAL` is always single. `FLOAT(p)` follows
    /// PostgreSQL's precision rule: `p <= 24` is `real`, `p >= 25` is
    /// `double precision`. A `(p)` that is absent, unterminated, non-numeric,
    /// or outside `1..=53` falls back to the wider `F64` rather than guessing —
    /// widening is the only lossless direction.
    pub fn from_declared_type(declared: &str) -> Option<Self> {
        let normalized = declared.trim().to_ascii_lowercase();
        let is = |name: &str| super::declared_type_matches(&normalized, name);

        if is("real") || is("float4") || is("float32") {
            Some(Self::F32)
        } else if is("double") || is("float8") || is("float64") {
            // `is("double")` already covers `double precision`: the boundary
            // character after `double` is whitespace.
            Some(Self::F64)
        } else if is("float") {
            // Bare `FLOAT`, or the parameterised `FLOAT(p)` form.
            Some(Self::from_float_precision(&normalized))
        } else {
            None
        }
    }

    /// The width PostgreSQL gives a `FLOAT(p)` spelling, given the already
    /// lowercased, trimmed declared type.
    ///
    /// Every malformed shape — no `(`, no closing `)`, non-numeric or
    /// out-of-range `p` — resolves to `F64`, the wider type. This is a total
    /// function by construction: it never indexes, never unwraps, and has no
    /// failure path to propagate, because a bare `FLOAT` legitimately has no
    /// precision and must land on the same answer as a broken one.
    fn from_float_precision(normalized: &str) -> Self {
        let Some(rest) = normalized.strip_prefix("float") else {
            return Self::F64;
        };
        let Some(inner) = rest.trim_start().strip_prefix('(') else {
            return Self::F64;
        };
        let Some(end) = inner.find(')') else {
            return Self::F64;
        };
        let Some(digits) = inner.get(..end) else {
            return Self::F64;
        };
        match digits.trim().parse::<u32>() {
            Ok(p) if (1..=MAX_SINGLE_PRECISION_P).contains(&p) => Self::F32,
            Ok(_) | Err(_) => Self::F64,
        }
    }

    /// The PostgreSQL type OID a column of this width advertises in
    /// `RowDescription` and in catalog introspection.
    pub const fn pg_oid(self) -> u32 {
        match self {
            Self::F32 => 700,
            Self::F64 => 701,
        }
    }

    /// The canonical PostgreSQL type name, for error messages that must read
    /// like PostgreSQL's own (`value out of range for type real`).
    pub const fn pg_type_name(self) -> &'static str {
        match self {
            Self::F32 => "real",
            Self::F64 => "double precision",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_every_declared_float_spelling() {
        let cases: &[(&str, FloatWidth)] = &[
            ("REAL", FloatWidth::F32),
            ("FLOAT4", FloatWidth::F32),
            ("FLOAT32", FloatWidth::F32),
            ("FLOAT", FloatWidth::F64),
            ("FLOAT8", FloatWidth::F64),
            ("FLOAT64", FloatWidth::F64),
            ("DOUBLE", FloatWidth::F64),
            ("DOUBLE PRECISION", FloatWidth::F64),
        ];
        for (declared, expected) in cases {
            assert_eq!(
                FloatWidth::from_declared_type(declared),
                Some(*expected),
                "{declared} must resolve to {expected:?}"
            );
            assert_eq!(
                FloatWidth::from_declared_type(&declared.to_lowercase()),
                Some(*expected),
                "{declared} must resolve case-insensitively"
            );
        }
    }

    /// Bare `FLOAT` is double precision in PostgreSQL, not single. Getting
    /// this backwards is the whole reason catalog introspection and the SELECT
    /// path disagreed for `FLOAT` columns before this type existed.
    #[test]
    fn bare_float_is_double_precision() {
        assert_eq!(
            FloatWidth::from_declared_type("FLOAT"),
            Some(FloatWidth::F64)
        );
        assert_eq!(FloatWidth::F64.pg_oid(), 701);
    }

    /// The boundary check — not the arm order — is what stops `float` from
    /// prefix-matching the numbered spellings. Locking it directly means a
    /// future reordering of the arms cannot silently regress the mapping.
    #[test]
    fn float_does_not_prefix_match_numbered_spellings() {
        for (declared, expected) in [
            ("float4", FloatWidth::F32),
            ("float32", FloatWidth::F32),
            ("float8", FloatWidth::F64),
            ("float64", FloatWidth::F64),
        ] {
            assert_eq!(FloatWidth::from_declared_type(declared), Some(expected));
        }
    }

    /// PostgreSQL's `FLOAT(p)` rule: `p <= 24` is `real`, `p >= 25` is
    /// `double precision`.
    #[test]
    fn float_precision_follows_the_postgres_rule() {
        for p in [1_u32, 10, 24] {
            assert_eq!(
                FloatWidth::from_declared_type(&format!("FLOAT({p})")),
                Some(FloatWidth::F32),
                "FLOAT({p}) must be single precision"
            );
        }
        for p in [25_u32, 40, 53] {
            assert_eq!(
                FloatWidth::from_declared_type(&format!("FLOAT({p})")),
                Some(FloatWidth::F64),
                "FLOAT({p}) must be double precision"
            );
        }
    }

    /// A malformed or out-of-range precision must never panic and must never
    /// narrow — it falls back to the wider `F64`, the only lossless default.
    #[test]
    fn malformed_float_precision_falls_back_to_f64() {
        for declared in [
            "FLOAT(x)",
            "FLOAT(",
            "FLOAT()",
            "FLOAT(24",
            "FLOAT(0)",
            "FLOAT(54)",
            "FLOAT(-3)",
            "FLOAT(99999999999999999999)",
            "FLOAT (24",
        ] {
            assert_eq!(
                FloatWidth::from_declared_type(declared),
                Some(FloatWidth::F64),
                "{declared} must fall back to double precision"
            );
        }
    }

    #[test]
    fn tolerates_trailing_modifiers_and_whitespace() {
        assert_eq!(
            FloatWidth::from_declared_type("REAL NOT NULL"),
            Some(FloatWidth::F32)
        );
        assert_eq!(
            FloatWidth::from_declared_type("  double precision  "),
            Some(FloatWidth::F64)
        );
        assert_eq!(
            FloatWidth::from_declared_type("FLOAT8 NOT NULL"),
            Some(FloatWidth::F64)
        );
    }

    #[test]
    fn rejects_non_float_types() {
        for declared in [
            "TEXT",
            "VARCHAR(20)",
            "INT4",
            "BIGINT",
            "BOOL",
            "TIMESTAMP",
            "GEOMETRY",
            "DECIMAL(10,2)",
            "",
            "floaty",
            "realistic",
        ] {
            assert_eq!(
                FloatWidth::from_declared_type(declared),
                None,
                "{declared} must not resolve to a float width"
            );
        }
    }

    #[test]
    fn pg_oids_match_the_postgres_catalog() {
        assert_eq!(FloatWidth::F32.pg_oid(), 700);
        assert_eq!(FloatWidth::F64.pg_oid(), 701);
    }

    #[test]
    fn pg_type_names_match_the_postgres_catalog() {
        assert_eq!(FloatWidth::F32.pg_type_name(), "real");
        assert_eq!(FloatWidth::F64.pg_type_name(), "double precision");
    }

    /// Narrow → wide ordering, mirroring `IntWidth`, so a future "widest of"
    /// resolution can rely on `Ord` rather than a bespoke comparison.
    #[test]
    fn orders_narrow_to_wide() {
        assert!(FloatWidth::F32 < FloatWidth::F64);
    }
}
