// SPDX-License-Identifier: Apache-2.0

//! [`IntWidth`] — the declared width of an integer column.
//!
//! nodedb stores every integer as a full `i64` regardless of the width the
//! DDL author declared: `ColumnType::Int64` is the one storage variant for
//! `SMALLINT`, `INTEGER`, and `BIGINT` alike. The declared width is therefore
//! not a storage property — it is a *constraint plus a wire contract*:
//!
//! * **Constraint** — a value written to a column declared `INTEGER` must fit
//!   in an `i32`, exactly as PostgreSQL enforces (`integer out of range`).
//!   Without this, the wire contract below cannot be honoured.
//! * **Wire contract** — the column's `RowDescription` OID is 21/23/20, and
//!   under the binary result format the value is transmitted as 2/4/8 bytes.
//!   A client that declared `SMALLINT` decodes exactly two bytes; handing it
//!   a wider value is silent corruption, not a rounding error.
//!
//! Because both of those depend on recognising the same set of SQL keywords,
//! [`IntWidth::from_declared_type`] is the **single** place that maps a
//! declared type string to a width. Every consumer — catalog introspection
//! OIDs, `RowDescription` OIDs, and write-time range validation — resolves
//! through it, so the three can never drift apart.

use serde::{Deserialize, Serialize};

/// The declared width of an integer column.
///
/// Ordered narrow → wide; `I64` is the storage width and the default for an
/// integer column whose declared type carries no width information.
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
pub enum IntWidth {
    /// `SMALLINT` / `INT2` — 2 bytes on the wire, PostgreSQL OID 21.
    I16 = 0,
    /// `INTEGER` / `INT` / `INT4` — 4 bytes on the wire, PostgreSQL OID 23.
    I32 = 1,
    /// `BIGINT` / `INT8` / `INT64` — 8 bytes on the wire, PostgreSQL OID 20.
    I64 = 2,
}

impl IntWidth {
    /// Resolve a declared SQL type string to an integer width, or `None` when
    /// the string does not name an integer type.
    ///
    /// Matching is case-insensitive and boundary-aware: a keyword matches when
    /// the (trimmed, lowercased) input either equals it exactly or is followed
    /// by `(` or whitespace, so trailing modifiers (`"BIGINT NOT NULL"`) and
    /// parameterised spellings still resolve. The boundary check is also what
    /// keeps `int` from swallowing `int2`/`int4`/`int8`/`int64` as prefixes —
    /// the next character is a digit, which is neither `(` nor whitespace —
    /// so the arm order below is not load-bearing.
    ///
    /// This is the single source of truth for keyword → width. Callers that
    /// need an OID or a range check derive it from the returned width rather
    /// than re-parsing the string.
    pub fn from_declared_type(declared: &str) -> Option<Self> {
        let normalized = declared.trim().to_ascii_lowercase();
        let is = |name: &str| super::declared_type_matches(&normalized, name);

        if is("smallint") || is("int2") {
            Some(Self::I16)
        } else if is("integer") || is("int4") || is("int") {
            Some(Self::I32)
        } else if is("bigint") || is("int8") || is("int64") {
            Some(Self::I64)
        } else {
            None
        }
    }

    /// The inclusive value range a column of this width accepts.
    pub const fn range(self) -> (i64, i64) {
        match self {
            Self::I16 => (i16::MIN as i64, i16::MAX as i64),
            Self::I32 => (i32::MIN as i64, i32::MAX as i64),
            Self::I64 => (i64::MIN, i64::MAX),
        }
    }

    /// Whether `value` fits in a column of this declared width.
    pub const fn contains(self, value: i64) -> bool {
        let (min, max) = self.range();
        value >= min && value <= max
    }

    /// The PostgreSQL type OID a column of this width advertises in
    /// `RowDescription` and in catalog introspection.
    pub const fn pg_oid(self) -> u32 {
        match self {
            Self::I16 => 21,
            Self::I32 => 23,
            Self::I64 => 20,
        }
    }

    /// The canonical PostgreSQL type name, for error messages that must read
    /// like PostgreSQL's own (`integer out of range`).
    pub const fn pg_type_name(self) -> &'static str {
        match self {
            Self::I16 => "smallint",
            Self::I32 => "integer",
            Self::I64 => "bigint",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_every_declared_integer_spelling() {
        let cases: &[(&str, IntWidth)] = &[
            ("SMALLINT", IntWidth::I16),
            ("INT2", IntWidth::I16),
            ("INTEGER", IntWidth::I32),
            ("INT", IntWidth::I32),
            ("INT4", IntWidth::I32),
            ("BIGINT", IntWidth::I64),
            ("INT8", IntWidth::I64),
            ("INT64", IntWidth::I64),
        ];
        for (declared, expected) in cases {
            assert_eq!(
                IntWidth::from_declared_type(declared),
                Some(*expected),
                "{declared} must resolve to {expected:?}"
            );
            assert_eq!(
                IntWidth::from_declared_type(&declared.to_lowercase()),
                Some(*expected),
                "{declared} must resolve case-insensitively"
            );
        }
    }

    /// The boundary check — not the arm order — is what stops `int` from
    /// prefix-matching the numbered spellings. Locking it directly means a
    /// future reordering of the arms cannot silently regress the mapping.
    #[test]
    fn int_does_not_prefix_match_numbered_spellings() {
        for (declared, expected) in [
            ("int2", IntWidth::I16),
            ("int4", IntWidth::I32),
            ("int8", IntWidth::I64),
            ("int64", IntWidth::I64),
        ] {
            assert_eq!(IntWidth::from_declared_type(declared), Some(expected));
        }
    }

    #[test]
    fn tolerates_trailing_modifiers_and_parameters() {
        assert_eq!(
            IntWidth::from_declared_type("BIGINT NOT NULL"),
            Some(IntWidth::I64)
        );
        assert_eq!(
            IntWidth::from_declared_type("  smallint  "),
            Some(IntWidth::I16)
        );
        assert_eq!(IntWidth::from_declared_type("int(11)"), Some(IntWidth::I32));
    }

    #[test]
    fn rejects_non_integer_types() {
        for declared in [
            "TEXT",
            "VARCHAR(20)",
            "FLOAT8",
            "BOOL",
            "TIMESTAMP",
            "GEOMETRY",
            "",
            "intergalactic",
        ] {
            assert_eq!(
                IntWidth::from_declared_type(declared),
                None,
                "{declared} must not resolve to an integer width"
            );
        }
    }

    #[test]
    fn range_matches_the_rust_primitive_it_names() {
        assert_eq!(IntWidth::I16.range(), (i16::MIN as i64, i16::MAX as i64));
        assert_eq!(IntWidth::I32.range(), (i32::MIN as i64, i32::MAX as i64));
        assert_eq!(IntWidth::I64.range(), (i64::MIN, i64::MAX));
    }

    #[test]
    fn contains_rejects_values_one_past_the_boundary() {
        assert!(IntWidth::I16.contains(i16::MAX as i64));
        assert!(!IntWidth::I16.contains(i16::MAX as i64 + 1));
        assert!(IntWidth::I16.contains(i16::MIN as i64));
        assert!(!IntWidth::I16.contains(i16::MIN as i64 - 1));

        assert!(IntWidth::I32.contains(i32::MAX as i64));
        assert!(!IntWidth::I32.contains(i32::MAX as i64 + 1));
        assert!(IntWidth::I32.contains(i32::MIN as i64));
        assert!(!IntWidth::I32.contains(i32::MIN as i64 - 1));

        assert!(IntWidth::I64.contains(i64::MAX));
        assert!(IntWidth::I64.contains(i64::MIN));
    }

    #[test]
    fn pg_oids_match_the_postgres_catalog() {
        assert_eq!(IntWidth::I16.pg_oid(), 21);
        assert_eq!(IntWidth::I32.pg_oid(), 23);
        assert_eq!(IntWidth::I64.pg_oid(), 20);
    }
}
