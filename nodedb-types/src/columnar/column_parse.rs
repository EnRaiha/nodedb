// SPDX-License-Identifier: Apache-2.0

//! [`std::fmt::Display`] and [`std::str::FromStr`] for [`ColumnType`], plus
//! [`ColumnTypeParseError`].

use std::fmt;
use std::str::FromStr;

use super::column_type::ColumnType;

/// Error from parsing a column type string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ColumnTypeParseError {
    #[error("unknown column type: '{0}'")]
    Unknown(String),
    #[error("'DATETIME' is not a valid type — use 'TIMESTAMP' instead")]
    UseTimestamp,
    #[error("invalid VECTOR dimension: '{0}' (must be a positive integer)")]
    InvalidVectorDim(String),
    #[error(
        "invalid DECIMAL/NUMERIC params: '{0}' (expected DECIMAL(precision, scale) with precision 1-38 and scale <= precision)"
    )]
    InvalidDecimalParams(String),
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int64 => f.write_str("BIGINT"),
            Self::Float64 => f.write_str("FLOAT64"),
            Self::String => f.write_str("TEXT"),
            Self::Bool => f.write_str("BOOL"),
            Self::Bytes => f.write_str("BYTES"),
            Self::Timestamp => f.write_str("TIMESTAMP"),
            Self::Timestamptz => f.write_str("TIMESTAMPTZ"),
            Self::SystemTimestamp => f.write_str("SYSTEM_TIMESTAMP"),
            Self::Decimal { precision, scale } => write!(f, "DECIMAL({precision},{scale})"),
            Self::Geometry => f.write_str("GEOMETRY"),
            Self::Vector(dim) => write!(f, "VECTOR({dim})"),
            Self::SparseVector => f.write_str("SPARSEVECTOR"),
            Self::Uuid => f.write_str("UUID"),
            Self::Json => f.write_str("JSON"),
            Self::Ulid => f.write_str("ULID"),
            Self::Duration => f.write_str("DURATION"),
            Self::Array => f.write_str("ARRAY"),
            Self::Set => f.write_str("SET"),
            Self::Regex => f.write_str("REGEX"),
            Self::Range => f.write_str("RANGE"),
            Self::Record => f.write_str("RECORD"),
        }
    }
}

impl FromStr for ColumnType {
    type Err = ColumnTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.trim().to_uppercase();

        // NUMERIC(p,s) / DECIMAL(p,s) special case.
        if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
            let base = if upper.starts_with("NUMERIC") {
                "NUMERIC"
            } else {
                "DECIMAL"
            };
            let rest = upper[base.len()..].trim();
            if rest.is_empty() {
                return Ok(Self::Decimal {
                    precision: 38,
                    scale: 10,
                });
            }
            if rest.starts_with('(') && rest.ends_with(')') {
                let inner = &rest[1..rest.len() - 1];
                let parts: Vec<&str> = inner.splitn(2, ',').collect();
                let precision: u8 = parts[0]
                    .trim()
                    .parse()
                    .map_err(|_| ColumnTypeParseError::InvalidDecimalParams(rest.to_string()))?;
                let scale: u8 = parts
                    .get(1)
                    .map(|p| p.trim())
                    .unwrap_or("0")
                    .parse()
                    .map_err(|_| ColumnTypeParseError::InvalidDecimalParams(rest.to_string()))?;
                if precision == 0 || precision > 38 {
                    return Err(ColumnTypeParseError::InvalidDecimalParams(format!(
                        "precision {precision} out of range 1-38"
                    )));
                }
                if scale > precision {
                    return Err(ColumnTypeParseError::InvalidDecimalParams(format!(
                        "scale {scale} must be <= precision {precision}"
                    )));
                }
                return Ok(Self::Decimal { precision, scale });
            }
            return Err(ColumnTypeParseError::InvalidDecimalParams(rest.to_string()));
        }

        // VECTOR(N) special case.
        if upper.starts_with("VECTOR") {
            let inner = upper
                .trim_start_matches("VECTOR")
                .trim()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .trim();
            if inner.is_empty() {
                return Err(ColumnTypeParseError::InvalidVectorDim("empty".into()));
            }
            let dim: u32 = inner
                .parse()
                .map_err(|_| ColumnTypeParseError::InvalidVectorDim(inner.into()))?;
            if dim == 0 {
                return Err(ColumnTypeParseError::InvalidVectorDim("0".into()));
            }
            return Ok(Self::Vector(dim));
        }

        match upper.as_str() {
            // `INT4`/`INT8`/`SMALLINT`/`INT2` are PostgreSQL wire-width integer
            // keywords: strict/kv `CREATE COLLECTION` used to reject
            // them as unknown types even though they're valid aliases. They
            // all collapse to the same `Int64` storage variant as
            // `BIGINT`/`INTEGER`/`INT` — nodedb always stores integers as a
            // full i64. The declared width is carried separately as an
            // [`super::IntWidth`], which is what bounds writes and narrows the
            // advertised wire OID; it is deliberately not a storage variant.
            "BIGINT" | "INT64" | "INTEGER" | "INT" | "INT4" | "INT8" | "SMALLINT" | "INT2" => {
                Ok(Self::Int64)
            }
            // `FLOAT4`/`FLOAT8`/`FLOAT32`/`DOUBLE PRECISION` are PostgreSQL
            // wire-width float keywords, rejected as unknown types here for
            // the same reason `INT4` was. They all collapse to
            // the same `Float64` storage variant as `DOUBLE`/`REAL`/`FLOAT` —
            // nodedb always stores floats as a full f64. The declared width is
            // carried separately as a [`super::FloatWidth`], which narrows the
            // advertised wire OID; it is deliberately not a storage variant.
            // Unlike integers it bounds no writes: narrowing a float rounds
            // rather than wraps, and PostgreSQL accepts-and-rounds too.
            "FLOAT64" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "REAL" | "FLOAT4"
            | "FLOAT32" | "FLOAT" => Ok(Self::Float64),
            "TEXT" | "STRING" | "VARCHAR" => Ok(Self::String),
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "BYTES" | "BYTEA" | "BLOB" => Ok(Self::Bytes),
            "TIMESTAMP" => Ok(Self::Timestamp),
            "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Ok(Self::Timestamptz),
            "SYSTEM_TIMESTAMP" | "SYSTEMTIMESTAMP" => Ok(Self::SystemTimestamp),
            "GEOMETRY" => Ok(Self::Geometry),
            // Dimensionless: an exact keyword with no `(N)`. Placed as an exact
            // match rather than a `starts_with` guard because "SPARSEVECTOR"
            // does not prefix-collide with the `starts_with("VECTOR")` branch
            // above ("SPARSEVECTOR" starts with "SPARSE", not "VECTOR").
            "SPARSEVECTOR" => Ok(Self::SparseVector),
            "UUID" => Ok(Self::Uuid),
            "JSON" | "JSONB" => Ok(Self::Json),
            "ULID" => Ok(Self::Ulid),
            "DURATION" => Ok(Self::Duration),
            "ARRAY" => Ok(Self::Array),
            "SET" => Ok(Self::Set),
            "REGEX" => Ok(Self::Regex),
            "RANGE" => Ok(Self::Range),
            "RECORD" => Ok(Self::Record),
            "DATETIME" => Err(ColumnTypeParseError::UseTimestamp),
            other => Err(ColumnTypeParseError::Unknown(other.to_string())),
        }
    }
}
