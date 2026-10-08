// SPDX-License-Identifier: Apache-2.0

//! Value conversion and SQL formatting helpers for the remote client.
//!
//! Holds the pgwire row-cell entry point (the decoders live in
//! [`crate::pg_cell`]), JSON-to-Value mapping, and SQL array formatting. Shared row decoders
//! (column-level `value_as_*`, `_system.dropped_collections` parsing)
//! live in [`crate::row_decode`] instead so the trait default impls can
//! reuse them without dragging in pgwire types.

use nodedb_types::Value;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::pg_cell::{RawCell, decode_cell};

/// Convert cell `idx` of a `tokio_postgres` row, whose column is `column`,
/// to `nodedb_types::Value` by the column type. A cell that does not decode
/// is an error naming the column, the PG type and the cell text, never a
/// NULL.
pub(crate) fn pg_value_to_value(
    row: &tokio_postgres::Row,
    idx: usize,
    column: &tokio_postgres::Column,
) -> NodeDbResult<Value> {
    let raw = row.try_get::<_, Option<RawCell<'_>>>(idx).map_err(|e| {
        NodeDbError::serialization(
            "pgwire",
            format!(
                "column \"{}\" of type {}: cannot read the cell: {e}",
                column.name(),
                column.type_().name()
            ),
        )
    })?;
    decode_cell(column.name(), column.type_(), raw.map(|cell| cell.0))
}

/// Convert `serde_json::Value` to `nodedb_types::Value`.
///
/// A JSON number keeps the kind the native client decodes it to: an integer
/// in `i64` range is an `Integer`, a larger unsigned integer is the `Decimal`
/// of [`Value::from_u64`], and every other number is a `Float`. A number
/// with no `f64` form is an error naming it, never a guessed value.
pub(crate) fn json_to_value(v: &serde_json::Value) -> NodeDbResult<Value> {
    Ok(match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => Value::Integer(i),
            (None, Some(u), _) => Value::from_u64(u),
            (None, None, Some(f)) => Value::Float(f),
            (None, None, None) => {
                return Err(NodeDbError::serialization(
                    "json",
                    format!("number {n} has no integer or f64 form"),
                ));
            }
        },
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(a) => {
            Value::Array(a.iter().map(json_to_value).collect::<NodeDbResult<_>>()?)
        }
        serde_json::Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| Ok((k.clone(), json_to_value(v)?)))
                .collect::<NodeDbResult<_>>()?,
        ),
    })
}

/// Format an f32 slice as a SQL ARRAY literal: `ARRAY[0.1,0.2,0.3]`.
pub(crate) fn format_vector_array(v: &[f32]) -> String {
    let inner: Vec<String> = v.iter().map(|f| format!("{f}")).collect();
    format!("ARRAY[{}]", inner.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_vector_array_works() {
        let arr = format_vector_array(&[0.1, 0.2, 0.3]);
        assert_eq!(arr, "ARRAY[0.1,0.2,0.3]");
    }

    #[test]
    fn format_vector_array_empty() {
        let arr = format_vector_array(&[]);
        assert_eq!(arr, "ARRAY[]");
    }

    fn decoded(v: serde_json::Value) -> Value {
        json_to_value(&v).expect("every serde_json number has an f64 form")
    }

    #[test]
    fn json_to_value_primitives() {
        assert_eq!(decoded(serde_json::json!(null)), Value::Null);
        assert_eq!(decoded(serde_json::json!(true)), Value::Bool(true));
        assert_eq!(decoded(serde_json::json!(42)), Value::Integer(42));
        assert_eq!(decoded(serde_json::json!(2.5)), Value::Float(2.5));
        assert_eq!(
            decoded(serde_json::json!("hello")),
            Value::String("hello".into())
        );
    }

    #[test]
    fn json_to_value_keeps_a_u64_above_i64_max_exact() {
        assert_eq!(
            decoded(serde_json::json!(u64::MAX)),
            Value::from_u64(u64::MAX)
        );
    }

    #[test]
    fn json_to_value_nested() {
        let v = decoded(serde_json::json!({"a": [1, 2]}));
        assert!(matches!(v, Value::Object(_)));
    }
}
