// SPDX-License-Identifier: BUSL-1.1

//! The write rule for a declared numeric column of a schemaless document or
//! KV collection.
//!
//! A value meets [`coerce_declared_value`], the rule the strict and columnar
//! encoders apply: it is re-typed, then checked against the declared integer
//! or float width. Every write path of the two engines runs it on the row it
//! is about to store, so a literal and a computed value meet the same rule.
//! Fitting is idempotent: a value the planner already fitted fits to itself.
//!
//! A decimal is stored as its text. That is the form the planner writes for
//! a decimal literal, and the read path renders it at the declared scale.
//! `NULL` passes through: nullability is checked elsewhere.

use std::collections::HashMap;

use nodedb_physical::physical_plan::DeclaredColumn;
use nodedb_types::value::Value;

use super::coerce::coerce_declared_value;

/// `value` as `column` stores it.
///
/// A value out of the declared range is refused with
/// [`crate::Error::NumericValueOutOfRange`] (SQLSTATE 22003).
pub(crate) fn coerce_declared(value: &Value, column: &DeclaredColumn) -> crate::Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let typed = coerce_declared_value(
        value,
        &column.column_type,
        column.int_width,
        column.float_width,
        &column.name,
    )?;
    Ok(match typed {
        Value::Decimal(d) => Value::String(d.to_string()),
        other => other,
    })
}

/// Re-type every declared field of a MessagePack map body.
///
/// Only the declared fields present in the body are rewritten. Every other
/// field keeps its bytes. A body that is not a map, or holds no declared
/// field, is returned unchanged.
pub(crate) fn coerce_declared_body(
    body: Vec<u8>,
    declared: &[DeclaredColumn],
) -> crate::Result<Vec<u8>> {
    if declared.is_empty() {
        return Ok(body);
    }
    let mut replaced: Vec<(&str, Vec<u8>)> = Vec::new();
    for column in declared {
        let Some((start, end)) = nodedb_query::msgpack_scan::extract_field(&body, 0, &column.name)
        else {
            continue;
        };
        let Some(field) = body.get(start..end) else {
            continue;
        };
        let value =
            nodedb_types::value_from_msgpack(field).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("column '{}': {e}", column.name),
            })?;
        let coerced = coerce_declared(&value, column)?;
        if coerced == value {
            continue;
        }
        let encoded =
            nodedb_types::value_to_msgpack(&coerced).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("column '{}': {e}", column.name),
            })?;
        replaced.push((column.name.as_str(), encoded));
    }
    if replaced.is_empty() {
        return Ok(body);
    }
    let pairs: Vec<(&str, &[u8])> = replaced
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    Ok(nodedb_query::msgpack_scan::merge_fields(&body, &pairs))
}

/// Re-type every declared field of a decoded document, in place.
pub(crate) fn coerce_declared_doc(
    doc: &mut serde_json::Value,
    declared: &[DeclaredColumn],
) -> crate::Result<()> {
    if declared.is_empty() {
        return Ok(());
    }
    let Some(object) = doc.as_object_mut() else {
        return Ok(());
    };
    for column in declared {
        let Some(slot) = object.get_mut(&column.name) else {
            continue;
        };
        let coerced = coerce_declared(&Value::from(slot.clone()), column)?;
        *slot = serde_json::Value::from(coerced);
    }
    Ok(())
}

/// Re-type every declared field of a decoded row, in place.
pub(crate) fn coerce_declared_row(
    row: &mut HashMap<String, Value>,
    declared: &[DeclaredColumn],
) -> crate::Result<()> {
    for column in declared {
        let Some(slot) = row.get_mut(&column.name) else {
            continue;
        };
        *slot = coerce_declared(slot, column)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(declared: &str) -> DeclaredColumn {
        DeclaredColumn::from_declared("v", declared).expect("numeric declaration")
    }

    fn body(value: &Value) -> Vec<u8> {
        let mut map = HashMap::new();
        map.insert("id".to_string(), Value::String("a".into()));
        map.insert("v".to_string(), value.clone());
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode body")
    }

    fn field_v(body: &[u8]) -> Value {
        let row = nodedb_types::value_from_msgpack(body).expect("decode body");
        row.get("v").cloned().expect("field v")
    }

    #[test]
    fn decimal_rounds_to_scale_and_is_stored_as_text() {
        let col = column("DECIMAL(5,2)");
        for (input, expected) in [
            (Value::String("1.005".into()), "1.01"),
            (Value::Float(2.5), "2.50"),
            (Value::Integer(7), "7.00"),
        ] {
            assert_eq!(
                coerce_declared(&input, &col).expect("fits"),
                Value::String(expected.into()),
                "{input:?}"
            );
        }
    }

    #[test]
    fn decimal_past_precision_is_numeric_out_of_range() {
        let err = coerce_declared(&Value::Float(1500.0), &column("DECIMAL(5,2)"))
            .expect_err("1500 has four integer digits");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn integer_width_refuses_a_value_past_smallint() {
        let col = column("SMALLINT");
        assert_eq!(
            coerce_declared(&Value::Integer(32767), &col).expect("fits"),
            Value::Integer(32767)
        );
        let err = coerce_declared(&Value::Integer(40000), &col).expect_err("past smallint");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
        let err = coerce_declared(&Value::Float(40000.0), &col).expect_err("whole float");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn real_refuses_only_an_overflow() {
        let col = column("REAL");
        assert_eq!(
            coerce_declared(&Value::Float(1.1), &col).expect("rounds"),
            Value::Float(1.1)
        );
        let err = coerce_declared(&Value::Float(1e300), &col).expect_err("overflows f32");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn null_passes_through() {
        assert_eq!(
            coerce_declared(&Value::Null, &column("SMALLINT")).expect("null"),
            Value::Null
        );
    }

    #[test]
    fn body_rewrites_only_declared_fields() {
        let declared = [column("DECIMAL(5,2)")];
        let rewritten =
            coerce_declared_body(body(&Value::String("1.005".into())), &declared).expect("fits");
        assert_eq!(field_v(&rewritten), Value::String("1.01".into()));

        let unchanged = body(&Value::String("1.50".into()));
        assert_eq!(
            coerce_declared_body(unchanged.clone(), &declared).expect("fits"),
            unchanged
        );

        let err = coerce_declared_body(body(&Value::Integer(123456)), &declared)
            .expect_err("past precision");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn body_without_declared_columns_is_untouched() {
        let raw = b"not a map".to_vec();
        assert_eq!(coerce_declared_body(raw.clone(), &[]).expect("noop"), raw);
        assert_eq!(
            coerce_declared_body(raw.clone(), &[column("SMALLINT")]).expect("not a map"),
            raw
        );
    }

    #[test]
    fn doc_rewrites_declared_fields_in_place() {
        let mut doc = serde_json::json!({ "id": "a", "v": "1.005", "other": 9.999 });
        coerce_declared_doc(&mut doc, &[column("DECIMAL(5,2)")]).expect("fits");
        assert_eq!(doc["v"], serde_json::json!("1.01"));
        assert_eq!(doc["other"], serde_json::json!(9.999));

        let mut doc = serde_json::json!({ "id": "a", "v": 1500.0 });
        let err =
            coerce_declared_doc(&mut doc, &[column("DECIMAL(5,2)")]).expect_err("past precision");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
    }
}
