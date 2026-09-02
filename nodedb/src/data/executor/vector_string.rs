// SPDX-License-Identifier: BUSL-1.1

//! Vector-string parsing shared by the strict UPDATE coerce path and the
//! schemaless point-put vector indexer.
//!
//! One field, two shapes: a vector arrives as `Value::Array` (native
//! msgpack) or `Value::String` — an SQL string literal serialized by the
//! planner's `SqlValue::String`. Both consumers must agree on the accepted
//! string forms and on what counts as malformed. A malformed or empty body
//! is an error, never a silent skip: an empty embedding is a missing
//! embedding, and a document that silently loses its embedding is
//! indistinguishable from one that never had it.

use nodedb_types::Value;

/// Parse a vector from the string representations the planner can produce.
///
/// Handles:
/// - `"[0.1, 0.2, 0.3]"` (JSON-style)
/// - `"ARRAY[0.1, 0.2, 0.3]"` (SQL literal)
/// - `"ArrayLiteral([Literal(Float(0.9)), Literal(Float(0.1)), ...])"` (sqlparser debug repr)
///
/// A bare number (`"7"`) is refused: it would build a dim-1 index and block
/// every real embedding written after it. Non-finite values (`"nan"`, `"inf"`)
/// are refused — NaN breaks HNSW ordering.
pub(crate) fn parse_vector_string(s: &str) -> Option<Vec<f32>> {
    // Try ARRAY[...] SQL literal format.
    if nodedb_types::starts_with_ascii_case_insensitive(s, "ARRAY[") {
        let start = "ARRAY[".len();
        let end = s.rfind(']')?;
        if end <= start {
            return None;
        }
        let inner = &s[start..end];
        return parse_float_list(inner);
    }

    // Try JSON-style [0.1, 0.2, ...] format. Require both brackets so a bare
    // number or prose never parses as a dim-1 vector.
    if s.starts_with('[') && s.ends_with(']') {
        let inner = &s[1..s.len() - 1];
        return parse_float_list(inner);
    }

    // Try sqlparser debug repr: "ArrayLiteral([Literal(Float(0.9)), ...])"
    if s.starts_with("ArrayLiteral(") {
        let floats: Vec<f32> = s
            .split("Float(")
            .skip(1)
            .filter_map(|chunk| {
                let end = chunk.find(')')?;
                chunk[..end].parse::<f32>().ok()
            })
            .collect();
        if !floats.is_empty() && floats.iter().all(|f| f.is_finite()) {
            return Some(floats);
        }
    }

    None
}

/// Split a comma-separated float list, rejecting any token that is not a
/// finite number. A list with one bad token returns `None` wholesale.
///
/// Comma is the only separator: no planner output produces a
/// whitespace-separated vector, and accepting one would widen the grammar
/// beyond what any caller emits.
fn parse_float_list(inner: &str) -> Option<Vec<f32>> {
    let mut floats = Vec::new();
    for tok in inner.split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        let f: f32 = tok.parse().ok()?;
        if !f.is_finite() {
            return None;
        }
        floats.push(f);
    }
    Some(floats)
}

fn vector_error(collection: &str, field_name: &str, detail: String) -> crate::Error {
    crate::Error::RejectedConstraint {
        collection: collection.to_string(),
        constraint: format!("vector field '{field_name}'"),
        detail,
    }
}

/// Extract the float vector from a field value on a point put.
///
/// Accepts the native `Value::Array` (message-pack floats/integers/decimals,
/// or numeric strings), plus the `Value::String` form the planner's
/// `SqlValue::String` produces for an SQL string literal (`"[0.1, 0.2, ...]"`,
/// `"ARRAY[...]"`, or the sqlparser debug repr).
///
/// `Ok(None)` means the value is absent or not embedding-shaped at all (a
/// scalar, an object) — callers treat it as "no vector field". `Err` means
/// the field IS present but unparseable, empty, or non-finite — callers must
/// reject the write, not skip it silently. `collection` and `field_name`
/// name the failed site so an operator sees which document shape to fix.
///
/// `Value::Bytes` is deliberately `Ok(None)` and not an error: the strict
/// tuple decoder materializes a `Vector(dim)` column as `Value::Array`
/// (`nodedb_strict::decode::value`), and the forward put path hands this
/// function pre-coerce MessagePack, so no caller can deliver the coerced
/// little-endian byte form here.
pub(crate) fn floats_from_value(
    collection: &str,
    field_name: &str,
    value: &Value,
) -> crate::Result<Option<Vec<f32>>> {
    match value {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Err(vector_error(
                    collection,
                    field_name,
                    "expected a non-empty vector, got an empty array".to_string(),
                ));
            }
            let mut floats = Vec::with_capacity(arr.len());
            for v in arr {
                let f = match v {
                    Value::Float(f) => *f as f32,
                    Value::Integer(i) => *i as f32,
                    Value::Decimal(d) => {
                        use rust_decimal::prelude::ToPrimitive;
                        d.to_f32().ok_or_else(|| {
                            vector_error(
                                collection,
                                field_name,
                                format!("expected a numeric element, got {d}"),
                            )
                        })?
                    }
                    Value::String(s) => s.parse::<f32>().map_err(|_| {
                        vector_error(
                            collection,
                            field_name,
                            format!("expected a numeric element, got String({s:?})"),
                        )
                    })?,
                    other => {
                        return Err(vector_error(
                            collection,
                            field_name,
                            format!("expected a numeric element, got {other:?}"),
                        ));
                    }
                };
                if !f.is_finite() {
                    return Err(vector_error(
                        collection,
                        field_name,
                        format!("expected a finite element, got {f}"),
                    ));
                }
                floats.push(f);
            }
            Ok(Some(floats))
        }
        Value::String(s) => {
            let s = s.trim();
            let floats = parse_vector_string(s).ok_or_else(|| {
                vector_error(
                    collection,
                    field_name,
                    format!("expected a VECTOR array literal, got String({s:?})"),
                )
            })?;
            if floats.is_empty() {
                return Err(vector_error(
                    collection,
                    field_name,
                    "expected a non-empty vector, got an empty array literal".to_string(),
                ));
            }
            Ok(Some(floats))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_vector_string_accepts_all_forms() {
        assert_eq!(
            parse_vector_string("[0.1, 0.2, 0.3]"),
            Some(vec![0.1, 0.2, 0.3])
        );
        assert_eq!(
            parse_vector_string("ARRAY[0.1, 0.2, 0.3]"),
            Some(vec![0.1, 0.2, 0.3])
        );
        assert_eq!(
            parse_vector_string("array[1,2,3]"),
            Some(vec![1.0, 2.0, 3.0])
        );
        assert_eq!(
            parse_vector_string("ArrayLiteral([Literal(Float(0.9)), Literal(Float(0.1))])"),
            Some(vec![0.9, 0.1])
        );
        assert_eq!(
            parse_vector_string("[0.1, 0.2, 0.3] extra"),
            None,
            "trailing text must be refused"
        );
        assert_eq!(parse_vector_string("not-json"), None);
        assert_eq!(
            parse_vector_string("7"),
            None,
            "bare number must be refused"
        );
        assert_eq!(parse_vector_string("[nan]"), None, "NaN must be refused");
        assert_eq!(parse_vector_string("[inf]"), None, "inf must be refused");
        assert_eq!(parse_vector_string("[0.1, \"bad\"]"), None);
        assert_eq!(parse_vector_string("{}"), None);
        assert_eq!(
            parse_vector_string("[1 2 3]"),
            None,
            "whitespace separators are not part of the grammar"
        );
        assert_eq!(
            parse_vector_string("[0.1, 0.2 0.3]"),
            None,
            "whitespace-separated elements are refused wholesale"
        );
    }

    #[test]
    fn floats_from_value_accepts_arrays_and_json_strings() {
        use nodedb_types::Value::*;
        assert_eq!(
            floats_from_value(
                "c",
                "embedding",
                &Array(vec![Float(0.1), Integer(2), Float(3.0)])
            )
            .expect("valid array"),
            Some(vec![0.1, 2.0, 3.0])
        );
        assert_eq!(
            floats_from_value("c", "embedding", &String("[0.1, 0.2, 0.3]".to_string()))
                .expect("valid json"),
            Some(vec![0.1, 0.2, 0.3])
        );
        assert_eq!(
            floats_from_value("c", "embedding", &String("ARRAY[0.1, 0.2]".to_string()))
                .expect("valid sql"),
            Some(vec![0.1, 0.2])
        );
        assert_eq!(
            floats_from_value("c", "embedding", &Integer(7)).expect("not a vector"),
            None
        );
        assert_eq!(
            floats_from_value("c", "embedding", &Null).expect("not a vector"),
            None
        );
    }

    #[test]
    fn floats_from_value_rejects_malformed() {
        use nodedb_types::Value::*;
        for bad in ["not-json", "7", "[0.1, \"bad\"]", "{}", "[nan]", "[1 2 3]"] {
            let res = floats_from_value("c", "embedding", &String(bad.to_string()));
            assert!(
                matches!(res, Err(crate::Error::RejectedConstraint { .. })),
                "String({bad:?}) must be rejected, got {res:?}"
            );
        }
        let res = floats_from_value(
            "c",
            "embedding",
            &Array(vec![Float(0.1), String("bad".to_string())]),
        );
        assert!(
            matches!(res, Err(crate::Error::RejectedConstraint { .. })),
            "array with a non-numeric element must be rejected"
        );
    }

    #[test]
    fn floats_from_value_rejects_empty() {
        use nodedb_types::Value::*;
        for empty in ["", "[]", "[,,]", "[ ]"] {
            let res = floats_from_value("c", "embedding", &String(empty.to_string()));
            assert!(
                matches!(res, Err(crate::Error::RejectedConstraint { .. })),
                "String({empty:?}) must be rejected as an empty vector, got {res:?}"
            );
        }
        let res = floats_from_value("c", "embedding", &Array(vec![]));
        assert!(
            matches!(res, Err(crate::Error::RejectedConstraint { .. })),
            "an empty array must be rejected as an empty vector"
        );
    }

    #[test]
    fn errors_name_collection_and_field() {
        let res = floats_from_value(
            "docs",
            "embedding",
            &nodedb_types::Value::String("not-json".to_string()),
        );
        match res {
            Err(crate::Error::RejectedConstraint {
                collection,
                constraint,
                detail,
            }) => {
                assert_eq!(collection, "docs", "error must name the collection");
                assert!(
                    constraint.contains("embedding"),
                    "error must name the field, got {constraint:?}"
                );
                assert!(!detail.is_empty(), "error must carry the offending input");
            }
            other => panic!("expected RejectedConstraint, got {other:?}"),
        }
    }
}
