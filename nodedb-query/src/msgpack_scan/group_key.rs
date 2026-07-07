// SPDX-License-Identifier: Apache-2.0

//! Binary group key construction for GROUP BY on raw MessagePack documents.
//!
//! Builds a deterministic string key from field values extracted directly
//! from msgpack bytes, avoiding full document decode.

use crate::expr::{GroupKeySpec, SqlExpr};
use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::index::FieldIndex;
use crate::msgpack_scan::reader::{read_f64, read_i64, read_null, read_str};

/// Build a GROUP BY key string from raw msgpack bytes.
///
/// Format: `[val1,val2,...]` where values are formatted as JSON literals.
/// This is compatible with the legacy `sonic_rs::to_string(&key_parts)` format,
/// so the result-construction code can parse it back with `sonic_rs::from_str`.
///
/// Each key contributes one positional slot: a bare-column key (`field: Some`)
/// contributes the value extracted from that document field; a computed key
/// (`field: None, expr: Some`) contributes the value of the expression
/// evaluated against the whole document. Evaluation is deterministic on the
/// document bytes, so the same spec on the same bytes yields a byte-identical
/// slot on producer and consumer. A spec with neither `field` nor `expr`
/// contributes no slot.
pub fn build_group_key(doc: &[u8], group_keys: &[GroupKeySpec]) -> String {
    if group_keys.is_empty() {
        return "__all__".to_string();
    }

    let mut key_buf = String::new();
    key_buf.push('[');
    let mut written = 0usize;
    for spec in group_keys {
        if let Some(field) = spec.field.as_deref() {
            if written > 0 {
                key_buf.push(',');
            }
            append_field_value(&mut key_buf, doc, field);
            written += 1;
        } else if let Some(expr) = spec.expr.as_ref() {
            if written > 0 {
                key_buf.push(',');
            }
            append_computed_value(&mut key_buf, doc, expr);
            written += 1;
        }
    }
    key_buf.push(']');
    key_buf
}

/// Build a GROUP BY key using a pre-built `FieldIndex` for O(1) lookups.
pub fn build_group_key_indexed(
    doc: &[u8],
    group_keys: &[GroupKeySpec],
    idx: &FieldIndex,
) -> String {
    if group_keys.is_empty() {
        return "__all__".to_string();
    }

    let mut key_buf = String::new();
    key_buf.push('[');
    let mut written = 0usize;
    for spec in group_keys {
        if let Some(field) = spec.field.as_deref() {
            if written > 0 {
                key_buf.push(',');
            }
            let range = idx.get(field);
            append_field_value_range(&mut key_buf, doc, range);
            written += 1;
        } else if let Some(expr) = spec.expr.as_ref() {
            // A computed key evaluates against the whole document, not a single
            // indexed field, so the field index cannot short-circuit it.
            if written > 0 {
                key_buf.push(',');
            }
            append_computed_value(&mut key_buf, doc, expr);
            written += 1;
        }
    }
    key_buf.push(']');
    key_buf
}

/// Append the msgpack value at `doc[start..end]` to the key buffer as a JSON
/// literal (string quoted, numbers/null verbatim, complex values hex-encoded).
fn append_value_at(buf: &mut String, doc: &[u8], start: usize, end: usize) {
    if read_null(doc, start) {
        buf.push_str("null");
    } else if let Some(s) = read_str(doc, start) {
        buf.push('"');
        buf.push_str(s);
        buf.push('"');
    } else if let Some(n) = read_i64(doc, start) {
        use std::fmt::Write;
        let _ = write!(buf, "{n}");
    } else if let Some(n) = read_f64(doc, start) {
        use std::fmt::Write;
        let _ = write!(buf, "{n}");
    } else {
        // Complex value (array/map/bin) — hex-encode raw bytes as key.
        let bytes = &doc[start..end];
        for b in bytes {
            use std::fmt::Write;
            let _ = write!(buf, "{b:02x}");
        }
    }
}

/// Append a single field's value to the key buffer as a JSON literal.
fn append_field_value(buf: &mut String, doc: &[u8], field: &str) {
    match extract_field(doc, 0, field) {
        Some((start, end)) => append_value_at(buf, doc, start, end),
        None => buf.push_str("null"),
    }
}

/// Append a field value from a pre-resolved range.
fn append_field_value_range(buf: &mut String, doc: &[u8], range: Option<(usize, usize)>) {
    match range {
        Some((start, end)) => append_value_at(buf, doc, start, end),
        None => buf.push_str("null"),
    }
}

/// Append a computed key's value: evaluate `expr` against the whole document
/// and encode the result the same way a field value is encoded, so a computed
/// slot is byte-identical wherever the same expression meets the same document.
/// A document that cannot be decoded, or a value that cannot be re-encoded,
/// contributes `null` — mirroring the missing-field handling above.
fn append_computed_value(buf: &mut String, doc: &[u8], expr: &SqlExpr) {
    let Ok(doc_val) = nodedb_types::json_msgpack::value_from_msgpack(doc) else {
        buf.push_str("null");
        return;
    };
    let val = expr.eval(&doc_val);
    match nodedb_types::json_msgpack::value_to_msgpack(&val) {
        Ok(vb) => append_value_at(buf, &vb, 0, vb.len()),
        Err(_) => buf.push_str("null"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    fn keys(fields: &[&str]) -> Vec<GroupKeySpec> {
        fields.iter().map(|f| GroupKeySpec::column(*f)).collect()
    }

    #[test]
    fn single_string_field() {
        let doc = encode(&json!({"name": "alice", "age": 30}));
        let key = build_group_key(&doc, &keys(&["name"]));
        assert_eq!(key, r#"["alice"]"#);
    }

    #[test]
    fn single_int_field() {
        let doc = encode(&json!({"status": 200}));
        let key = build_group_key(&doc, &keys(&["status"]));
        assert_eq!(key, "[200]");
    }

    #[test]
    fn multiple_fields() {
        let doc = encode(&json!({"city": "ny", "year": 2024}));
        let key = build_group_key(&doc, &keys(&["city", "year"]));
        assert_eq!(key, r#"["ny",2024]"#);
    }

    #[test]
    fn missing_field_is_null() {
        let doc = encode(&json!({"x": 1}));
        let key = build_group_key(&doc, &keys(&["missing"]));
        assert_eq!(key, "[null]");
    }

    #[test]
    fn empty_group_fields() {
        let doc = encode(&json!({"x": 1}));
        let key = build_group_key(&doc, &[]);
        assert_eq!(key, "__all__");
    }

    #[test]
    fn null_field_value() {
        let doc = encode(&json!({"v": null}));
        let key = build_group_key(&doc, &keys(&["v"]));
        assert_eq!(key, "[null]");
    }

    #[test]
    fn float_field() {
        let doc = encode(&json!({"temp": 36.6}));
        let key = build_group_key(&doc, &keys(&["temp"]));
        assert_eq!(key, "[36.6]");
    }

    /// A computed group key evaluates its expression and folds the result into
    /// the key slot; `alpha` and `ALPHA` collapse under `UPPER(label)`.
    fn upper_label_spec() -> GroupKeySpec {
        GroupKeySpec {
            output_name: "u".to_string(),
            field: None,
            expr: Some(SqlExpr::Function {
                name: "upper".to_string(),
                args: vec![SqlExpr::Column("label".to_string())],
            }),
        }
    }

    #[test]
    fn computed_key_folds_expression_value() {
        let lower = encode(&json!({"label": "alpha", "score": 7}));
        let upper = encode(&json!({"label": "ALPHA", "score": 3}));
        assert_eq!(
            build_group_key(&lower, &[upper_label_spec()]),
            r#"["ALPHA"]"#
        );
        assert_eq!(
            build_group_key(&upper, &[upper_label_spec()]),
            r#"["ALPHA"]"#
        );
    }

    #[test]
    fn computed_key_indexed_matches_unindexed() {
        let doc = encode(&json!({"label": "beta", "score": 5}));
        let specs = [upper_label_spec()];
        let idx = FieldIndex::build(&doc, 0).unwrap_or_else(FieldIndex::empty);
        assert_eq!(
            build_group_key(&doc, &specs),
            build_group_key_indexed(&doc, &specs, &idx),
        );
    }

    #[test]
    fn mixed_column_and_computed_keys() {
        let doc = encode(&json!({"region": "us", "label": "west"}));
        let specs = [GroupKeySpec::column("region"), upper_label_spec()];
        assert_eq!(build_group_key(&doc, &specs), r#"["us","WEST"]"#);
    }
}
