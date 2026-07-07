// SPDX-License-Identifier: Apache-2.0

//! Binary group key construction for GROUP BY on raw MessagePack documents.
//!
//! Builds a deterministic string key from field values extracted directly
//! from msgpack bytes, avoiding full document decode.

use crate::expr::GroupKeySpec;
use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::index::FieldIndex;
use crate::msgpack_scan::reader::{read_f64, read_i64, read_null, read_str};

/// Build a GROUP BY key string from raw msgpack bytes.
///
/// Format: `[val1,val2,...]` where values are formatted as JSON literals.
/// This is compatible with the legacy `sonic_rs::to_string(&key_parts)` format,
/// so the result-construction code can parse it back with `sonic_rs::from_str`.
///
/// Each key contributes one positional slot, extracted from the document field
/// named by its `field`. A key with no `field` (a computed expression, not yet
/// evaluated here) contributes no slot at all.
pub fn build_group_key(doc: &[u8], group_keys: &[GroupKeySpec]) -> String {
    if group_keys.is_empty() {
        return "__all__".to_string();
    }

    let mut key_buf = String::new();
    key_buf.push('[');
    let mut written = 0usize;
    for spec in group_keys {
        let Some(field) = spec.field.as_deref() else {
            continue;
        };
        if written > 0 {
            key_buf.push(',');
        }
        append_field_value(&mut key_buf, doc, field);
        written += 1;
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
        let Some(field) = spec.field.as_deref() else {
            continue;
        };
        if written > 0 {
            key_buf.push(',');
        }
        let range = idx.get(field);
        append_field_value_range(&mut key_buf, doc, range);
        written += 1;
    }
    key_buf.push(']');
    key_buf
}

/// Append a single field's value to the key buffer as a JSON literal.
fn append_field_value(buf: &mut String, doc: &[u8], field: &str) {
    let Some((start, end)) = extract_field(doc, 0, field) else {
        buf.push_str("null");
        return;
    };

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

/// Append a field value from a pre-resolved range.
fn append_field_value_range(buf: &mut String, doc: &[u8], range: Option<(usize, usize)>) {
    let Some((start, end)) = range else {
        buf.push_str("null");
        return;
    };

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
        let bytes = &doc[start..end];
        for b in bytes {
            use std::fmt::Write;
            let _ = write!(buf, "{b:02x}");
        }
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
}
