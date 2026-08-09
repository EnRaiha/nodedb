// SPDX-License-Identifier: Apache-2.0

//! Roundtrip tests for json_msgpack reader/writer.

use crate::json_msgpack::{
    MsgpackError, json_from_msgpack, json_to_msgpack, msgpack_to_json_string, value_from_msgpack,
    value_to_msgpack,
};
use serde_json::json;

#[test]
fn roundtrip_null() {
    let val = json!(null);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_bool() {
    for val in [json!(true), json!(false)] {
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }
}

#[test]
fn roundtrip_integers() {
    for val in [
        json!(0),
        json!(42),
        json!(-1),
        json!(i64::MAX),
        json!(i64::MIN),
    ] {
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }
}

#[test]
fn roundtrip_float() {
    let val = json!(9.81);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_string() {
    let val = json!("hello world");
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_array() {
    let val = json!([1, "two", true, null, 2.72]);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_nested_object() {
    let val = json!({"a": 1, "b": {"c": [2, 3]}, "d": null});
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_empty_map() {
    let val = json!({});
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_empty_array() {
    let val = json!([]);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_large_string() {
    let s = "x".repeat(300);
    let val = json!(s);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn native_value_roundtrip() {
    let mut map = std::collections::HashMap::new();
    map.insert("id".to_string(), crate::Value::String("host1".into()));
    map.insert("cpu".to_string(), crate::Value::Float(0.75));
    map.insert("mem".to_string(), crate::Value::Float(0.5));

    let row = crate::Value::Object(map);
    let arr = crate::Value::Array(vec![row]);

    let bytes = value_to_msgpack(&arr).unwrap();
    let decoded = value_from_msgpack(&bytes).unwrap();

    match &decoded {
        crate::Value::Array(items) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                crate::Value::Object(m) => {
                    assert_eq!(m.len(), 3);
                    assert_eq!(m.get("id"), Some(&crate::Value::String("host1".into())));
                    assert_eq!(m.get("cpu"), Some(&crate::Value::Float(0.75)));
                    assert_eq!(m.get("mem"), Some(&crate::Value::Float(0.5)));
                }
                other => panic!("expected Object, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

/// The readers decode exactly one top-level value. Bytes left over mean the
/// input is not the value it claims to be, so returning the leading value would
/// report success on corrupt input.
#[test]
fn trailing_byte_is_rejected() {
    let val = json!({"a": 1, "b": "two"});
    let mut bytes = json_to_msgpack(&val).unwrap();
    assert_eq!(json_from_msgpack(&bytes).unwrap(), val);

    bytes.push(0xC0);
    match json_from_msgpack(&bytes) {
        Err(MsgpackError::TrailingBytes { consumed, total }) => {
            assert_eq!(total, consumed + 1);
        }
        other => panic!("expected TrailingBytes, got {other:?}"),
    }
    assert!(value_from_msgpack(&bytes).is_err());
    assert!(msgpack_to_json_string(&bytes).is_err());
}

#[test]
fn two_concatenated_values_are_rejected() {
    let first = json_to_msgpack(&json!({"a": 1})).unwrap();
    let second = json_to_msgpack(&json!({"b": 2})).unwrap();
    let mut joined = first.clone();
    joined.extend_from_slice(&second);

    assert!(json_from_msgpack(&first).is_ok());
    assert!(json_from_msgpack(&joined).is_err());
    assert!(value_from_msgpack(&joined).is_err());
    assert!(msgpack_to_json_string(&joined).is_err());
}

#[test]
fn empty_input_is_unchanged() {
    // Readers still fail on empty input; the transcoder still yields "".
    assert!(json_from_msgpack(&[]).is_err());
    assert!(value_from_msgpack(&[]).is_err());
    assert_eq!(msgpack_to_json_string(&[]).unwrap(), "");
}

#[test]
fn native_value_scalars() {
    let cases: Vec<crate::Value> = vec![
        crate::Value::Null,
        crate::Value::Bool(true),
        crate::Value::Integer(42),
        crate::Value::Float(2.72),
        crate::Value::String("hello".into()),
    ];
    for val in cases {
        let bytes = value_to_msgpack(&val).unwrap();
        let decoded = value_from_msgpack(&bytes).unwrap();
        assert_eq!(val, decoded);
    }
}
