// SPDX-License-Identifier: Apache-2.0

//! Convert [`loro::LoroValue`] into [`nodedb_types::Value`].
//!
//! Used by CHECK-constraint evaluation to hand a row (built from a
//! [`crate::validator::ProposedChange`]'s `LoroValue` fields) to the shared
//! `nodedb_query` expression evaluator, which operates on `nodedb_types::Value`.

use std::collections::HashMap;

use loro::LoroValue;
use nodedb_types::Value;

/// Convert a `LoroValue` scalar/collection into the shared `Value` type.
///
/// `Container` variants are nested CRDT references (Text/List/Map containers),
/// not scalar row data — they convert to `Value::Null` rather than panicking,
/// since a CHECK predicate has no meaningful way to evaluate a container
/// reference.
pub(crate) fn loro_to_value(v: &LoroValue) -> Value {
    match v {
        LoroValue::Null => Value::Null,
        LoroValue::Bool(b) => Value::Bool(*b),
        LoroValue::I64(n) => Value::Integer(*n),
        LoroValue::Double(f) => Value::Float(*f),
        LoroValue::String(s) => Value::String(s.to_string()),
        LoroValue::Binary(b) => Value::Bytes(b.to_vec()),
        LoroValue::List(items) => Value::Array(items.iter().map(loro_to_value).collect()),
        LoroValue::Map(map) => {
            let mut out = HashMap::with_capacity(map.len());
            for (k, val) in map.iter() {
                out.insert(k.clone(), loro_to_value(val));
            }
            Value::Object(out)
        }
        LoroValue::Container(_) => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loro::LoroValue;

    #[test]
    fn scalars_round_trip() {
        assert_eq!(loro_to_value(&LoroValue::Null), Value::Null);
        assert_eq!(loro_to_value(&LoroValue::Bool(true)), Value::Bool(true));
        assert_eq!(loro_to_value(&LoroValue::I64(42)), Value::Integer(42));
        assert_eq!(loro_to_value(&LoroValue::Double(1.5)), Value::Float(1.5));
        assert_eq!(
            loro_to_value(&LoroValue::String("hi".into())),
            Value::String("hi".to_string())
        );
    }

    #[test]
    fn list_converts_recursively() {
        let list = LoroValue::List(
            vec![LoroValue::I64(1), LoroValue::I64(2)]
                .into_iter()
                .collect::<Vec<_>>()
                .into(),
        );
        let got = loro_to_value(&list);
        assert_eq!(
            got,
            Value::Array(vec![Value::Integer(1), Value::Integer(2)])
        );
    }

    #[test]
    fn map_converts_recursively() {
        let mut map = LoroValue::Map(Default::default());
        if let LoroValue::Map(m) = &mut map {
            m.make_mut().insert("age".to_string(), LoroValue::I64(30));
        }
        let got = loro_to_value(&map);
        match got {
            Value::Object(obj) => {
                assert_eq!(obj.get("age"), Some(&Value::Integer(30)));
            }
            other => panic!("expected Value::Object, got {other:?}"),
        }
    }
}
