// SPDX-License-Identifier: BUSL-1.1

//! Pure value computation for `FieldSet` (HSET-style field merge), shared by
//! the autocommit handler (`field.rs`) and the in-transaction staging
//! handler (`stage_kv_transfer.rs`), so a staged value and its COMMIT-time
//! durable replay are always computed by the exact same code — mirrors the
//! `engine_atomic_compute` / `stage_kv_atomic` split for `Incr`/`Cas`/etc.

use crate::bridge::envelope::ErrorCode;

/// Result of merging field updates into a KV document body.
pub(in crate::data::executor) struct FieldSetComputation {
    /// The re-encoded (plain msgpack, not zerompk-tagged) document body.
    pub new_value: Vec<u8>,
    /// Count of updated fields that did not previously exist on the document.
    pub fields_added: u64,
}

/// Decode `current` (if any) as a msgpack object, merge `updates` into it,
/// and re-encode. Mirrors [`super::field::CoreLoop::execute_kv_field_set`]'s
/// prior inline logic exactly.
pub(in crate::data::executor) fn merge_field_updates(
    current: Option<&[u8]>,
    updates: &[(String, Vec<u8>)],
) -> Result<FieldSetComputation, ErrorCode> {
    let mut doc: serde_json::Map<String, serde_json::Value> = current
        .and_then(|v| nodedb_types::json_from_msgpack(v).ok())
        .and_then(|v| {
            if let serde_json::Value::Object(m) = v {
                Some(m)
            } else {
                None
            }
        })
        .unwrap_or_default();

    let mut fields_added = 0u64;
    for (field, value_bytes) in updates {
        let new_value = if value_bytes.is_empty() {
            serde_json::Value::Null
        } else {
            nodedb_types::json_from_msgpack(value_bytes).map_err(|e| ErrorCode::Internal {
                detail: format!("field set '{field}': msgpack decode: {e}"),
            })?
        };
        if !doc.contains_key(field) {
            fields_added += 1;
        }
        doc.insert(field.clone(), new_value);
    }

    let new_value =
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(doc)).map_err(|e| {
            ErrorCode::Internal {
                detail: format!("field set serialization: {e}"),
            }
        })?;

    Ok(FieldSetComputation {
        new_value,
        fields_added,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_into_empty_document() {
        let val = nodedb_types::json_to_msgpack(&serde_json::json!(42)).unwrap();
        let result = merge_field_updates(None, &[("score".to_string(), val)]).unwrap();
        assert_eq!(result.fields_added, 1);
        let doc: serde_json::Value = nodedb_types::json_from_msgpack(&result.new_value).unwrap();
        assert_eq!(doc["score"], serde_json::json!(42));
    }

    #[test]
    fn overwrite_does_not_count_as_added() {
        let existing = nodedb_types::json_to_msgpack(&serde_json::json!({"score": 1})).unwrap();
        let val = nodedb_types::json_to_msgpack(&serde_json::json!(2)).unwrap();
        let result = merge_field_updates(Some(&existing), &[("score".to_string(), val)]).unwrap();
        assert_eq!(result.fields_added, 0);
        let doc: serde_json::Value = nodedb_types::json_from_msgpack(&result.new_value).unwrap();
        assert_eq!(doc["score"], serde_json::json!(2));
    }

    #[test]
    fn empty_value_bytes_set_null() {
        let result = merge_field_updates(None, &[("f".to_string(), Vec::new())]).unwrap();
        let doc: serde_json::Value = nodedb_types::json_from_msgpack(&result.new_value).unwrap();
        assert!(doc["f"].is_null());
    }
}
