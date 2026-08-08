// SPDX-License-Identifier: BUSL-1.1

//! Document format conversion between JSON and MessagePack.
//!
//! Documents enter the system as JSON (from SQL INSERT via DataFusion).
//! They are stored in redb as MessagePack (compact binary, faster to
//! deserialize, supports targeted field extraction).
//!
//! On read, documents are returned as `serde_json::Value` regardless of
//! storage format. During migration, both JSON and MessagePack blobs may
//! coexist in the same redb table — format is detected by inspecting the
//! first byte (MessagePack maps start with 0x80-0x8F for fixmap, 0xDE for
//! map16, 0xDF for map32; JSON objects start with `{` = 0x7B).

use sonic_rs;

/// Convert a document byte blob to `serde_json::Value`.
///
/// Auto-detects the format: MessagePack, JSON, or Binary Tuple.
/// Binary Tuple detection requires knowing the schema — if the bytes
/// don't match MessagePack or JSON, returns `None` (the caller should
/// use `strict_format::binary_tuple_to_json` with the schema if the
/// collection is known to be strict).
pub(super) fn decode_document(bytes: &[u8]) -> Option<serde_json::Value> {
    if bytes.is_empty() {
        return None;
    }

    // Detect MessagePack: maps start with 0x80-0x8F (fixmap), 0xDE (map16), 0xDF (map32).
    let first = bytes[0];
    if (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF {
        // Try MessagePack first.
        if let Ok(val) = nodedb_types::json_from_msgpack(bytes) {
            return Some(val);
        }
    }

    // Fall back to JSON.
    sonic_rs::from_slice(bytes).ok()

    // Note: Binary Tuple bytes are NOT auto-detected here because decoding
    // requires the schema. For strict collections, callers must check
    // doc_configs.storage_mode and use strict_format::binary_tuple_to_json().
}

/// Convert a document byte blob to `nodedb_types::Value`.
///
/// Preserves all native types (Geometry, DateTime, Decimal, etc.) that
/// would be lost when decoding to `serde_json::Value`.
/// Auto-detects msgpack vs JSON. Binary Tuple requires schema — callers
/// should use `strict_format::binary_tuple_to_value` for strict collections.
pub(super) fn decode_document_value(bytes: &[u8]) -> Option<nodedb_types::Value> {
    if bytes.is_empty() {
        return None;
    }

    let first = bytes[0];
    if ((0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF)
        && let Ok(val) = nodedb_types::value_from_msgpack(bytes)
    {
        return Some(val);
    }

    // JSON input boundary: parse then convert.
    let json: serde_json::Value = sonic_rs::from_slice(bytes).ok()?;
    Some(nodedb_types::Value::from(json))
}

/// Encode a JSON value as MessagePack bytes for storage.
///
/// If encoding fails (should not happen for valid `serde_json::Value`),
/// falls back to JSON bytes.
pub(super) fn encode_to_msgpack(value: &serde_json::Value) -> Vec<u8> {
    nodedb_types::json_to_msgpack(value).unwrap_or_else(|_| {
        // Fallback: store as JSON if MessagePack encoding fails.
        sonic_rs::to_vec(value).unwrap_or_default()
    })
}

/// Convert JSON bytes to MessagePack bytes.
///
/// If the input is already MessagePack, returns it unchanged.
///
/// Handles three input formats:
/// - Standard msgpack map (0x80–0x8F / 0xDE / 0xDF): returned as-is.
/// - JSON bytes: parsed and re-encoded as standard msgpack map.
/// - Unknown bytes: returned as-is.
pub(super) fn json_to_msgpack(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return bytes.to_vec();
    }

    // Already a standard MessagePack map? Return as-is.
    let first = bytes[0];
    if (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF {
        return bytes.to_vec();
    }

    // Try parsing as JSON and converting to MessagePack.
    match sonic_rs::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => encode_to_msgpack(&value),
        Err(_) => bytes.to_vec(),
    }
}

/// Convert a vector-primary metadata sidecar body to a standard MessagePack map.
///
/// A sidecar is `zerompk::to_msgpack_vec(&HashMap<String, nodedb_types::Value>)`
/// — the TAGGED form, where each value is a `[tag, payload]` array
/// (`Value::String("r1")` → `[4,"r1"]`) — stored verbatim by the vector upsert
/// handler. Handing those bytes to a document decoder yields tag arrays where
/// the client expects values, because the outer container is an ordinary
/// MessagePack map and passes every "is this already msgpack?" guard.
///
/// This is why the choice is never made by inspecting bytes: it is made by the
/// caller, from the collection's registered kind, and lands here.
///
/// Bytes that do not decode as a tagged map are returned unchanged rather than
/// re-guessed — a sidecar that cannot be read as one is not something a second
/// format guess can rescue.
pub(super) fn vector_sidecar_to_msgpack(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return bytes.to_vec();
    }
    match zerompk::from_msgpack::<std::collections::HashMap<String, nodedb_types::Value>>(bytes) {
        Ok(map) => {
            let json: serde_json::Value = nodedb_types::Value::Object(map).into();
            encode_to_msgpack(&json)
        }
        Err(_) => bytes.to_vec(),
    }
}

fn is_standard_msgpack_map(bytes: &[u8]) -> bool {
    let first = bytes[0];
    ((0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF)
        && nodedb_query::msgpack_scan::map_header(bytes, 0).is_some()
}

/// Canonicalize a schemaless document for storage as a top-level standard msgpack map.
///
/// This is the write-path invariant for schemaless collections. Scans should not
/// rely on this helper for repair; new writes must already be canonical.
pub(super) fn canonicalize_document_for_storage(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return bytes.to_vec();
    }

    if is_standard_msgpack_map(bytes) {
        return bytes.to_vec();
    }

    if let Ok(val @ nodedb_types::Value::Object(_)) =
        zerompk::from_msgpack::<nodedb_types::Value>(bytes)
    {
        let json: serde_json::Value = val.into();
        let mp = encode_to_msgpack(&json);
        if is_standard_msgpack_map(&mp) {
            return mp;
        }
    }

    match sonic_rs::from_slice::<serde_json::Value>(bytes) {
        Ok(value) if value.is_object() => {
            let mp = encode_to_msgpack(&value);
            if is_standard_msgpack_map(&mp) {
                return mp;
            }
            bytes.to_vec()
        }
        _ => bytes.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_roundtrip_through_msgpack() {
        let original = serde_json::json!({"name": "alice", "age": 30, "tags": ["ml", "rust"]});
        let json_bytes = serde_json::to_vec(&original).unwrap();

        // Convert JSON → MessagePack.
        let msgpack_bytes = json_to_msgpack(&json_bytes);
        assert_ne!(
            json_bytes, msgpack_bytes,
            "should convert to different format"
        );

        // Decode from MessagePack.
        let decoded = decode_document(&msgpack_bytes).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn json_input_detected_correctly() {
        let json_bytes = b"{\"x\":1}";
        let decoded = decode_document(json_bytes).unwrap();
        assert_eq!(decoded["x"], 1);
    }

    #[test]
    fn msgpack_input_detected_correctly() {
        let value = serde_json::json!({"key": "value"});
        let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
        let decoded = decode_document(&msgpack).unwrap();
        assert_eq!(decoded["key"], "value");
    }

    #[test]
    fn already_msgpack_unchanged() {
        let value = serde_json::json!({"a": 1});
        let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
        let result = json_to_msgpack(&msgpack);
        assert_eq!(result, msgpack, "msgpack should pass through unchanged");
    }

    #[test]
    fn noncanonical_msgpack_is_not_rewritten_on_read_path() {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "user_id".to_string(),
            nodedb_types::Value::String("u1".into()),
        );
        let tagged = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap();

        let result = json_to_msgpack(&tagged);
        assert_eq!(result, tagged);
    }

    #[test]
    fn tagged_object_msgpack_is_canonicalized_to_standard_map_for_storage() {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "user_id".to_string(),
            nodedb_types::Value::String("u1".into()),
        );
        obj.insert(
            "item".to_string(),
            nodedb_types::Value::String("book".into()),
        );
        let tagged = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap();

        let canonical = canonicalize_document_for_storage(&tagged);
        assert!(
            nodedb_query::msgpack_scan::map_header(&canonical, 0).is_some(),
            "expected standard msgpack map"
        );
        assert!(nodedb_query::msgpack_scan::extract_field(&canonical, 0, "user_id").is_some());
    }

    /// The sidecar normalizer must turn tagged values into real values.
    ///
    /// Passing the same bytes through `json_to_msgpack` returns them untouched
    /// (its guard only reads the outer map header), which is exactly how
    /// `[4,"alice"]` reached clients as a payload column value.
    #[test]
    fn a_tagged_sidecar_decodes_to_values_not_tag_arrays() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("id".to_string(), nodedb_types::Value::String("r1".into()));
        obj.insert(
            "owner".to_string(),
            nodedb_types::Value::String("alice".into()),
        );
        let tagged = zerompk::to_msgpack_vec(&obj).unwrap();

        assert_eq!(
            json_to_msgpack(&tagged),
            tagged,
            "the document normalizer must NOT be the thing that fixes this"
        );

        let normalized = vector_sidecar_to_msgpack(&tagged);
        let doc = decode_document(&normalized).expect("sidecar must decode as msgpack");
        assert_eq!(doc.get("owner").and_then(|v| v.as_str()), Some("alice"));
        assert_eq!(doc.get("id").and_then(|v| v.as_str()), Some("r1"));
    }

    #[test]
    fn a_non_sidecar_body_is_returned_unchanged() {
        let value = serde_json::json!({"a": 1});
        let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
        assert_eq!(vector_sidecar_to_msgpack(&msgpack), msgpack);
        assert!(vector_sidecar_to_msgpack(b"").is_empty());
    }

    #[test]
    fn empty_bytes_handled() {
        assert!(decode_document(b"").is_none());
        assert!(json_to_msgpack(b"").is_empty());
    }
}
