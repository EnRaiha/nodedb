// SPDX-License-Identifier: BUSL-1.1

//! Document format conversion between JSON and MessagePack.
//!
//! Documents enter as JSON, are stored as MessagePack, and read back as
//! `serde_json::Value` regardless of storage format. Both may coexist in the
//! same redb table — format is detected from the first byte (MessagePack map
//! headers are 0x80-0x8F/0xDE/0xDF; JSON objects start with `{`).

use sonic_rs;

/// Build the typed error a document decode failure surfaces as.
fn decode_err(format: &str, detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: format.to_string(),
        detail: detail.to_string(),
    }
}

/// True when the first byte marks a standard MessagePack map header.
fn looks_like_msgpack_map(first: u8) -> bool {
    (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF
}

/// Convert a document byte blob to `serde_json::Value`. Auto-detects
/// MessagePack or JSON; a stray suffix or two concatenated documents fail
/// rather than silently decode the leading value. Binary Tuple is NOT
/// auto-detected — strict collections must use `binary_tuple_to_json()`.
pub(super) fn decode_document(bytes: &[u8]) -> crate::Result<serde_json::Value> {
    if bytes.is_empty() {
        return Err(decode_err("document", "empty body"));
    }

    // Those header bytes cannot begin JSON, so a msgpack decode failure is
    // reported directly rather than falling back to a JSON re-guess.
    if looks_like_msgpack_map(bytes[0]) {
        return nodedb_types::json_from_msgpack(bytes).map_err(|e| decode_err("msgpack", e));
    }

    sonic_rs::from_slice(bytes).map_err(|e| decode_err("json", e))
}

/// Decode a stored row that may be schemaless (MessagePack/JSON) or a strict
/// collection's Binary Tuple, dispatching on whether a schema was given.
/// `context` names the caller in the error message.
pub(super) fn decode_document_or_binary_tuple(
    bytes: &[u8],
    strict_schema: Option<&nodedb_types::columnar::StrictSchema>,
    context: &str,
) -> crate::Result<serde_json::Value> {
    match strict_schema {
        Some(schema) => crate::data::executor::strict_format::binary_tuple_to_json(bytes, schema)
            .ok_or_else(|| crate::Error::Serialization {
                format: "binary_tuple".to_string(),
                detail: format!(
                    "{context} ({} bytes) is not a Binary Tuple readable under the \
                         collection's strict schema",
                    bytes.len()
                ),
            }),
        None => decode_document(bytes),
    }
}

/// Convert a document byte blob to `nodedb_types::Value`, preserving native
/// types lost when decoding to `serde_json::Value`. Same detection and
/// full-consumption rules as [`decode_document`]; strict collections should
/// use `strict_format::binary_tuple_to_value` instead.
pub(super) fn decode_document_value(bytes: &[u8]) -> crate::Result<nodedb_types::Value> {
    if bytes.is_empty() {
        return Err(decode_err("document", "empty body"));
    }

    if looks_like_msgpack_map(bytes[0]) {
        return nodedb_types::value_from_msgpack(bytes).map_err(|e| decode_err("msgpack", e));
    }

    // JSON input boundary: parse then convert.
    let json: serde_json::Value = sonic_rs::from_slice(bytes).map_err(|e| decode_err("json", e))?;
    Ok(nodedb_types::Value::from(json))
}

/// Encode a JSON value as MessagePack bytes; falls back to JSON bytes if
/// encoding fails.
pub(super) fn encode_to_msgpack(value: &serde_json::Value) -> Vec<u8> {
    nodedb_types::json_to_msgpack(value).unwrap_or_else(|_| {
        // Fallback: store as JSON if MessagePack encoding fails.
        sonic_rs::to_vec(value).unwrap_or_default()
    })
}

/// Encode a resolved document (MERGE / `UPDATE ... FROM`) to the standard
/// schemaless wire body — the same `nodedb_types::Value` msgpack encoding
/// plain `INSERT` produces. `json_to_msgpack` instead yields a JSON-flavoured
/// map the vector-search flattener can't decode, so a resolved row's
/// non-key fields would come back empty from a vector search.
pub(super) fn encode_resolved_wire_body(doc: &serde_json::Value) -> Vec<u8> {
    let value: nodedb_types::Value = doc.clone().into();
    nodedb_types::value_to_msgpack(&value).unwrap_or_else(|_| encode_to_msgpack(doc))
}

/// Convert JSON bytes to MessagePack bytes, borrowing when nothing changes.
/// A standard msgpack map or unknown bytes pass through borrowed; JSON is
/// parsed and re-encoded (owned). Borrowing the common case is what keeps a
/// whole-scan normalization pass from `memcpy`-ing every document body.
pub(super) fn json_to_msgpack_cow(bytes: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    use std::borrow::Cow;

    if bytes.is_empty() {
        return Cow::Borrowed(bytes);
    }

    // Already a standard MessagePack map? Pass through untouched.
    let first = bytes[0];
    if (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF {
        return Cow::Borrowed(bytes);
    }

    // Try parsing as JSON and converting to MessagePack.
    match sonic_rs::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => Cow::Owned(encode_to_msgpack(&value)),
        Err(_) => Cow::Borrowed(bytes),
    }
}

/// Owning form of [`json_to_msgpack_cow`], for callers that must keep the
/// result past the input's lifetime or hand it on as a `Vec`.
pub(super) fn json_to_msgpack(bytes: &[u8]) -> Vec<u8> {
    json_to_msgpack_cow(bytes).into_owned()
}

/// Convert a vector-primary metadata sidecar body (tagged msgpack, each value
/// a `[tag, payload]` array) to a standard MessagePack map. A document
/// decoder would return the tag arrays verbatim since the outer container
/// still passes every "is this already msgpack?" guard — the caller must
/// route by collection kind, never by inspecting bytes. Non-tagged bytes
/// pass through unchanged (borrowed); only the real transcode allocates.
pub(super) fn vector_sidecar_to_msgpack_cow(bytes: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    use std::borrow::Cow;

    if bytes.is_empty() {
        return Cow::Borrowed(bytes);
    }
    match zerompk::from_msgpack::<std::collections::HashMap<String, nodedb_types::Value>>(bytes) {
        Ok(map) => {
            let json: serde_json::Value = nodedb_types::Value::Object(map).into();
            Cow::Owned(encode_to_msgpack(&json))
        }
        Err(_) => Cow::Borrowed(bytes),
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

    /// The sidecar normalizer must turn tagged values into real values;
    /// `json_to_msgpack` alone returns them untouched (tag arrays leaking out).
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

        let normalized = vector_sidecar_to_msgpack_cow(&tagged);
        let doc = decode_document(&normalized).expect("sidecar must decode as msgpack");
        assert_eq!(doc.get("owner").and_then(|v| v.as_str()), Some("alice"));
        assert_eq!(doc.get("id").and_then(|v| v.as_str()), Some("r1"));
    }

    #[test]
    fn a_non_sidecar_body_is_returned_unchanged() {
        let value = serde_json::json!({"a": 1});
        let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
        assert_eq!(vector_sidecar_to_msgpack_cow(&msgpack), msgpack);
        assert!(vector_sidecar_to_msgpack_cow(b"").is_empty());
    }

    #[test]
    fn empty_bytes_handled() {
        assert!(decode_document(b"").is_err());
        assert!(decode_document_value(b"").is_err());
        assert!(json_to_msgpack(b"").is_empty());
    }

    /// A body holds exactly one top-level value. Anything after it means the
    /// slot does not hold the document it claims to, so decoding the leading
    /// value and dropping the rest would report success on corrupt bytes.
    #[test]
    fn a_body_with_an_appended_byte_is_rejected() {
        let value = serde_json::json!({"a": 1, "b": "two"});
        let mut msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
        assert!(
            decode_document(&msgpack).is_ok(),
            "baseline body must decode"
        );

        msgpack.push(0xC0);
        assert!(
            decode_document(&msgpack).is_err(),
            "trailing byte must be a decode failure, not a silent prefix decode"
        );
        assert!(decode_document_value(&msgpack).is_err());
    }

    #[test]
    fn two_concatenated_bodies_are_rejected() {
        let first = nodedb_types::json_to_msgpack(&serde_json::json!({"a": 1})).unwrap();
        let second = nodedb_types::json_to_msgpack(&serde_json::json!({"b": 2})).unwrap();
        let mut joined = first.clone();
        joined.extend_from_slice(&second);

        assert!(decode_document(&first).is_ok());
        assert!(
            decode_document(&joined).is_err(),
            "two documents in one slot must not decode as the first"
        );
        assert!(decode_document_value(&joined).is_err());
    }

    #[test]
    fn a_json_body_with_a_stray_suffix_is_rejected() {
        assert!(decode_document(b"{\"x\":1}").is_ok());
        assert!(decode_document(b"{\"x\":1}{\"y\":2}").is_err());
        assert!(decode_document(b"{\"x\":1}x").is_err());
    }
}
