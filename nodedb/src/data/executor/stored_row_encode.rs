// SPDX-License-Identifier: BUSL-1.1

//! Encode a decoded row back into the bytes its collection actually stores.
//!
//! The inverse of [`crate::data::executor::handlers::document::read::decode`]:
//! that module answers "which decoder do these stored bytes need", this one
//! answers "which encoder does this collection's store expect", and both take
//! the answer as a
//! [`SparseBodyFormatRef`](crate::data::executor::sparse_body_format::SparseBodyFormatRef)
//! resolved once from `doc_configs`.
//!
//! Read-modify-write paths need both halves, and they must agree. A path that
//! reads with the resolved decoder but writes back with a hardcoded MessagePack
//! encoder replaces a Binary Tuple with a msgpack map — the row survives the
//! statement that rewrote it and becomes unreadable to every reader afterwards,
//! which is strictly worse than the read alone having been wrong. So the two
//! decisions live side by side and neither is made inline at a call site.

use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::data::executor::{doc_format, strict_format};

/// Encode `doc` in the encoding `format` names.
///
/// The exact inverse of `decode_scanned_row`, so a row that is decoded,
/// modified and re-encoded comes back in the encoding it was stored in.
pub(in crate::data::executor) fn encode_stored_row(
    doc: &serde_json::Value,
    format: SparseBodyFormatRef<'_>,
) -> crate::Result<Vec<u8>> {
    match format {
        SparseBodyFormatRef::Document => Ok(doc_format::encode_to_msgpack(doc)),
        SparseBodyFormatRef::Strict(schema) => {
            let value: nodedb_types::Value = doc.clone().into();
            strict_format::value_to_binary_tuple(&value, schema)
        }
        SparseBodyFormatRef::VectorSidecar => {
            // A sidecar is the TAGGED `zerompk` encoding of the metadata map,
            // not a standard msgpack map. Writing the standard form here would
            // pass every "is this msgpack?" guard and then read back as tag
            // arrays (`[4,"alice"]` where the client wrote `alice`).
            let nodedb_types::Value::Object(map) = nodedb_types::Value::from(doc.clone()) else {
                return Err(crate::Error::Serialization {
                    format: "vector_sidecar".to_string(),
                    detail: "a vector-primary metadata sidecar row must be an object".to_string(),
                });
            };
            zerompk::to_msgpack_vec(&map).map_err(|e| crate::Error::Serialization {
                format: "vector_sidecar".to_string(),
                detail: format!("re-encode vector-primary sidecar: {e}"),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
    use nodedb_types::Value;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    fn strict_schema() -> StrictSchema {
        StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::required("balance", ColumnType::String),
            ],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        }
    }

    /// Encode-then-decode must be the identity on the document's fields for
    /// every encoding, which is what a read-modify-write path relies on.
    #[test]
    fn a_round_trip_preserves_the_document_in_every_encoding() {
        let doc = serde_json::json!({"id": "a1", "balance": "150.25"});
        let schema = strict_schema();

        for format in [
            SparseBodyFormatRef::Document,
            SparseBodyFormatRef::Strict(&schema),
            SparseBodyFormatRef::VectorSidecar,
        ] {
            let bytes = encode_stored_row(&doc, format).expect("encode");
            let decoded = decode_scanned_document(&bytes, format).expect("decode");
            assert_eq!(
                decoded.get("balance").and_then(|v| v.as_str()),
                Some("150.25"),
                "round trip must preserve the modified column"
            );
            assert_eq!(decoded.get("id").and_then(|v| v.as_str()), Some("a1"));
        }
    }

    /// A strict row must come back as a Binary Tuple, not a msgpack map.
    ///
    /// This is the write half of the bug: the schemaless decoder cannot read a
    /// Binary Tuple, so a strict row rewritten as msgpack is unreadable to the
    /// strict decoder every reader after it will use.
    #[test]
    fn a_strict_row_re_encodes_as_a_binary_tuple_not_msgpack() {
        let doc = serde_json::json!({"id": "a1", "balance": "10"});
        let schema = strict_schema();

        let strict_bytes =
            encode_stored_row(&doc, SparseBodyFormatRef::Strict(&schema)).expect("encode strict");
        let msgpack_bytes =
            encode_stored_row(&doc, SparseBodyFormatRef::Document).expect("encode document");
        assert_ne!(
            strict_bytes, msgpack_bytes,
            "the strict encoding must not be the msgpack encoding"
        );
        assert!(
            strict_format::binary_tuple_to_json(&strict_bytes, &schema).is_some(),
            "the strict decoder must be able to read what the strict encoder wrote"
        );
        assert!(
            strict_format::binary_tuple_to_json(&msgpack_bytes, &schema).is_none(),
            "a msgpack body is NOT a readable Binary Tuple — this is what a \
             hardcoded msgpack write-back would leave behind"
        );
    }

    /// A sidecar must come back TAGGED, which the plain document decoder reads
    /// as tag arrays rather than values.
    #[test]
    fn a_sidecar_re_encodes_tagged() {
        let doc = serde_json::json!({"id": "r1", "owner": "alice"});
        let bytes =
            encode_stored_row(&doc, SparseBodyFormatRef::VectorSidecar).expect("encode sidecar");

        let as_sidecar =
            decode_scanned_document(&bytes, SparseBodyFormatRef::VectorSidecar).expect("decode");
        assert_eq!(
            as_sidecar.get("owner").and_then(|v| v.as_str()),
            Some("alice")
        );

        let as_document =
            decode_scanned_document(&bytes, SparseBodyFormatRef::Document).expect("decode");
        assert_ne!(
            as_document.get("owner").and_then(|v| v.as_str()),
            Some("alice"),
            "if the plain decoder can read it, the encoder did not write the \
             tagged form and this test proves nothing"
        );

        let decoded: std::collections::HashMap<String, Value> =
            zerompk::from_msgpack(&bytes).expect("the bytes must be a tagged zerompk map");
        assert_eq!(decoded.get("owner"), Some(&Value::String("alice".into())));
    }
}
