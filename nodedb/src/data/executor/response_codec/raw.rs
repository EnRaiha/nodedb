// SPDX-License-Identifier: BUSL-1.1

//! Raw-msgpack passthrough encoders and the inverse decoder.
//!
//! The "raw" pattern eliminates the decode→re-encode cycle on document reads
//! by writing storage bytes directly into the response. The decoder accepts
//! both raw scan rows (`{id, data}` wrappers) and plain msgpack rows produced
//! by aggregate/join paths.

use super::super::msgpack_utils::write_str;

/// Encode document rows with raw MessagePack passthrough for the data field.
///
/// Each row is `(doc_id, raw_msgpack_bytes)`. The raw bytes are written directly
/// into the output without decoding to `serde_json::Value` first. This eliminates
/// the decode→re-encode cycle that was the main serialization tax on document reads.
///
/// Output format: msgpack array of `{"id": "<doc_id>", "data": <raw_msgpack_value>}`.
///
/// Visibility is `pub(crate)` (not Data-Plane-only) so the Control Plane sync
/// layer can re-encode the rows that survive shape-predicate filtering before
/// shipping a snapshot to subscribers.
pub(crate) fn encode_raw_document_rows(rows: &[(String, Vec<u8>)]) -> crate::Result<Vec<u8>> {
    let data_size: usize = rows.iter().map(|(id, d)| id.len() + d.len() + 16).sum();
    let mut buf = Vec::with_capacity(data_size + 8);

    msgpack_write_array_header(&mut buf, rows.len());

    for (id, data_bytes) in rows {
        // Write map header (2 entries: "id" and "data").
        buf.push(0x82); // fixmap with 2 entries

        write_str(&mut buf, "id");
        write_str(&mut buf, id);

        write_str(&mut buf, "data");

        // Raw passthrough: write the msgpack bytes directly as the value.
        // These bytes are already a valid msgpack map from storage.
        buf.extend_from_slice(data_bytes);
    }

    Ok(buf)
}

/// Decode concatenated row payloads into `(doc_id, msgpack_data)` pairs.
///
/// Also used by the Control Plane sync layer to filter snapshot documents
/// by a shape predicate before sending them to subscribers.
///
/// Input: zero or more msgpack arrays back-to-back. Elements may be either:
/// - raw scan rows from `encode_raw_document_rows` with `{id, data}` wrappers
/// - plain msgpack rows from aggregate/join paths serialized via `encode_json_vec`
///
/// For wrapped scan rows, the `data` field's raw bytes are extracted. For
/// plain rows, the entire row value is returned as `msgpack_data`.
pub(crate) fn decode_raw_scan_to_docs(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    use nodedb_query::msgpack_scan;

    let mut results = Vec::new();
    let mut pos = 0;

    while pos < bytes.len() {
        let first = bytes[pos];
        let (count, hdr_len) = if (0x90..=0x9f).contains(&first) {
            ((first & 0x0f) as usize, 1)
        } else if first == 0xdc && pos + 3 <= bytes.len() {
            (
                u16::from_be_bytes([bytes[pos + 1], bytes[pos + 2]]) as usize,
                3,
            )
        } else if first == 0xdd && pos + 5 <= bytes.len() {
            (
                u32::from_be_bytes([
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                    bytes[pos + 4],
                ]) as usize,
                5,
            )
        } else {
            break;
        };

        let mut inner = pos + hdr_len;
        for _ in 0..count {
            if inner >= bytes.len() {
                break;
            }

            let elem_start = inner;
            let elem_end = msgpack_scan::skip_value(bytes, inner).unwrap_or(bytes.len());

            let id = msgpack_scan::extract_field(bytes, elem_start, "id")
                .and_then(|(s, _e)| msgpack_scan::read_value(bytes, s))
                .and_then(|v| match v {
                    nodedb_types::Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_default();

            let data = msgpack_scan::extract_field(bytes, elem_start, "data")
                .map(|(s, e)| bytes[s..e].to_vec())
                .unwrap_or_else(|| bytes[elem_start..elem_end].to_vec());

            results.push((id, data));

            inner = elem_end;
        }
        pos = inner;
    }

    results
}

/// Storage→relational boundary: flatten gathered storage rows into a single
/// flat relational row array.
///
/// Gathered output is heterogeneous by producer — document/columnar scans emit
/// the storage wire format (`{id, data:<value>}` wrappers, carrying the
/// surrogate), while computed producers (aggregates, joins) already emit flat
/// column maps. This extracts the inner `data` value for storage rows and passes
/// flat rows through unchanged, returning one msgpack array of flat rows.
///
/// This is the ONE place storage rows become relational rows. The
/// relational-operator layer (hash-join probe, `ProviderScan`) consumes only
/// this flat shape and never sees — or sniffs for — a `{id, data}` wrapper. The
/// `{id, data}` format stays confined to the storage / transport / sync layer.
pub fn flatten_to_relational_rows(bytes: &[u8]) -> Vec<u8> {
    let flat: Vec<Vec<u8>> = decode_raw_scan_to_docs(bytes)
        .into_iter()
        .map(|(_id, data)| data)
        .collect();
    encode_binary_rows(&flat)
}

/// Flatten a gathered array of vector-search hits into flat relational rows for
/// the post-processing tail (`QueryOp::PostProcess` → `ProviderScan`).
///
/// A hit is `{id: <surrogate u32>, distance, doc_id?, body?: <doc msgpack>}`.
/// This mirrors the client-facing vector response translator
/// (`control::server::response_translate::vector`) but emits MessagePack rows
/// instead of JSON: the document `body`'s columns become top-level (so ORDER BY
/// / DISTINCT / projection can reference any document column), `distance` is
/// surfaced, `_surrogate` carries the internal id, and the document's own `id`
/// column wins over the surrogate. Rows without a body (e.g. a
/// `skip_payload_fetch` hit) still surface `id` / `distance` / `_surrogate`.
///
/// RLS is already enforced by the Data Plane (`VectorOp::Search.rls_filters`,
/// injected into the inner search before dispatch), so these gathered hits are
/// post-RLS.
pub fn flatten_vector_hits_to_relational_rows(bytes: &[u8]) -> Vec<u8> {
    use nodedb_types::Value;

    #[derive(zerompk::FromMessagePack)]
    #[msgpack(map)]
    struct Hit {
        id: u32,
        distance: f32,
        doc_id: Option<String>,
        body: Option<Vec<u8>>,
    }

    let hits: Vec<Hit> = match zerompk::from_msgpack(bytes) {
        Ok(h) => h,
        // Not a hit array (already flat, or a non-row payload): leave as-is.
        Err(_) => return bytes.to_vec(),
    };

    let rows: Vec<Vec<u8>> = hits
        .into_iter()
        .filter_map(|h| {
            // Base columns come from the document body (its own `id` wins over
            // the internal surrogate). The body is *bare* msgpack (the storage
            // wire shape), so decode/encode with the native `Value` codec — the
            // derived `zerompk` codec is tagged and would corrupt the row.
            let mut fields: std::collections::HashMap<String, Value> = match h
                .body
                .as_deref()
                .and_then(|b| nodedb_types::value_from_msgpack(b).ok())
            {
                Some(Value::Object(map)) => map,
                _ => std::collections::HashMap::new(),
            };
            fields
                .entry("distance".to_string())
                .or_insert(Value::Float(h.distance as f64));
            if !fields.contains_key("id") {
                match h.doc_id {
                    Some(pk) => {
                        fields.insert("id".to_string(), Value::String(pk));
                    }
                    None => {
                        fields.insert("id".to_string(), Value::Integer(h.id as i64));
                    }
                }
            }
            fields
                .entry("_surrogate".to_string())
                .or_insert(Value::Integer(h.id as i64));
            nodedb_types::value_to_msgpack(&Value::Object(fields)).ok()
        })
        .collect();

    encode_binary_rows(&rows)
}

/// Encode a list of pre-built binary msgpack rows into a single msgpack array.
///
/// Each row is already a valid msgpack value (typically a map). This just
/// wraps them in an array header and concatenates — zero decode.
pub fn encode_binary_rows(rows: &[Vec<u8>]) -> Vec<u8> {
    let data_size: usize = rows.iter().map(|r| r.len()).sum();
    let mut buf = Vec::with_capacity(data_size + 8);
    msgpack_write_array_header(&mut buf, rows.len());
    for row in rows {
        buf.extend_from_slice(row);
    }
    buf
}

/// Write a msgpack array header.
pub(super) fn msgpack_write_array_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x90 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDC);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;
    use std::collections::HashMap;

    /// A vector hit in the Data-Plane wire shape: a *bare* msgpack map with
    /// the same field names `VectorSearchHit` emits (`#[msgpack(map)]`).
    #[derive(zerompk::ToMessagePack)]
    #[msgpack(map)]
    struct TestHit {
        id: u32,
        distance: f32,
        doc_id: Option<String>,
        body: Option<Vec<u8>>,
    }

    fn bare_doc(pairs: Vec<(&str, Value)>) -> Vec<u8> {
        let map = pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).unwrap()
    }

    /// Decode the flattened output (a bare msgpack array of bare row maps) into
    /// per-row column maps.
    fn decode_rows(out: &[u8]) -> Vec<HashMap<String, Value>> {
        match nodedb_types::value_from_msgpack(out) {
            Ok(Value::Array(rows)) => rows
                .into_iter()
                .map(|r| match r {
                    Value::Object(m) => m,
                    other => panic!("expected row map, got {other:?}"),
                })
                .collect(),
            other => panic!("expected row array, got {other:?}"),
        }
    }

    #[test]
    fn flatten_vector_hits_merges_body_and_surfaces_metadata() {
        let body = bare_doc(vec![
            ("id", Value::String("r0".into())),
            ("tag", Value::String("keep".into())),
        ]);
        let input = zerompk::to_msgpack_vec(&vec![TestHit {
            id: 7,
            distance: 0.5,
            doc_id: None,
            body: Some(body),
        }])
        .unwrap();

        let rows = decode_rows(&flatten_vector_hits_to_relational_rows(&input));
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        // Document's own `id` wins over the internal surrogate.
        assert_eq!(row.get("id"), Some(&Value::String("r0".into())));
        // Payload column surfaced to top level.
        assert_eq!(row.get("tag"), Some(&Value::String("keep".into())));
        // Search metadata preserved.
        assert_eq!(row.get("distance"), Some(&Value::Float(0.5f32 as f64)));
        assert_eq!(row.get("_surrogate"), Some(&Value::Integer(7)));
    }

    #[test]
    fn flatten_vector_hits_without_body_falls_back_to_surrogate_id() {
        let input = zerompk::to_msgpack_vec(&vec![TestHit {
            id: 42,
            distance: 1.25,
            doc_id: None,
            body: None,
        }])
        .unwrap();

        let rows = decode_rows(&flatten_vector_hits_to_relational_rows(&input));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::Integer(42)));
        assert_eq!(rows[0].get("_surrogate"), Some(&Value::Integer(42)));
        assert_eq!(rows[0].get("distance"), Some(&Value::Float(1.25)));
    }
}
