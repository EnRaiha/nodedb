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
/// - plain msgpack rows from aggregate/join paths serialized via
///   `encode_json_vec_as_msgpack`
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

/// Flatten a gathered array of vector-family search hits (dense / sparse /
/// multi-vector) into bare relational rows for the post-processing tail.
///
/// A hit is `{id: <HybridFusionKey wire string>, distance, doc_id?,
/// body?: <doc msgpack>}`. The document `body`'s columns become top-level (so
/// ORDER BY / DISTINCT / projection can reference any document column),
/// `distance` is surfaced, and `_surrogate` carries the internal id (or, for
/// a headless hit with no surrogate binding, the `__local_<id>` sentinel
/// text). The output `id` is, in order: the document body's own `id`; else
/// the user PK from `resolve_pk(surrogate)`; else the `doc_id`; else the raw
/// surrogate (or sentinel). `resolve_pk` lets a body-less hit (sparse /
/// multi-vector, or a `skip_payload_fetch` dense hit) still surface the user
/// PK — mirroring the Control-Plane response translator.
///
/// The body is *bare* msgpack (the storage wire shape), so it is decoded and
/// re-encoded with the native `Value` codec (`value_from_msgpack` /
/// `value_to_msgpack`); the derived `zerompk` `Value` codec is tagged and would
/// corrupt the row. RLS is already enforced by the Data Plane before the gather,
/// so these hits are post-RLS.
///
/// A hit whose stored body does not decode fails the call with a
/// serialization error: corruption is reported, never named from a fallback.
pub fn flatten_vector_hits_to_relational_rows(
    bytes: &[u8],
    resolve_pk: impl Fn(u32) -> Option<String>,
) -> crate::Result<Vec<u8>> {
    use nodedb_types::Value;

    let hits: Vec<GatheredVectorHit> = match zerompk::from_msgpack(bytes) {
        Ok(h) => h,
        // Not a hit array (already flat, or a non-row payload): leave as-is.
        Err(_) => return Ok(bytes.to_vec()),
    };

    let mut rows: Vec<Vec<u8>> = Vec::with_capacity(hits.len());
    for h in hits {
        // A stored body that does not decode is corruption: the hit is
        // refused, never named from a fallback.
        let decoded = match h.body.as_deref() {
            Some(body) => Some(nodedb_types::value_from_msgpack(body).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!(
                        "vector hit {} carries a stored body that does not decode: {e}",
                        h.id
                    ),
                }
            })?),
            None => None,
        };
        let mut fields: std::collections::HashMap<String, Value> = match decoded {
            Some(Value::Object(map)) => map,
            _ => std::collections::HashMap::new(),
        };
        fields
            .entry("distance".to_string())
            .or_insert(Value::Float(h.distance as f64));
        // `None` for a headless hit (no surrogate binding) — never
        // resolvable to a user PK.
        let surrogate = h.id.storage_key().map(|k| k.surrogate().as_u32());
        let raw_identity = match surrogate {
            Some(s) => Value::Integer(s as i64),
            None => Value::String(h.id.to_string()),
        };
        if !fields.contains_key("id") {
            // Prefer a catalog-resolved PK, then the DP-set doc_id, then the
            // raw surrogate (or the headless sentinel) as a last resort. The
            // resolver is asked exactly when `vector_hit_surrogates` lists
            // the hit: both decide through `body_needs_resolved_key`.
            let resolved = if body_needs_resolved_key(h.body.as_deref()) {
                surrogate.and_then(&resolve_pk)
            } else {
                None
            };
            match resolved.or(h.doc_id) {
                Some(pk) => {
                    fields.insert("id".to_string(), Value::String(pk));
                }
                None => {
                    fields.insert("id".to_string(), raw_identity.clone());
                }
            }
        }
        fields
            .entry("_surrogate".to_string())
            .or_insert(raw_identity);
        let row = nodedb_types::value_to_msgpack(&Value::Object(fields)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("vector hit row does not encode: {e}"),
            }
        })?;
        rows.push(row);
    }

    Ok(encode_binary_rows(&rows))
}

/// One gathered vector-family hit in the Data-Plane wire shape.
#[derive(zerompk::FromMessagePack)]
#[msgpack(map)]
struct GatheredVectorHit {
    id: crate::data::executor::handlers::hybrid_key::HybridFusionKey,
    distance: f32,
    doc_id: Option<String>,
    body: Option<Vec<u8>>,
}

/// Whether a hit takes its key from the resolver: it has no body, or a whole
/// body with no `id` key. The scan reads the map header and each key, and
/// skips each value without decoding or allocating. A truncated body takes
/// no key: the flatten reports it as corruption.
fn body_needs_resolved_key(body: Option<&[u8]>) -> bool {
    use nodedb_query::msgpack_scan::{extract_field, skip_value};

    match body {
        None => true,
        Some(body) => {
            skip_value(body, 0).is_some()
                && extract_field(body, 0, nodedb_types::DEFAULT_IDENTITY_COLUMN).is_none()
        }
    }
}

/// The surrogates of the bound hits in a gathered vector hit array, in hit
/// order, that [`flatten_vector_hits_to_relational_rows`] asks its resolver
/// for: those with no body, or a whole body with no `id`. Bodies are scanned,
/// never decoded. A payload that is not a hit array yields none, as the
/// flatten leaves it unchanged.
pub fn vector_hit_surrogates(bytes: &[u8]) -> Vec<u32> {
    let hits: Vec<GatheredVectorHit> = zerompk::from_msgpack(bytes).unwrap_or_default();
    hits.iter()
        .filter(|h| body_needs_resolved_key(h.body.as_deref()))
        .filter_map(|h| h.id.storage_key().map(|k| k.surrogate().as_u32()))
        .collect()
}

/// The surrogates of the bound hits in a gathered hybrid hit array, in hit
/// order: each `doc_id` storage key [`flatten_hybrid_hits_to_relational_rows`]
/// hands its resolver. A headless sentinel names no surrogate. A payload that
/// is not a hit array yields none.
pub fn hybrid_hit_surrogates(bytes: &[u8]) -> Vec<u32> {
    use nodedb_types::Value;

    let Ok(Value::Array(hits)) = nodedb_types::value_from_msgpack(bytes) else {
        return Vec::new();
    };
    hits.iter()
        .filter_map(|hit| match hit {
            Value::Object(fields) => match fields.get("doc_id") {
                Some(Value::String(doc_id)) => {
                    nodedb_types::StorageKey::parse(doc_id).map(|key| key.surrogate().as_u32())
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Flatten a gathered array of hybrid (RRF fusion) search hits into bare
/// relational rows for the post-processing tail.
///
/// A hybrid hit is a bare map `{doc_id: <surrogate hex | __local_ sentinel>,
/// <score alias>: f64, vector_rank?, text_rank?}` — no document body. Every
/// field is preserved; `doc_id` is resolved to the user PK via
/// `resolve_pk_from_hex` and injected as `id` (the column `SELECT id` reads),
/// mirroring the Control-Plane hybrid translator. An unresolvable `doc_id`
/// (sentinel or unknown surrogate) leaves the row without an `id`.
pub fn flatten_hybrid_hits_to_relational_rows(
    bytes: &[u8],
    resolve_pk_from_hex: impl Fn(&str) -> Option<String>,
) -> Vec<u8> {
    use nodedb_types::Value;

    let Ok(Value::Array(hits)) = nodedb_types::value_from_msgpack(bytes) else {
        return bytes.to_vec();
    };

    let rows: Vec<Vec<u8>> = hits
        .into_iter()
        .filter_map(|hit| {
            let Value::Object(mut fields) = hit else {
                return None;
            };
            if let Some(Value::String(hex)) = fields.get("doc_id").cloned()
                && let Some(pk) = resolve_pk_from_hex(&hex)
            {
                fields.insert("id".to_string(), Value::String(pk));
            }
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
    use super::super::encode::{decode_payload_to_json, encode_json_vec_as_msgpack};
    use super::*;
    use nodedb_types::Value;
    use std::collections::HashMap;

    #[test]
    fn raw_document_rows_roundtrip() {
        let doc1 = serde_json::json!({"name": "alice", "age": 30});
        let doc2 = serde_json::json!({"name": "bob", "age": 25});
        let msgpack1 = nodedb_types::json_to_msgpack(&doc1).unwrap();
        let msgpack2 = nodedb_types::json_to_msgpack(&doc2).unwrap();

        let rows = vec![
            ("doc1".to_string(), msgpack1),
            ("doc2".to_string(), msgpack2),
        ];

        let encoded = encode_raw_document_rows(&rows).unwrap();
        let json = decode_payload_to_json(&encoded);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["id"], "doc1");
        assert_eq!(parsed[0]["data"]["name"], "alice");
        assert_eq!(parsed[0]["data"]["age"], 30);
        assert_eq!(parsed[1]["id"], "doc2");
        assert_eq!(parsed[1]["data"]["name"], "bob");
    }

    #[test]
    fn raw_document_rows_empty() {
        let rows: Vec<(String, Vec<u8>)> = vec![];
        let encoded = encode_raw_document_rows(&rows).unwrap();
        let json = decode_payload_to_json(&encoded);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn decode_raw_scan_to_docs_accepts_plain_rows() {
        let rows = vec![serde_json::json!({"avg_amount": 43.598})];
        let encoded = encode_json_vec_as_msgpack(&rows).unwrap();

        let decoded = decode_raw_scan_to_docs(&encoded);

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, "");
        let decoded_json = decode_payload_to_json(&decoded[0].1);
        let parsed: serde_json::Value = serde_json::from_str(&decoded_json).unwrap();
        assert_eq!(parsed["avg_amount"], 43.598);
    }

    #[test]
    fn decode_raw_scan_to_docs_handles_mixed_arrays() {
        let wrapped_doc = serde_json::json!({"name": "alice"});
        let wrapped_rows = vec![(
            "doc1".to_string(),
            nodedb_types::json_to_msgpack(&wrapped_doc).unwrap(),
        )];
        let wrapped = encode_raw_document_rows(&wrapped_rows).unwrap();

        let plain_rows = vec![serde_json::json!({"avg_amount": 43.598})];
        let plain = encode_json_vec_as_msgpack(&plain_rows).unwrap();

        let mut combined = wrapped;
        combined.extend_from_slice(&plain);

        let decoded = decode_raw_scan_to_docs(&combined);

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, "doc1");
        assert_eq!(decode_payload_to_json(&decoded[0].1), r#"{"name":"alice"}"#);
        assert_eq!(decoded[1].0, "");
        let parsed: serde_json::Value =
            serde_json::from_str(&decode_payload_to_json(&decoded[1].1)).unwrap();
        assert_eq!(parsed["avg_amount"], 43.598);
    }

    fn bare_doc(pairs: Vec<(&str, Value)>) -> Vec<u8> {
        let map = pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).unwrap()
    }

    /// Decode a bare msgpack array of bare row maps into per-row column maps.
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

    /// A vector-family hit in the Data-Plane wire shape (bare `#[msgpack(map)]`).
    /// `id` is the `HybridFusionKey` wire string: an 8-hex-char storage key
    /// for a bound hit, matching `test_storage_key`.
    #[derive(zerompk::ToMessagePack)]
    #[msgpack(map)]
    struct TestHit {
        id: String,
        distance: f32,
        doc_id: Option<String>,
        body: Option<Vec<u8>>,
    }

    /// Render surrogate `n` as the 8-hex-char storage key text a bound
    /// `HybridFusionKey` carries on the wire.
    fn test_storage_key(n: u32) -> String {
        nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(n)).to_string()
    }

    #[test]
    fn vector_hits_merge_body_and_surface_metadata() {
        let body = bare_doc(vec![
            ("id", Value::String("r0".into())),
            ("tag", Value::String("keep".into())),
        ]);
        let input = zerompk::to_msgpack_vec(&vec![TestHit {
            id: test_storage_key(7),
            distance: 0.5,
            doc_id: None,
            body: Some(body),
        }])
        .unwrap();

        // Body carries `id`, so the resolver is not consulted.
        let rows = decode_rows(&flatten_vector_hits_to_relational_rows(&input, |_| None).unwrap());
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.get("id"), Some(&Value::String("r0".into())));
        assert_eq!(row.get("tag"), Some(&Value::String("keep".into())));
        assert_eq!(row.get("distance"), Some(&Value::Float(0.5f32 as f64)));
        assert_eq!(row.get("_surrogate"), Some(&Value::Integer(7)));
    }

    #[test]
    fn body_less_vector_hit_resolves_surrogate_to_user_pk() {
        // Sparse / multivec hit: no body → `id` comes from the PK resolver, not
        // the raw surrogate.
        let input = zerompk::to_msgpack_vec(&vec![TestHit {
            id: test_storage_key(42),
            distance: 1.25,
            doc_id: None,
            body: None,
        }])
        .unwrap();

        let rows = decode_rows(
            &flatten_vector_hits_to_relational_rows(&input, |s| Some(format!("pk-{s}"))).unwrap(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::String("pk-42".into())));
        assert_eq!(rows[0].get("_surrogate"), Some(&Value::Integer(42)));
        assert_eq!(rows[0].get("distance"), Some(&Value::Float(1.25)));
    }

    #[test]
    fn body_less_vector_hit_falls_back_to_surrogate_when_unresolved() {
        let input = zerompk::to_msgpack_vec(&vec![TestHit {
            id: test_storage_key(9),
            distance: 0.0,
            doc_id: None,
            body: None,
        }])
        .unwrap();

        let rows = decode_rows(&flatten_vector_hits_to_relational_rows(&input, |_| None).unwrap());
        assert_eq!(rows[0].get("id"), Some(&Value::Integer(9)));
    }

    #[test]
    fn hybrid_hits_resolve_doc_id_to_user_pk() {
        // Hybrid hit: bare map with a hex `doc_id` and a dynamic score alias.
        let hit = Value::Object(
            [
                ("doc_id".to_string(), Value::String("0000000a".into())),
                ("score".to_string(), Value::Float(0.9)),
            ]
            .into_iter()
            .collect(),
        );
        let input = nodedb_types::value_to_msgpack(&Value::Array(vec![hit])).unwrap();

        let rows = decode_rows(&flatten_hybrid_hits_to_relational_rows(&input, |hex| {
            (hex == "0000000a").then(|| "doc-ten".to_string())
        }));
        assert_eq!(rows.len(), 1);
        // doc_id resolved and injected as `id`; the score alias is preserved.
        assert_eq!(rows[0].get("id"), Some(&Value::String("doc-ten".into())));
        assert_eq!(rows[0].get("score"), Some(&Value::Float(0.9)));
        assert_eq!(
            rows[0].get("doc_id"),
            Some(&Value::String("0000000a".into()))
        );
    }

    fn hit(surrogate: u32, body: Option<Vec<u8>>) -> TestHit {
        TestHit {
            id: test_storage_key(surrogate),
            distance: 0.1,
            doc_id: None,
            body,
        }
    }

    /// Only the hits the flatten names from the resolver are listed: a body
    /// with an `id` is left out, a body without one and no body are listed.
    #[test]
    fn vector_hit_surrogates_lists_the_hits_the_flatten_resolves() {
        let input = zerompk::to_msgpack_vec(&vec![
            hit(9, Some(bare_doc(vec![("id", Value::String("r9".into()))]))),
            hit(5, Some(bare_doc(vec![("tag", Value::String("t".into()))]))),
            TestHit {
                id: "__local_3".into(),
                distance: 0.2,
                doc_id: None,
                body: None,
            },
            hit(4, None),
        ])
        .unwrap();
        assert_eq!(vector_hit_surrogates(&input), vec![5, 4]);
        assert!(vector_hit_surrogates(b"not msgpack hits").is_empty());

        // The flatten asks the resolver for exactly the listed surrogates.
        let asked = std::cell::RefCell::new(Vec::new());
        flatten_vector_hits_to_relational_rows(&input, |s| {
            asked.borrow_mut().push(s);
            None
        })
        .unwrap();
        assert_eq!(asked.into_inner(), vec![5, 4]);
    }

    /// A truncated stored body is corruption: the flatten refuses it, and
    /// `vector_hit_surrogates` asks no key for it.
    #[test]
    fn a_truncated_body_fails_the_flatten() {
        let mut body = bare_doc(vec![("tag", Value::String("keep".into()))]);
        body.truncate(body.len() - 2);
        let input = zerompk::to_msgpack_vec(&vec![hit(6, Some(body))]).unwrap();
        assert!(vector_hit_surrogates(&input).is_empty());
        let err = flatten_vector_hits_to_relational_rows(&input, |_| Some("resolved".into()))
            .expect_err("a truncated body fails the flatten");
        assert!(
            matches!(err, crate::Error::Serialization { .. }),
            "corruption is a serialization error: {err:?}"
        );
    }

    /// An `id` key after a nested map value is still found.
    #[test]
    fn an_id_after_a_nested_map_is_detected() {
        let nested = Value::Object(
            [("inner".to_string(), Value::Integer(1))]
                .into_iter()
                .collect(),
        );
        // A fixmap of two pairs, written by hand so `meta` precedes `id`.
        let mut body = vec![0x82u8];
        for (key, value) in [("meta", nested), ("id", Value::String("r7".into()))] {
            body.extend(nodedb_types::value_to_msgpack(&Value::String(key.into())).unwrap());
            body.extend(nodedb_types::value_to_msgpack(&value).unwrap());
        }
        assert!(!body_needs_resolved_key(Some(body.as_slice())));
        let without_id = bare_doc(vec![("x", Value::Integer(1))]);
        assert!(body_needs_resolved_key(Some(without_id.as_slice())));
        assert!(body_needs_resolved_key(None));

        let input = zerompk::to_msgpack_vec(&vec![hit(7, Some(body))]).unwrap();
        assert!(vector_hit_surrogates(&input).is_empty());
        let rows = decode_rows(
            &flatten_vector_hits_to_relational_rows(&input, |_| Some("resolved".into())).unwrap(),
        );
        assert_eq!(rows[0].get("id"), Some(&Value::String("r7".into())));
    }

    #[test]
    fn hybrid_hit_surrogates_skips_the_headless_sentinel() {
        let hit = |doc_id: &str| {
            Value::Object(
                [
                    ("doc_id".to_string(), Value::String(doc_id.into())),
                    ("score".to_string(), Value::Float(0.5)),
                ]
                .into_iter()
                .collect(),
            )
        };
        let input = nodedb_types::value_to_msgpack(&Value::Array(vec![
            hit("0000000a"),
            hit("__local_2"),
            hit("0000000b"),
        ]))
        .unwrap();
        assert_eq!(hybrid_hit_surrogates(&input), vec![10, 11]);
    }
}
