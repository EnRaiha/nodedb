// SPDX-License-Identifier: BUSL-1.1

use super::*;

#[test]
fn encode_vector_hits() {
    let hits = vec![
        VectorSearchHit {
            id: 1,
            distance: 0.5,
            doc_id: None,
            body: None,
        },
        VectorSearchHit {
            id: 2,
            distance: 0.8,
            doc_id: None,
            body: None,
        },
    ];
    let bytes = encode(&hits).unwrap();
    assert!(!bytes.is_empty());

    let json = decode_payload_to_json(&bytes);
    assert!(json.contains("\"id\""));
    assert!(json.contains("\"distance\""));
}

#[test]
fn encode_count_msg() {
    let bytes = encode_count("inserted", 42).unwrap();
    let json = decode_payload_to_json(&bytes);
    assert!(json.contains("\"inserted\""));
    assert!(json.contains("42"));
}

#[test]
fn json_passthrough() {
    let json_str = r#"[{"id":1}]"#;
    let result = decode_payload_to_json(json_str.as_bytes());
    assert_eq!(result, json_str);
}

#[test]
fn msgpack_to_json_roundtrip() {
    let value = serde_json::json!({"key": "value", "num": 42});
    let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
    let json = decode_payload_to_json(&msgpack);
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["key"], "value");
    assert_eq!(parsed["num"], 42);
}

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

// ── decode_payload: the counterpart every encoder here needs ────────────
//
// Each case below pins one Control-Plane read whose decoder used to be a bare
// JSON parser with the failure defaulted away. The shared assertion is the same
// one in every case: the bytes an encoder produced must NOT parse as JSON (that
// is the trap), and must decode through `decode_payload` to exactly what went
// in.

/// A JSON parser must fail on these bytes. If it ever stops failing, the
/// encoders changed format and the `decode_payload` contract needs rechecking —
/// but while it does fail, any decoder that defaults the failure away is
/// silently reporting an empty result.
fn assert_not_json(payload: &[u8]) {
    assert!(
        sonic_rs::from_slice::<serde_json::Value>(payload).is_err(),
        "encoder output parsed as JSON; the trap this guards no longer exists"
    );
}

/// `TOPK` / `RANGE` rows — `encode_json_vec_as_msgpack`.
#[test]
fn decode_payload_reads_back_json_vec_rows() {
    let rows = vec![
        serde_json::json!({ "rank": 1, "key": "p2" }),
        serde_json::json!({ "rank": 2, "key": "p1" }),
    ];
    let payload = encode_json_vec_as_msgpack(&rows).unwrap();
    assert_not_json(&payload);

    let decoded: Vec<serde_json::Value> = decode_payload(&payload).unwrap();
    assert_eq!(decoded, rows, "the rows encoded must be the rows decoded");
}

/// `LAST_VALUES` — `encode` over a tuple list.
#[test]
fn decode_payload_reads_back_last_values() {
    let entries: Vec<(u64, i64, f64)> =
        vec![(7, 1_700_000_000_000, 21.5), (9, 1_700_000_001_000, 4.0)];
    let payload = encode(&entries).unwrap();
    assert_not_json(&payload);

    let decoded: Vec<(u64, i64, f64)> = decode_payload(&payload).unwrap();
    assert_eq!(decoded, entries);
}

/// `LAST_VALUE` — `encode` over an `Option`. A present series and an absent one
/// are different facts, and both must survive the round trip.
#[test]
fn decode_payload_distinguishes_present_and_absent_last_value() {
    let present = encode(&Some((1_700_000_000_000i64, 21.5f64))).unwrap();
    assert_not_json(&present);
    let decoded: Option<(i64, f64)> = decode_payload(&present).unwrap();
    assert_eq!(decoded, Some((1_700_000_000_000, 21.5)));

    let absent = encode(&Option::<(i64, f64)>::None).unwrap();
    let decoded: Option<(i64, f64)> = decode_payload(&absent).unwrap();
    assert_eq!(
        decoded, None,
        "an absent series decodes to None, not an error"
    );
}

/// Remote graph traverse node ids — `encode` over a string list. Dropping one
/// of these payloads makes a cross-shard traversal report the local shard's
/// nodes as the whole answer.
#[test]
fn decode_payload_reads_back_traverse_node_ids() {
    let nodes: Vec<String> = vec!["n1".into(), "n2".into(), "n3".into()];
    let payload = encode(&nodes).unwrap();
    assert_not_json(&payload);

    let decoded: Vec<String> = decode_payload(&payload).unwrap();
    assert_eq!(decoded, nodes);
}

/// `SHOW CONTINUOUS AGGREGATES` runtime stats — `encode_serde` over a
/// `Serialize` type.
#[test]
fn decode_payload_reads_back_serde_encoded_stats() {
    #[derive(serde::Serialize, serde::Deserialize, Default, PartialEq, Debug)]
    struct Stats {
        name: String,
        watermark_ts: i64,
        stale: bool,
    }
    let stats = vec![Stats {
        name: "hourly".into(),
        watermark_ts: 1_700_000_000_000,
        stale: false,
    }];
    let payload = encode_serde(&stats).unwrap();
    assert_not_json(&payload);

    let decoded: Vec<Stats> = decode_payload(&payload).unwrap();
    assert_eq!(decoded, stats);
}

/// An empty payload is an empty result: a handler with nothing to report sends
/// no bytes, and that is a fact, not a failure.
#[test]
fn decode_payload_treats_an_empty_payload_as_an_empty_result() {
    let decoded: Vec<serde_json::Value> = decode_payload(&[]).unwrap();
    assert!(decoded.is_empty());
}

/// A NON-empty payload that will not deserialize must be an error. This is the
/// distinction whose absence turned every one of the decode bugs above into a
/// successful empty answer instead of a loud one.
#[test]
fn decode_payload_errors_on_an_undecodable_payload() {
    // Valid msgpack, wrong shape for the target type.
    let payload = encode_json_as_msgpack(&serde_json::json!("not a row list")).unwrap();
    let decoded: crate::Result<Vec<serde_json::Value>> = decode_payload(&payload);
    assert!(
        decoded.is_err(),
        "an unreadable payload must surface as an error, never as zero rows"
    );

    // Bytes that are neither msgpack nor JSON.
    let garbage: Vec<u8> = vec![0xC1, 0xC1, 0xC1];
    let decoded: crate::Result<Vec<String>> = decode_payload(&garbage);
    assert!(decoded.is_err());
}
