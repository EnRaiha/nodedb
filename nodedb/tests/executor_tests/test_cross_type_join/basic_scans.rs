// SPDX-License-Identifier: BUSL-1.1

//! Basic scan and roundtrip tests: KV, document, merge, and broadcast merge.

use crate::helpers::{make_ctx, send_ok};
use nodedb::data::executor::handlers::join;
use nodedb::data::executor::response_codec;
use nodedb_physical::physical_plan::{
    DocumentOp, EnforcementOptions, KvOp, PhysicalPlan, StorageMode,
};
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

pub(super) fn build_msgpack_map(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert(k.to_string(), serde_json::Value::String(v.to_string()));
    }
    nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).unwrap()
}

#[test]
fn kv_put_scan_roundtrip() {
    let mut ctx = make_ctx();

    let value1 = build_msgpack_map(&[("theme", "dark"), ("lang", "en")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "prefs".into(),
            key: b"d1".to_vec(),
            value: value1,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    let value2 = build_msgpack_map(&[("theme", "light"), ("lang", "fr")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "prefs".into(),
            key: b"d3".to_vec(),
            value: value2,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    let kv_docs = ctx.core.scan_collection(0, 1, "prefs", 100).unwrap();
    assert_eq!(
        kv_docs.len(),
        2,
        "expected 2 KV entries, got {}",
        kv_docs.len()
    );

    for (doc_id, bytes) in &kv_docs {
        let json = nodedb_types::json_from_msgpack(bytes).unwrap();
        let obj = json.as_object().unwrap();
        assert!(
            obj.contains_key("key"),
            "doc {doc_id} missing 'key': {json}"
        );
        assert!(
            obj.contains_key("theme"),
            "doc {doc_id} missing 'theme': {json}"
        );
    }
}

#[test]
fn document_scan_preserves_kv_rows_when_collection_has_strict_config() {
    let mut ctx = make_ctx();

    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Register {
            collection: "prefs".into(),
            indexes: Vec::new(),
            crdt_enabled: false,
            storage_mode: StorageMode::Strict {
                schema: StrictSchema {
                    columns: vec![
                        ColumnDef::required("key", ColumnType::String).with_primary_key(),
                        ColumnDef::required("theme", ColumnType::String),
                        ColumnDef::nullable("lang", ColumnType::String),
                    ],
                    version: 1,
                    dropped_columns: Vec::new(),
                    bitemporal: false,
                },
            },
            enforcement: Box::new(EnforcementOptions::default()),
            bitemporal: false,
            conflict_policy: None,
            timeseries: None,
            vector_primary: None,
        }),
    );

    let value = build_msgpack_map(&[("theme", "dark"), ("lang", "en")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "prefs".into(),
            key: b"d1".to_vec(),
            value,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: "prefs".into(),
            filters: Vec::new(),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        }),
    );

    let json = response_codec::decode_payload_to_json(&payload);
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));
    assert_eq!(parsed.len(), 1, "expected 1 row, got {json}");

    let data = parsed[0]["data"]
        .as_object()
        .unwrap_or_else(|| panic!("expected object data, got {}", parsed[0]["data"]));
    assert_eq!(data.get("key").and_then(|v| v.as_str()), Some("d1"));
    assert_eq!(data.get("theme").and_then(|v| v.as_str()), Some("dark"));
    assert_eq!(data.get("lang").and_then(|v| v.as_str()), Some("en"));
}

#[test]
fn schemaless_put_scan_roundtrip() {
    let mut ctx = make_ctx();

    let doc1 = build_msgpack_map(&[("id", "d1"), ("name", "Alice")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "docs".into(),
            document_id: "d1".into(),
            value: doc1,
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"d1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    let doc2 = build_msgpack_map(&[("id", "d3"), ("name", "Carol")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "docs".into(),
            document_id: "d3".into(),
            value: doc2,
            surrogate: nodedb_types::Surrogate::new(2),
            pk_bytes: b"d3".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    let docs = ctx.core.scan_collection(0, 1, "docs", 100).unwrap();
    assert_eq!(docs.len(), 2, "expected 2 docs, got {}", docs.len());

    for (doc_id, bytes) in &docs {
        let json = nodedb_types::json_from_msgpack(bytes).unwrap();
        let obj = json.as_object().unwrap();
        assert!(obj.contains_key("id"), "doc {doc_id} missing 'id': {json}");
        assert!(
            obj.contains_key("name"),
            "doc {doc_id} missing 'name': {json}"
        );
    }
}

#[test]
fn merge_encode_decode_roundtrip() {
    let left = build_msgpack_map(&[("id", "d1"), ("name", "Alice")]);
    let right = build_msgpack_map(&[("key", "d1"), ("theme", "dark")]);

    let merged = join::merge_join_docs_binary(&left, Some(&right), "docs", "prefs");
    let merged_json = nodedb_types::json_from_msgpack(&merged).unwrap();
    let obj = merged_json.as_object().unwrap();
    assert!(
        obj.contains_key("docs.id"),
        "missing docs.id: {merged_json}"
    );
    assert!(
        obj.contains_key("prefs.theme"),
        "missing prefs.theme: {merged_json}"
    );

    let rows = vec![merged.clone(), merged.clone(), merged];
    let encoded = response_codec::encode_binary_rows(&rows);
    let json_str = response_codec::decode_payload_to_json(&encoded);
    assert!(json_str.starts_with('['), "expected JSON array: {json_str}");

    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed.len(), 3);
}

#[test]
fn broadcast_merge_preserves_nonempty() {
    let row = build_msgpack_map(&[("docs.id", "d1"), ("prefs.theme", "dark")]);
    let nonempty_encoded = response_codec::encode_binary_rows(&[row]);
    let nonempty_json = response_codec::decode_payload_to_json(&nonempty_encoded);

    let mut all_elements: Vec<String> = Vec::new();
    // 22 empty cores
    for _ in 0..22 {
        let inner = "";
        if !inner.trim().is_empty() {
            all_elements.push(inner.to_string());
        }
    }
    // 1 core with data
    if nonempty_json.starts_with('[') && nonempty_json.ends_with(']') {
        let inner = &nonempty_json[1..nonempty_json.len() - 1];
        if !inner.trim().is_empty() {
            all_elements.push(inner.to_string());
        }
    }

    let merged = format!("[{}]", all_elements.join(","));
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&merged).unwrap();
    assert_eq!(parsed.len(), 1, "expected 1 row, got {}", parsed.len());
}
