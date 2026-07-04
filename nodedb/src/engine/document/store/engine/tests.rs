// SPDX-License-Identifier: BUSL-1.1

use super::batch::DocumentEngine;
use crate::engine::document::store::config::CollectionConfig;
use crate::engine::document::store::extract::json_to_msgpack;
use crate::engine::sparse::btree::SparseEngine;

fn make_engine() -> (SparseEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = SparseEngine::open(&dir.path().join("doc.redb")).unwrap();
    (engine, dir)
}

#[test]
fn put_and_get_document() {
    let (sparse, _dir) = make_engine();
    let doc_engine = DocumentEngine::new(&sparse, 0, 1);

    let doc = serde_json::json!({
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30
    });

    doc_engine.put("users", "u1", &doc).unwrap();
    let retrieved = doc_engine.get("users", "u1").unwrap().unwrap();

    assert_eq!(retrieved["name"], "Alice");
    assert_eq!(retrieved["email"], "alice@example.com");
    assert_eq!(retrieved["age"], 30);
}

#[test]
fn get_nonexistent_returns_none() {
    let (sparse, _dir) = make_engine();
    let doc_engine = DocumentEngine::new(&sparse, 0, 1);
    assert!(doc_engine.get("users", "missing").unwrap().is_none());
}

#[test]
fn delete_document() {
    let (sparse, _dir) = make_engine();
    let doc_engine = DocumentEngine::new(&sparse, 0, 1);

    let doc = serde_json::json!({"name": "Bob"});
    doc_engine.put("users", "u1", &doc).unwrap();
    assert!(doc_engine.delete("users", "u1").unwrap());
    assert!(doc_engine.get("users", "u1").unwrap().is_none());
}

#[test]
fn overwrite_document() {
    let (sparse, _dir) = make_engine();
    let doc_engine = DocumentEngine::new(&sparse, 0, 1);

    doc_engine
        .put("users", "u1", &serde_json::json!({"v": 1}))
        .unwrap();
    doc_engine
        .put("users", "u1", &serde_json::json!({"v": 2}))
        .unwrap();

    let doc = doc_engine.get("users", "u1").unwrap().unwrap();
    assert_eq!(doc["v"], 2);
}

#[test]
fn secondary_index_extraction() {
    let (sparse, _dir) = make_engine();
    let mut doc_engine = DocumentEngine::new(&sparse, 0, 1);

    doc_engine.register_collection(CollectionConfig::new("users").with_index("$.email"));

    doc_engine
        .put(
            "users",
            "u1",
            &serde_json::json!({"name": "Alice", "email": "alice@example.com"}),
        )
        .unwrap();
    doc_engine
        .put(
            "users",
            "u2",
            &serde_json::json!({"name": "Bob", "email": "bob@example.com"}),
        )
        .unwrap();

    let results = doc_engine
        .index_lookup("users", "$.email", "alice@example.com", false)
        .unwrap();
    assert_eq!(results, vec!["u1"]);
}

#[test]
fn array_index_extraction() {
    let (sparse, _dir) = make_engine();
    let mut doc_engine = DocumentEngine::new(&sparse, 0, 1);

    doc_engine.register_collection(CollectionConfig::new("users").with_index("$.tags[]"));

    doc_engine
        .put(
            "users",
            "u1",
            &serde_json::json!({"name": "Alice", "tags": ["admin", "editor"]}),
        )
        .unwrap();

    let results = doc_engine
        .index_lookup("users", "$.tags", "admin", false)
        .unwrap();
    assert_eq!(results, vec!["u1"]);

    let results = doc_engine
        .index_lookup("users", "$.tags", "editor", false)
        .unwrap();
    assert_eq!(results, vec!["u1"]);
}

#[test]
fn nested_field_index() {
    let (sparse, _dir) = make_engine();
    let mut doc_engine = DocumentEngine::new(&sparse, 0, 1);

    doc_engine.register_collection(CollectionConfig::new("docs").with_index("$.metadata.lang"));

    doc_engine
        .put(
            "docs",
            "d1",
            &serde_json::json!({"title": "Hello", "metadata": {"lang": "en"}}),
        )
        .unwrap();

    let results = doc_engine
        .index_lookup("docs", "$.metadata.lang", "en", false)
        .unwrap();
    assert_eq!(results, vec!["d1"]);
}

#[test]
fn raw_msgpack_roundtrip() {
    let (sparse, _dir) = make_engine();
    let doc_engine = DocumentEngine::new(&sparse, 0, 1);

    let doc = serde_json::json!({"key": "value", "num": 42});
    let rmpv_val = json_to_msgpack(&doc);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &rmpv_val).unwrap();

    doc_engine.put_raw("col", "id1", &buf).unwrap();

    let raw = doc_engine.get_raw("col", "id1").unwrap().unwrap();
    assert_eq!(raw, buf);

    let decoded = doc_engine.get("col", "id1").unwrap().unwrap();
    assert_eq!(decoded["key"], "value");
    assert_eq!(decoded["num"], 42);
}

#[test]
fn collections_are_isolated() {
    let (sparse, _dir) = make_engine();
    let doc_engine = DocumentEngine::new(&sparse, 0, 1);

    doc_engine
        .put("users", "id1", &serde_json::json!({"type": "user"}))
        .unwrap();
    doc_engine
        .put("orders", "id1", &serde_json::json!({"type": "order"}))
        .unwrap();

    let user = doc_engine.get("users", "id1").unwrap().unwrap();
    let order = doc_engine.get("orders", "id1").unwrap().unwrap();
    assert_eq!(user["type"], "user");
    assert_eq!(order["type"], "order");
}

#[test]
fn put_raw_with_index_extraction() {
    let (sparse, _dir) = make_engine();
    let mut doc_engine = DocumentEngine::new(&sparse, 0, 1);

    doc_engine.register_collection(CollectionConfig::new("items").with_index("$.category"));

    let doc = serde_json::json!({"name": "Widget", "category": "tools"});
    let rmpv_val = json_to_msgpack(&doc);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &rmpv_val).unwrap();

    doc_engine.put_raw("items", "i1", &buf).unwrap();

    let results = doc_engine
        .index_lookup("items", "$.category", "tools", false)
        .unwrap();
    assert_eq!(results, vec!["i1"]);
}
