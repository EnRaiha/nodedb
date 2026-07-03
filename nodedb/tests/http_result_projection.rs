// SPDX-License-Identifier: BUSL-1.1

//! Result-shape parity between HTTP `/v1/query` and pgwire/native.
//!
//! The HTTP query path was unified onto the same neutral shaping core that
//! backs pgwire and the native binary protocol: exactly one authority decides
//! what a query's result rows look like, regardless of which transport asked
//! for them. Before this, HTTP returned raw storage-level `{id, data}`
//! envelopes with no projection at all — KV reads were missing the `value`
//! payload, vector search dropped the computed `distance`, and document scans
//! surfaced encoded blobs instead of the declared columns.
//!
//! Schema and data are established over pgwire (the catalog-backed path, via
//! `TestServer::exec`); the assertions read back over HTTP's `/v1/query` and
//! check the JSON `rows` array has the same shape pgwire/native already
//! guarantee (see `native_result_projection.rs`). Covers the same three
//! symptoms of the storage-tuple leak, on the HTTP transport:
//!   - Document (strict + schemaless) full scans must project declared
//!     columns, not the internal `data` blob.
//!   - KV reads must include the `value` column (the actual payload).
//!   - Vector search must serialize the computed `distance`, not collapse it
//!     to null.

mod common;

use common::pgwire_harness::TestServer;

/// POST `sql` to the server's `/v1/query` endpoint (Trust auth mode — no
/// bearer token required) and return the parsed `rows` JSON array.
///
/// Asserts the HTTP status is a success before parsing the body, and that the
/// response carries a `rows` array at all, so callers can go straight to
/// shape assertions on the returned rows.
async fn query_rows(http_port: u16, sql: &str) -> Vec<serde_json::Value> {
    let url = format!("http://127.0.0.1:{http_port}/v1/query");
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .expect("POST /v1/query");
    assert!(
        resp.status().is_success(),
        "query must succeed over HTTP: {} — sql={sql}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.expect("decode JSON body");
    body["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("`rows` array present in body: {body:?}"))
        .clone()
}

/// True if `row` is the storage-tuple leak: a bare `{id, data}` envelope, or a
/// single `result` key whose value is an encoded JSON document string, rather
/// than the declared columns projected as top-level fields.
fn looks_like_envelope_leak(row: &serde_json::Value) -> bool {
    let Some(obj) = row.as_object() else {
        return false;
    };
    obj.contains_key("data")
        || (obj.len() == 1
            && obj
                .get("result")
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.trim_start().starts_with('{')))
}

// ── Symptom 1: document-backed full scans must project declared columns ──────

/// A strict-document full scan (no WHERE) over HTTP must return the declared
/// columns `[id, title]` as row fields, not the storage-level `(data, id)`
/// blob.
#[tokio::test]
async fn http_doc_strict_full_scan_projects_declared_columns() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE nb_doc_http (id TEXT PRIMARY KEY, title TEXT)")
        .await
        .expect("create");
    srv.exec("INSERT INTO nb_doc_http (id, title) VALUES ('x', 'foo')")
        .await
        .expect("insert");

    let rows = query_rows(srv.http_port, "SELECT id, title FROM nb_doc_http").await;
    srv.graceful_shutdown().await;

    assert_eq!(rows.len(), 1, "one row expected: {rows:?}");
    let row = &rows[0];
    assert!(
        !looks_like_envelope_leak(row),
        "row must not be the raw storage envelope: {row:?}"
    );
    let obj = row.as_object().expect("row is a JSON object");
    assert!(
        !obj.contains_key("data"),
        "row must not expose the internal `data` blob column: {row:?}"
    );
    assert_eq!(
        obj.get("id").and_then(|v| v.as_str()),
        Some("x"),
        "id field: {row:?}"
    );
    assert_eq!(
        obj.get("title").and_then(|v| v.as_str()),
        Some("foo"),
        "title field: {row:?}"
    );
}

/// Sibling of the strict-document case on the schemaless document engine: a
/// full scan over HTTP must project the declared columns, sharing the same
/// shaping core as pgwire/native.
#[tokio::test]
async fn http_doc_schemaless_full_scan_projects_declared_columns() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION nb_docs_http WITH (engine='document_schemaless')")
        .await
        .expect("create");
    srv.exec("INSERT INTO nb_docs_http { id: 'x', title: 'foo' }")
        .await
        .expect("insert");

    let rows = query_rows(srv.http_port, "SELECT id, title FROM nb_docs_http").await;
    srv.graceful_shutdown().await;

    assert_eq!(rows.len(), 1, "one row expected: {rows:?}");
    let row = &rows[0];
    assert!(
        !looks_like_envelope_leak(row),
        "row must not be the raw storage envelope: {row:?}"
    );
    let obj = row.as_object().expect("row is a JSON object");
    assert!(
        !obj.contains_key("data"),
        "row must not expose the internal `data` blob column: {row:?}"
    );
    assert_eq!(
        obj.get("id").and_then(|v| v.as_str()),
        Some("x"),
        "id field: {row:?}"
    );
    assert_eq!(
        obj.get("title").and_then(|v| v.as_str()),
        Some("foo"),
        "title field: {row:?}"
    );
}

// ── Symptom 2: KV reads must include the `value` column ──────────────────────

/// A KV full scan over HTTP must include the `value` column — the actual
/// payload, not just metadata. Dropping it makes KV unusable over HTTP.
#[tokio::test]
async fn http_kv_full_scan_includes_value_column() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION nb_kv_http (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')")
        .await
        .expect("create");
    srv.exec("INSERT INTO nb_kv_http (key, value) VALUES ('k1', 'v1')")
        .await
        .expect("insert");

    let rows = query_rows(srv.http_port, "SELECT key, value FROM nb_kv_http").await;
    srv.graceful_shutdown().await;

    assert_eq!(rows.len(), 1, "one row expected: {rows:?}");
    let row = &rows[0];
    let obj = row.as_object().expect("row is a JSON object");
    assert_eq!(
        obj.get("key").and_then(|v| v.as_str()),
        Some("k1"),
        "key field: {row:?}"
    );
    // Regression guard: `value` must not silently vanish from the shape.
    assert_eq!(
        obj.get("value").and_then(|v| v.as_str()),
        Some("v1"),
        "value field must carry the payload: {row:?}"
    );
}

// ── Symptom 3: vector search must serialize the computed distance ────────────

/// `SEARCH ... USING VECTOR(...)` over HTTP must serialize the computed
/// `distance` as a JSON number, not collapse it to null.
#[tokio::test]
async fn http_vector_search_serializes_distance() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION nb_vec_http WITH (engine='vector')")
        .await
        .expect("create");
    srv.exec("CREATE INDEX ON nb_vec_http (embedding)")
        .await
        .expect("index");
    srv.exec("INSERT INTO nb_vec_http { id: 'a', embedding: [0.1, 0.2, 0.3] }")
        .await
        .expect("insert");

    let rows = query_rows(
        srv.http_port,
        "SEARCH nb_vec_http USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3], 1)",
    )
    .await;
    srv.graceful_shutdown().await;

    assert_eq!(rows.len(), 1, "one match expected: {rows:?}");
    let row = &rows[0];
    let obj = row.as_object().expect("row is a JSON object");
    let distance = obj
        .get("distance")
        .unwrap_or_else(|| panic!("a `distance` field must be present: {row:?}"));
    // Regression guard against the `Some(f32) -> None` collapse: the distance
    // field must be a real JSON number, not null.
    assert!(
        !distance.is_null(),
        "distance must be serialized, not collapsed to null: row={row:?}"
    );
    assert!(
        distance.is_number(),
        "distance must be numeric, got {distance:?}"
    );
}
