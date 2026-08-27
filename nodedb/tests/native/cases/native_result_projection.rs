// SPDX-License-Identifier: BUSL-1.1

//! Result-shape parity between the native binary protocol and pgwire.
//!
//! A single daemon exposes both transports over one `SharedState`. The native
//! SQL response must project the user-declared columns from the query plan —
//! the same shape pgwire returns — regardless of engine or whether the read is
//! a point lookup or a full scan. A read that collapses to the storage-level
//! `(data, id)` blob, drops a declared column, or nulls out a computed cell is
//! a transport-gated divergence: the same query, same schema, same daemon
//! returns a shape one client can consume over pgwire but not over native.
//!
//! Schema and data are established over pgwire (the catalog-backed path); the
//! assertions read back over native and compare to the pgwire shape. Covers
//! three faces of the same flaw — the native full-scan path returning storage
//! tuples instead of the plan's column projection:
//!   - Document (strict + schemaless) full scans must project declared columns,
//!     the way point lookups by `id` already do.
//!   - KV reads must include the `value` column (the actual payload).
//!   - Vector search must serialize the computed `distance`, not collapse it
//!     to NULL.

mod common;

use common::native_harness::{do_handshake, send_sql};
use common::pgwire_harness::TestServer;

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::value::Value;
use tokio::net::TcpStream;

/// Open a native session against the server's native listener and complete the
/// handshake. The returned stream is ready for `send_sql`.
async fn native_session(srv: &TestServer) -> TcpStream {
    let addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

/// True if `cell` is a MessagePack/JSON-encoded document blob rather than a
/// projected scalar — a string that looks like `{"...":...}`. This is the
/// specific silent failure mode: the storage tuple leaking through instead of
/// the declared column value.
fn looks_like_document_blob(cell: &Value) -> bool {
    matches!(cell, Value::String(s) if s.trim_start().starts_with('{'))
}

// ── Symptom 1: document-backed full scans must project declared columns ──────

/// A strict-document full scan (no WHERE) over native must return the declared
/// columns `[id, title]`, not the storage-level `(data, id)` blob.
#[tokio::test]
async fn doc_strict_full_scan_projects_declared_columns() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE nb_doc (id TEXT PRIMARY KEY, title TEXT)")
        .await
        .expect("create");
    srv.exec("INSERT INTO nb_doc (id, title) VALUES ('x', 'foo')")
        .await
        .expect("insert");

    let mut stream = native_session(&srv).await;
    let resp = send_sql(&mut stream, 1, "SELECT id, title FROM nb_doc").await;
    srv.graceful_shutdown().await;

    assert_ne!(
        resp.status,
        ResponseStatus::Error,
        "full scan must succeed: {resp:?}"
    );
    let columns = resp.columns.expect("columns present");
    assert_eq!(
        columns,
        vec!["id".to_string(), "title".to_string()],
        "full scan must project declared columns, not storage tuple; got {columns:?}"
    );
    // Regression guard against the storage-tuple leak.
    assert!(
        !columns.contains(&"data".to_string()),
        "full scan must not expose the internal `data` blob column: {columns:?}"
    );
    let rows = resp.rows.expect("rows present");
    assert_eq!(rows.len(), 1, "one row expected: {rows:?}");
    assert_eq!(rows[0][0], Value::String("x".into()), "id cell");
    assert_eq!(rows[0][1], Value::String("foo".into()), "title cell");
    assert!(
        !looks_like_document_blob(&rows[0][0]),
        "id cell must be the scalar, not a JSON document blob: {:?}",
        rows[0][0]
    );
}

/// Positive control: strict-document point lookup by `id` already projects the
/// declared columns over native. This must stay green — it demonstrates the
/// divergence is full-scan-only.
#[tokio::test]
async fn doc_strict_point_lookup_projects_declared_columns() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE nb_doc_pt (id TEXT PRIMARY KEY, title TEXT)")
        .await
        .expect("create");
    srv.exec("INSERT INTO nb_doc_pt (id, title) VALUES ('x', 'foo')")
        .await
        .expect("insert");

    let mut stream = native_session(&srv).await;
    let resp = send_sql(
        &mut stream,
        1,
        "SELECT id, title FROM nb_doc_pt WHERE id = 'x'",
    )
    .await;
    srv.graceful_shutdown().await;

    assert_ne!(
        resp.status,
        ResponseStatus::Error,
        "point lookup must succeed: {resp:?}"
    );
    let columns = resp.columns.expect("columns present");
    assert_eq!(
        columns,
        vec!["id".to_string(), "title".to_string()],
        "point lookup must project declared columns; got {columns:?}"
    );
    let rows = resp.rows.expect("rows present");
    assert_eq!(rows[0][1], Value::String("foo".into()), "title cell");
}

/// Sibling of symptom 1 on the schemaless document engine: a full scan must
/// project the declared columns, sharing the same native full-scan path.
#[tokio::test]
async fn doc_schemaless_full_scan_projects_declared_columns() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION nb_docs WITH (engine='document_schemaless')")
        .await
        .expect("create");
    srv.exec("INSERT INTO nb_docs { id: 'x', title: 'foo' }")
        .await
        .expect("insert");

    let mut stream = native_session(&srv).await;
    let resp = send_sql(&mut stream, 1, "SELECT id, title FROM nb_docs").await;
    srv.graceful_shutdown().await;

    assert_ne!(
        resp.status,
        ResponseStatus::Error,
        "full scan must succeed: {resp:?}"
    );
    let columns = resp.columns.expect("columns present");
    assert_eq!(
        columns,
        vec!["id".to_string(), "title".to_string()],
        "schemaless full scan must project declared columns, not storage tuple; got {columns:?}"
    );
    assert!(
        !columns.contains(&"data".to_string()),
        "full scan must not expose the internal `data` blob column: {columns:?}"
    );
    let rows = resp.rows.expect("rows present");
    assert_eq!(rows[0][0], Value::String("x".into()), "id cell");
    assert_eq!(rows[0][1], Value::String("foo".into()), "title cell");
}

// ── Symptom 2: KV reads must include the `value` column ──────────────────────

/// A KV full scan over native must include the `value` column — the actual
/// payload, not a metadata field. Dropping it makes KV unusable over native.
#[tokio::test]
async fn kv_full_scan_includes_value_column() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION nb_kv (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')")
        .await
        .expect("create");
    srv.exec("INSERT INTO nb_kv (key, value) VALUES ('k1', 'v1')")
        .await
        .expect("insert");

    let mut stream = native_session(&srv).await;
    let resp = send_sql(&mut stream, 1, "SELECT key, value FROM nb_kv").await;
    srv.graceful_shutdown().await;

    assert_ne!(
        resp.status,
        ResponseStatus::Error,
        "full scan must succeed: {resp:?}"
    );
    let columns = resp.columns.expect("columns present");
    assert_eq!(
        columns,
        vec!["key".to_string(), "value".to_string()],
        "KV full scan must project key AND value; got {columns:?}"
    );
    // Regression guard: `value` must not silently vanish from the shape.
    assert!(
        columns.contains(&"value".to_string()),
        "KV full scan must include the `value` column: {columns:?}"
    );
    let rows = resp.rows.expect("rows present");
    assert_eq!(rows[0][0], Value::String("k1".into()), "key cell");
    assert_eq!(
        rows[0][1],
        Value::String("v1".into()),
        "value cell must carry the payload"
    );
}

/// Sibling: KV point lookup by key must also include the `value` column.
#[tokio::test]
async fn kv_point_lookup_includes_value_column() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION nb_kv_pt (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')")
        .await
        .expect("create");
    srv.exec("INSERT INTO nb_kv_pt (key, value) VALUES ('k1', 'v1')")
        .await
        .expect("insert");

    let mut stream = native_session(&srv).await;
    let resp = send_sql(
        &mut stream,
        1,
        "SELECT key, value FROM nb_kv_pt WHERE key = 'k1'",
    )
    .await;
    srv.graceful_shutdown().await;

    assert_ne!(
        resp.status,
        ResponseStatus::Error,
        "point lookup must succeed: {resp:?}"
    );
    let columns = resp.columns.expect("columns present");
    assert!(
        columns.contains(&"value".to_string()),
        "KV point lookup must include the `value` column: {columns:?}"
    );
    let rows = resp.rows.expect("rows present");
    let value_idx = columns
        .iter()
        .position(|c| c == "value")
        .expect("value column");
    assert_eq!(
        rows[0][value_idx],
        Value::String("v1".into()),
        "value cell must carry the payload"
    );
}

// ── Symptom 3: vector search must serialize the computed distance ────────────

/// `SEARCH ... USING VECTOR(...)` over native must serialize the computed
/// `distance`, not collapse it to NULL. Without distance the ranking signal is
/// gone and the surrogate ids are not actionable.
#[tokio::test]
async fn vector_search_serializes_distance() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION nb_vec WITH (engine='vector')")
        .await
        .expect("create");
    srv.exec("CREATE INDEX ON nb_vec (embedding)")
        .await
        .expect("index");
    srv.exec("INSERT INTO nb_vec { id: 'a', embedding: [0.1, 0.2, 0.3] }")
        .await
        .expect("insert");

    let mut stream = native_session(&srv).await;
    let resp = send_sql(
        &mut stream,
        1,
        "SEARCH nb_vec USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3], 1)",
    )
    .await;
    srv.graceful_shutdown().await;

    assert_ne!(
        resp.status,
        ResponseStatus::Error,
        "vector search must succeed: {resp:?}"
    );
    let columns = resp.columns.expect("columns present");
    let dist_idx = columns
        .iter()
        .position(|c| c == "distance")
        .unwrap_or_else(|| panic!("a `distance` column must be present: {columns:?}"));
    let rows = resp.rows.expect("rows present");
    assert_eq!(rows.len(), 1, "one match expected: {rows:?}");
    // Regression guard against the `Some(f32) -> None` collapse: the distance
    // cell must be a real number, not NULL.
    assert_ne!(
        rows[0][dist_idx],
        Value::Null,
        "distance must be serialized, not collapsed to NULL: row={:?}",
        rows[0]
    );
    assert!(
        matches!(rows[0][dist_idx], Value::Float(_) | Value::Integer(_)),
        "distance must be numeric, got {:?}",
        rows[0][dist_idx]
    );
}
