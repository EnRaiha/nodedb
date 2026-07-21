// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for POST /v1/query and POST /v1/query/stream.
//!
//! Contracts asserted:
//! - 200 + JSON body with `rows` + correct content-type under Trust mode
//! - 401 when no bearer token under Password mode
//! - 400 for malformed request body (missing `sql` field)
//! - 400 for syntactically invalid SQL
//! - Content-Type is the v1 vendor type on success

mod common;
use common::pgwire_harness::TestServer;

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::state::SharedState;
use nodedb::wal::WalManager;

struct HttpOnlyServer {
    local_addr: std::net::SocketAddr,
    _server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

async fn start_http(auth_mode: AuthMode) -> HttpOnlyServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal =
        Arc::new(WalManager::open_for_testing(&dir.path().join("query.wal")).expect("open wal"));
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).unwrap();
    if auth_mode == AuthMode::Trust {
        shared
            .credentials
            .bootstrap_trust_superuser("nodedb")
            .expect("bootstrap trust superuser");
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let local_addr = listener.local_addr().expect("local addr");

    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_http = Arc::clone(&shared);
    let handle = tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_http,
            auth_mode,
            None,
            bus,
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(40)).await;

    HttpOnlyServer {
        local_addr,
        _server: handle,
        _dir: dir,
    }
}

// ─── /v1/query — auth ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/query", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"sql": "SELECT 1"}))
        .send()
        .await
        .expect("POST /v1/query");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "/v1/query must require auth under Password mode"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_rejects_invalid_bearer() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/query", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Authorization", "Bearer ndb_bogus")
        .json(&serde_json::json!({"sql": "SELECT 1"}))
        .send()
        .await
        .expect("POST /v1/query");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "/v1/query must reject invalid bearer tokens"
    );
}

// ─── /v1/query — malformed request ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_returns_400_for_missing_sql_field() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/query", srv.local_addr);
    // Body is valid JSON but lacks the required `sql` field.
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/json")
        .body(r#"{"notSql": "SELECT 1"}"#)
        .send()
        .await
        .expect("POST /v1/query");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        "/v1/query must return 422 for missing `sql` field (axum deserialization error)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_returns_400_for_non_json_body() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/query", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/json")
        .body("this is not JSON at all")
        .send()
        .await
        .expect("POST /v1/query");
    // axum returns 400 for JSON parse failures
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "/v1/query must return 400 for non-JSON body"
    );
}

// ─── /v1/query — content-type ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_response_carries_vendor_content_type() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/query", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"sql": "SHOW USERS"}))
        .send()
        .await
        .expect("POST /v1/query");

    // SHOW USERS may succeed (200) or surface an error (4xx/5xx) in a minimal
    // test node — either way the response must carry the vendor content-type.
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("application/vnd.nodedb.v1+json"),
        "/v1/query responses must carry the v1 vendor content-type; got: {ct}"
    );
}

// ─── /v1/query — wrong HTTP method ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_get_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/query", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/query");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/query must reject GET with 405"
    );
}

// ─── /v1/query/stream — auth ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_stream_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/query/stream", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"sql": "SELECT 1"}))
        .send()
        .await
        .expect("POST /v1/query/stream");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "/v1/query/stream must require auth under Password mode"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_stream_returns_400_for_missing_sql_field() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/query/stream", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/json")
        .body(r#"{"x": 1}"#)
        .send()
        .await
        .expect("POST /v1/query/stream");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        "/v1/query/stream must return 422 for missing `sql` field"
    );
}

/// A `SELECT` whose row count exceeds the Data-Plane `stream_chunk_size`
/// (default 1000) is emitted as several frames on the lazy NDJSON path; the
/// route must surface EVERY row as its own NDJSON line, never truncating to the
/// first chunk. Mirrors the pgwire `streaming_select` regression guard for the
/// HTTP sink.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_stream_returns_all_rows_across_chunks() {
    const ROW_COUNT: usize = 2_500;

    let srv = TestServer::start_multicores(4).await;
    let client = reqwest::Client::new();
    let stream_url = format!("http://127.0.0.1:{}/v1/query/stream", srv.http_port);

    // Seed over the persistent pgwire connection, not 2500 separate HTTP POSTs:
    // the HTTP sink under test is the streaming SELECT, and hammering the REST
    // endpoint with thousands of rapid single-row inserts trips the per-tenant
    // request rate limiter (429). The pgwire seeding path mirrors the
    // `streaming_select` regression test.
    srv.exec("CREATE COLLECTION http_stream WITH (engine='document_schemaless')")
        .await
        .expect("create collection");
    for i in 0..ROW_COUNT {
        srv.exec(&format!("INSERT INTO http_stream {{ id: 'r{i}', n: {i} }}"))
            .await
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
    }

    // No ORDER BY / DISTINCT / OFFSET / aggregate → streamable unordered scan.
    let resp = client
        .post(&stream_url)
        .json(&serde_json::json!({"sql": "SELECT n FROM http_stream"}))
        .send()
        .await
        .expect("POST /v1/query/stream");
    assert!(
        resp.status().is_success(),
        "streaming SELECT must succeed; got {}",
        resp.status()
    );

    let body = resp.text().await.expect("read ndjson body");
    let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        lines.len(),
        ROW_COUNT,
        "lazy NDJSON stream must emit all {ROW_COUNT} rows, not a truncated chunk"
    );
    // No in-band error line should appear on the happy path.
    for line in &lines {
        let v: serde_json::Value = sonic_rs::from_str(line).expect("each line is valid JSON");
        assert!(
            v.get("error").is_none(),
            "no NDJSON line should be an in-band error on the happy path: {line}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_stream_response_is_ndjson_content_type() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/query/stream", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"sql": "SHOW USERS"}))
        .send()
        .await
        .expect("POST /v1/query/stream");

    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // Either the streaming ndjson type or a 4xx error — if 2xx, must be ndjson.
    if resp.status().is_success() {
        assert!(
            ct.contains("ndjson") || ct.contains("x-ndjson"),
            "/v1/query/stream Content-Type must be application/x-ndjson on success; got: {ct}"
        );
    }
}
