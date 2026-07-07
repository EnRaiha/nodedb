// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for health/probe endpoints.
//!
//! Endpoints covered:
//! - GET /healthz          — k8s readiness probe
//! - GET /health/live      — unconditional liveness
//! - GET /health/ready     — WAL-recovered readiness
//! - POST /health/drain    — graceful drain trigger

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::state::SharedState;
use nodedb::wal::WalManager;

struct TestServer {
    local_addr: std::net::SocketAddr,
    _server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

async fn start_http() -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal =
        Arc::new(WalManager::open_for_testing(&dir.path().join("health.wal")).expect("open wal"));
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).unwrap();

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
            AuthMode::Trust,
            None,
            bus,
        )
        .await
        .ok();
    });

    // Wait for the gate (startup phase must reach GatewayEnable).
    // For testing purposes the Trust-mode server starts in Trust mode which
    // also fires the gate because startup is bypassed in test builds.
    tokio::time::sleep(Duration::from_millis(40)).await;

    TestServer {
        local_addr,
        _server: handle,
        _dir: dir,
    }
}

// ─── /healthz ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthz_returns_json_with_status_field() {
    let srv = start_http().await;
    let url = format!("http://{}/healthz", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /healthz");

    // Status is either 200 or 503 depending on startup phase; both are valid
    // here — the key contract is that the body is JSON with a "status" field.
    let status = resp.status();
    assert!(
        status == reqwest::StatusCode::OK || status == reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "/healthz must return 200 or 503, got {status}"
    );

    let body: serde_json::Value = resp.json().await.expect("parse JSON body");
    assert!(
        body.get("status").is_some(),
        "/healthz response must contain a 'status' field; got: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthz_404_not_served_on_wrong_path() {
    let srv = start_http().await;
    let url = format!("http://{}/health", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /health");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/health (bare) must be 404; use /healthz"
    );
}

// ─── /health/live ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_live_always_200() {
    let srv = start_http().await;
    let url = format!("http://{}/health/live", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /health/live");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "/health/live must always return 200"
    );
    let body: serde_json::Value = resp.json().await.expect("parse JSON");
    assert_eq!(
        body["status"], "alive",
        "/health/live body must have status='alive'"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_live_wrong_method_405() {
    let srv = start_http().await;
    let url = format!("http://{}/health/live", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("POST /health/live");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/health/live must reject POST with 405"
    );
}

// ─── /health/ready ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_ready_returns_json_with_status_and_wal_lsn() {
    let srv = start_http().await;
    let url = format!("http://{}/health/ready", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /health/ready");

    let status = resp.status();
    assert!(
        status == reqwest::StatusCode::OK || status == reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "/health/ready must be 200 or 503, got {status}"
    );

    let body: serde_json::Value = resp.json().await.expect("parse JSON");
    assert!(
        body.get("status").is_some(),
        "/health/ready must have 'status' field"
    );
    assert!(
        body.get("wal_lsn").is_some(),
        "/health/ready must have 'wal_lsn' field"
    );
    assert!(
        body.get("node_id").is_some(),
        "/health/ready must have 'node_id' field"
    );
}

// ─── /health/drain ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_drain_returns_draining_status() {
    let srv = start_http().await;
    let url = format!("http://{}/health/drain", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("POST /health/drain");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "/health/drain must return 200"
    );
    let body: serde_json::Value = resp.json().await.expect("parse JSON");
    assert_eq!(
        body["status"], "draining",
        "/health/drain body must have status='draining'"
    );
    assert!(
        body.get("node_id").is_some(),
        "/health/drain body must contain node_id"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_drain_wrong_method_405() {
    let srv = start_http().await;
    let url = format!("http://{}/health/drain", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /health/drain");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/health/drain GET must be 405 (only POST is registered)"
    );
}
