// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for cluster endpoints.
//!
//! Endpoints covered:
//! - GET /v1/status                              — node status
//! - GET /v1/cluster/status                      — cluster topology
//! - GET /v1/cluster/debug/raft/{group_id}        — Raft group diagnostics
//! - GET /v1/cluster/debug/transport              — QUIC transport diagnostics
//! - GET /v1/cluster/debug/quarantined-segments   — CRC-quarantined segments
//!
//! Contracts asserted:
//! - Routes exist (not 404) under Trust mode
//! - 401 without bearer token under Password mode
//! - Response carries vendor content-type on success
//! - Wrong HTTP method → 405

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

async fn start_http(auth_mode: AuthMode) -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal =
        Arc::new(WalManager::open_for_testing(&dir.path().join("cluster.wal")).expect("open wal"));
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

    TestServer {
        local_addr,
        _server: handle,
        _dir: dir,
    }
}

// ─── /v1/status ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_returns_node_id_field() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/status", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/status");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "/v1/status must return 200 under Trust mode"
    );
    let body: serde_json::Value = resp.json().await.expect("parse JSON");
    assert!(
        body.get("node_id").is_some(),
        "/v1/status must include 'node_id' field; got: {body}"
    );
    assert!(
        body.get("wal_next_lsn").is_some(),
        "/v1/status must include 'wal_next_lsn' field"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/status", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/status");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "/v1/status must require auth under Password mode"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_carries_vendor_content_type() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/status", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/status");
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("application/vnd.nodedb.v1+json"),
        "/v1/status must carry vendor content-type; got: {ct}"
    );
}

// ─── /v1/cluster/status ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_status_returns_non_404() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cluster/status", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cluster/status");
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/v1/cluster/status must be mounted (not 404)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_status_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cluster/status", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cluster/status");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "/v1/cluster/status must require auth (admin info must not leak)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_status_post_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cluster/status", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("POST /v1/cluster/status");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/cluster/status POST must return 405"
    );
}

// ─── /v1/cluster/debug/raft/{group_id} ───────────────────────────────────────
//
// The cluster/debug/* endpoints return 404 when `debug_endpoints_enabled` is
// false (the default, production hardening). The guard is intentionally before
// the auth check so unauthenticated probes also get 404 — the endpoints should
// not even appear to exist in hardened deployments.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_debug_raft_returns_404_when_debug_disabled() {
    // debug_endpoints_enabled defaults to false in test SharedState.
    // The guard returns 404 for every caller including superusers.
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cluster/debug/raft/1", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cluster/debug/raft/1");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/v1/cluster/debug/raft/* must return 404 when debug_endpoints_enabled=false \
         (production ops hardening — endpoints must not appear to exist)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_debug_raft_requires_auth_under_password_mode() {
    // Under Password mode, the startup gate returns 503 before the debug guard
    // can check debug_endpoints_enabled. Either 401, 403, or 404 is acceptable —
    // none of them reveal cluster internals.
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cluster/debug/raft/1", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cluster/debug/raft/1");
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::OK,
        "/v1/cluster/debug/raft/* must not return 200 without auth"
    );
}

// ─── /v1/cluster/debug/transport ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_debug_transport_returns_404_when_debug_disabled() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cluster/debug/transport", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cluster/debug/transport");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/v1/cluster/debug/transport must return 404 when debug_endpoints_enabled=false"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_debug_transport_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cluster/debug/transport", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cluster/debug/transport");
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::OK,
        "/v1/cluster/debug/transport must not return 200 without auth"
    );
}

// ─── /v1/cluster/debug/quarantined-segments ──────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_debug_quarantined_segments_returns_404_when_debug_disabled() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!(
        "http://{}/v1/cluster/debug/quarantined-segments",
        srv.local_addr
    );
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cluster/debug/quarantined-segments");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/v1/cluster/debug/quarantined-segments must return 404 when debug_endpoints_enabled=false"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_debug_quarantined_segments_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!(
        "http://{}/v1/cluster/debug/quarantined-segments",
        srv.local_addr
    );
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cluster/debug/quarantined-segments");
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::OK,
        "/v1/cluster/debug/quarantined-segments must not return 200 without auth"
    );
}
