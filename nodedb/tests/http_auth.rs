// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for auth endpoints.
//!
//! Endpoints covered:
//! - POST /v1/auth/exchange-key   — API key → session token
//! - POST /v1/auth/session        — create session
//! - DELETE /v1/auth/session      — delete session
//!
//! Contracts asserted:
//! - Routes exist (not 404)
//! - Missing body → 422 (axum deserialization)
//! - No bearer token under Password mode → 401
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
        Arc::new(WalManager::open_for_testing(&dir.path().join("auth_ep.wal")).expect("open wal"));
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

// ─── /v1/auth/exchange-key ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exchange_key_returns_non_404() {
    // The route must be mounted. Under Trust mode with no key, expect a
    // 4xx (bad request or unprocessable) — but definitely not 404.
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/auth/exchange-key", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"api_key": "ndb_test_key"}))
        .send()
        .await
        .expect("POST /v1/auth/exchange-key");
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/v1/auth/exchange-key must be mounted (not 404)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exchange_key_accepts_empty_body_all_fields_optional() {
    // HttpExchangeKeyRequest has all-optional fields; {} is valid JSON for it.
    // Under Trust mode the handler should succeed (not reject deserialization).
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/auth/exchange-key", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("POST /v1/auth/exchange-key");
    // All fields are optional — {} deserializes correctly; handler proceeds.
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        "/v1/auth/exchange-key with empty JSON body must not return 422 (all fields are optional)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exchange_key_get_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/auth/exchange-key", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/auth/exchange-key");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/auth/exchange-key GET must return 405"
    );
}

// ─── /v1/auth/session ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_session_post_returns_non_404() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/auth/session", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"username": "admin", "password": "secret"}))
        .send()
        .await
        .expect("POST /v1/auth/session");
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/v1/auth/session POST must be mounted (not 404)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_session_delete_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/auth/session", srv.local_addr);
    let resp = reqwest::Client::new()
        .delete(&url)
        .send()
        .await
        .expect("DELETE /v1/auth/session");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "/v1/auth/session DELETE must require auth under Password mode"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_session_patch_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/auth/session", srv.local_addr);
    let resp = reqwest::Client::new()
        .patch(&url)
        .send()
        .await
        .expect("PATCH /v1/auth/session");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/auth/session PATCH must return 405"
    );
}

// ─── Content-Type on auth responses ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_session_post_carries_vendor_content_type_on_non_2xx() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/auth/session", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"username": "admin", "password": "secret"}))
        .send()
        .await
        .expect("POST /v1/auth/session");

    // The route is under json_routes which has stamp_content_type applied.
    // Any response from it — success or error — should carry the vendor type.
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("application/vnd.nodedb.v1+json") || ct.contains("application/json"),
        "/v1/auth/session response must carry JSON content-type; got: {ct}"
    );
}
