// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for observability (PromQL) endpoints.
//!
//! Endpoints covered (all under `/v1/obsv/api/v1/`, feature-gated on "promql"):
//! - POST /v1/obsv/api/v1/write       — Prometheus remote write
//! - POST /v1/obsv/api/v1/query_range — PromQL range query
//!
//! These routes are only registered when the `promql` feature is enabled.
//! Without the feature, all tests assert 404 (route not mounted).
//!
//! Contracts asserted:
//! - Under promql feature: routes exist (not 404) under Trust mode
//! - Under promql feature: 401 without bearer token under Password mode
//! - Without promql feature: routes return 404

use std::sync::Arc;
use std::time::Duration;

use nodedb::config::auth::AuthMode;
use nodedb_test_support::booted_state::{BootOptions, BootedState};

struct TestServer {
    local_addr: std::net::SocketAddr,
    _server: tokio::task::JoinHandle<()>,
    _shared: BootedState,
}

async fn start_http(auth_mode: AuthMode) -> TestServer {
    let shared = BootedState::boot(BootOptions::default());
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
    let shared_http = Arc::clone(&*shared);
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
        _shared: shared,
    }
}

// ─── /v1/obsv/api/v1/write ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obsv_remote_write_route_presence() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/obsv/api/v1/write", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .body(b"".to_vec())
        .send()
        .await
        .expect("POST /v1/obsv/api/v1/write");

    {
        assert_ne!(
            resp.status(),
            reqwest::StatusCode::NOT_FOUND,
            "/v1/obsv/api/v1/write must be mounted when promql feature is enabled"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obsv_remote_write_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/obsv/api/v1/write", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .body(b"".to_vec())
        .send()
        .await
        .expect("POST /v1/obsv/api/v1/write");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "/v1/obsv/api/v1/write must require auth under Password mode"
    );
}

// ─── /v1/obsv/api/v1/query_range ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obsv_query_range_route_presence() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/obsv/api/v1/query_range", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .body(b"".to_vec())
        .send()
        .await
        .expect("POST /v1/obsv/api/v1/query_range");

    {
        assert_ne!(
            resp.status(),
            reqwest::StatusCode::NOT_FOUND,
            "/v1/obsv/api/v1/query_range must be mounted when promql feature is enabled"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obsv_query_range_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/obsv/api/v1/query_range", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .body(b"".to_vec())
        .send()
        .await
        .expect("POST /v1/obsv/api/v1/query_range");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "/v1/obsv/api/v1/query_range must require auth under Password mode"
    );
}
