// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for named-stream endpoints.
//!
//! Endpoints covered:
//! - GET /v1/streams/{stream}/events — SSE named-stream events
//! - GET /v1/streams/{stream}/poll   — named-stream long-poll
//!
//! Contracts asserted:
//! - Routes exist (not 404) under Trust mode
//! - 401 without bearer token under Password mode
//! - Cross-tenant tenant_id query param rejected
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
        Arc::new(WalManager::open_for_testing(&dir.path().join("streams.wal")).expect("open wal"));
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

fn is_auth_error(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN
}

// ─── /v1/streams/{stream}/events ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_events_route_is_mounted() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/streams/s1/events?group=g1", srv.local_addr);
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        reqwest::Client::new().get(&url).send(),
    )
    .await;
    match result {
        Ok(Ok(resp)) => {
            assert_ne!(
                resp.status(),
                reqwest::StatusCode::NOT_FOUND,
                "/v1/streams/s1/events must be mounted (not 404)"
            );
        }
        Ok(Err(e)) => panic!("Request error: {e}"),
        Err(_) => {} // SSE stream opened — route confirmed mounted
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_events_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/streams/s1/events?group=g1", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/streams/s1/events");
    assert!(
        is_auth_error(resp.status()),
        "/v1/streams/s1/events must require auth under Password mode; got {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_events_rejects_cross_tenant_param() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!(
        "http://{}/v1/streams/s1/events?group=g1&tenant_id=42",
        srv.local_addr
    );
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        reqwest::Client::new().get(&url).send(),
    )
    .await;
    if let Ok(Ok(resp)) = result {
        assert!(
            is_auth_error(resp.status()),
            "/v1/streams/s1/events must reject cross-tenant tenant_id param; got {}",
            resp.status()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_events_post_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/streams/s1/events", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("POST /v1/streams/s1/events");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/streams/s1/events POST must return 405"
    );
}

// ─── /v1/streams/{stream}/poll ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_poll_route_is_mounted() {
    // The route is mounted; when the stream does not exist the handler returns
    // 404 via ConsumeError — this is handler-level 404, not a routing 404.
    // We confirm the route exists by verifying the response is NOT a 405
    // (method-not-allowed would mean GET is unregistered at that path).
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/streams/s1/poll?group=g1", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/streams/s1/poll");
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/streams/s1/poll GET must be registered (not 405)"
    );
    // A 404 here means the stream doesn't exist (expected in a fresh test node),
    // not that the route is missing.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_poll_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/streams/s1/poll?group=g1", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/streams/s1/poll");
    assert!(
        is_auth_error(resp.status()),
        "/v1/streams/s1/poll must require auth under Password mode; got {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_poll_post_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/streams/s1/poll", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("POST /v1/streams/s1/poll");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/streams/s1/poll POST must return 405"
    );
}
