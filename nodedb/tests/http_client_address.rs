// SPDX-License-Identifier: BUSL-1.1

//! The HTTP API's guards must see the client's real socket address.
//!
//! Every admission guard that takes a peer address — the IP blacklist and the
//! adaptive-auth risk gate — parses it as an address and ignores anything that
//! is not one. A route that hands them a fixed transport label instead of the
//! accepted socket's address therefore disables both guards for the whole
//! HTTP surface while still appearing to call them.
//!
//! Contracts asserted:
//! - a blacklisted client IP is refused on `/v1/query` and `/v1/query/stream`
//! - a client IP outside the blacklisted range is still served
//! - with `[auth.risk]` enabled, an HTTP query is scored, not refused as
//!   unassessed

mod common;
use common::pgwire_harness::TestServer;

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::security::risk::RiskConfig;
use nodedb::control::state::SharedState;
use nodedb::wal::WalManager;

/// A live HTTP listener bound to loopback, so requests carry a real
/// `127.0.0.1:<ephemeral>` peer address.
struct HttpEndpoint {
    local_addr: std::net::SocketAddr,
    _server: tokio::task::JoinHandle<()>,
    _dir: Option<tempfile::TempDir>,
}

async fn serve_http(
    shared: Arc<SharedState>,
    auth_mode: AuthMode,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback HTTP listener");
    let local_addr = listener.local_addr().expect("HTTP listener address");
    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let handle = tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener, shared, auth_mode, None, bus,
        )
        .await
        .ok();
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    (local_addr, handle)
}

/// An HTTP endpoint over a fully started node, so an admitted query really
/// executes and returns 200 rather than failing later for want of a Data
/// Plane.
async fn serve_over(shared: Arc<SharedState>) -> HttpEndpoint {
    let (local_addr, handle) = serve_http(shared, AuthMode::Trust).await;
    HttpEndpoint {
        local_addr,
        _server: handle,
        _dir: None,
    }
}

/// An HTTP endpoint over a minimal node whose risk scorer is built from
/// `risk_config`. `SharedState::new_with_risk_config` needs sole ownership of
/// the state, which rules out reusing a started server here.
async fn serve_with_risk(risk_config: RiskConfig) -> HttpEndpoint {
    let dir = tempfile::tempdir().expect("create test directory");
    let wal = Arc::new(
        WalManager::open_for_testing(&dir.path().join("risk.wal")).expect("open test WAL"),
    );
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new_with_risk_config(dispatcher, wal, risk_config)
        .expect("construct shared state with risk scoring");
    shared
        .credentials
        .bootstrap_trust_superuser("nodedb")
        .expect("bootstrap trust superuser");

    let (local_addr, handle) = serve_http(Arc::clone(&shared), AuthMode::Trust).await;
    HttpEndpoint {
        local_addr,
        _server: handle,
        _dir: Some(dir),
    }
}

async fn post_query(
    endpoint: &HttpEndpoint,
    path: &str,
    sql: &str,
) -> (reqwest::StatusCode, String) {
    let response = reqwest::Client::new()
        .post(format!("http://{}{path}", endpoint.local_addr))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .expect("POST HTTP query");
    let status = response.status();
    let body = response.text().await.expect("read HTTP query response");
    (status, body)
}

fn permissive_risk() -> RiskConfig {
    RiskConfig {
        enabled: true,
        allow_threshold: 1.0,
        deny_threshold: 2.0,
        ..Default::default()
    }
}

fn deny_everything_risk() -> RiskConfig {
    RiskConfig {
        enabled: true,
        allow_threshold: -1.0,
        deny_threshold: 0.0,
        ..Default::default()
    }
}

// ─── IP blacklist ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_is_refused_on_http_query() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION http_blacklist_rows")
        .await
        .expect("create collection");
    srv.shared
        .blacklist
        .blacklist_ip("127.0.0.0/8", "test ban", "admin", 0)
        .expect("blacklist the loopback range");

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = post_query(&http, "/v1/query", "SELECT * FROM http_blacklist_rows").await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "an HTTP query from a blacklisted client IP must be refused; response: {body}"
    );
    assert!(
        body.contains("IP blacklisted"),
        "the refusal must come from the IP blacklist, not some later guard; response: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_is_refused_on_http_query_stream() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION http_blacklist_stream_rows")
        .await
        .expect("create collection");
    srv.shared
        .blacklist
        .blacklist_ip("127.0.0.0/8", "test ban", "admin", 0)
        .expect("blacklist the loopback range");

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = post_query(
        &http,
        "/v1/query/stream",
        "SELECT * FROM http_blacklist_stream_rows",
    )
    .await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "an NDJSON HTTP query from a blacklisted client IP must be refused; response: {body}"
    );
    assert!(
        body.contains("IP blacklisted"),
        "the refusal must come from the IP blacklist, not some later guard; response: {body}"
    );
}

/// Regression guard for the fix above: threading the real address must not
/// turn every HTTP query into a refusal. The blacklisted range deliberately
/// excludes loopback, so this request is the same shape as the refused one
/// and differs only in whether the client's address matches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_ip_outside_the_blacklisted_range_is_still_served() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION http_allowed_rows")
        .await
        .expect("create collection");
    srv.shared
        .blacklist
        .blacklist_ip("10.0.0.0/8", "test ban", "admin", 0)
        .expect("blacklist a range the loopback client is not in");

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = post_query(&http, "/v1/query", "SELECT * FROM http_allowed_rows").await;

    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "a client outside the blacklisted range must still be served; response: {body}"
    );
}

// ─── Risk gate ───────────────────────────────────────────────────────────────

/// With risk scoring enabled the request must be *assessed*. Without a real
/// client address the scope carries no score at all, and the gate refuses it
/// as unassessed — a different refusal, with a different reason, from the
/// policy decision this asserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn risk_enabled_http_query_is_scored_rather_than_unassessed() {
    let http = serve_with_risk(deny_everything_risk()).await;
    let (status, body) = post_query(&http, "/v1/query", "SHOW USERS").await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a deny-everything risk policy must refuse the request; response: {body}"
    );
    assert!(
        body.contains("denied by risk policy"),
        "the request must be refused by the scored policy decision; response: {body}"
    );
    assert!(
        !body.contains("risk assessment unavailable"),
        "an HTTP query carries a real client address and must never be unassessed; response: {body}"
    );
}

/// The same route, same configuration shape, thresholds that admit: a scored
/// request in the allow band is not refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn risk_enabled_http_query_in_the_allow_band_is_admitted() {
    let http = serve_with_risk(permissive_risk()).await;
    let (status, body) = post_query(&http, "/v1/query", "SHOW USERS").await;

    assert_ne!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a scored request inside the allow band must pass the admission gate; response: {body}"
    );
    assert!(
        !body.contains("risk assessment unavailable"),
        "an HTTP query carries a real client address and must never be unassessed; response: {body}"
    );
}
