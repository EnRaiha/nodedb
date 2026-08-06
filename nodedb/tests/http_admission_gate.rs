// SPDX-License-Identifier: BUSL-1.1

//! Every HTTP route that does work on a principal's behalf must run the
//! request-admission gate.
//!
//! Authorization alone is not admission: a caller can hold a perfectly good
//! grant on a collection and still be a blacklisted IP or a suspended/banned
//! account. A route that only authorizes serves that caller. These tests drive
//! real loopback HTTP so the accepted socket's address reaches the IP
//! blacklist, and assert the refusal on the routes that carry user data or act
//! for a principal.
//!
//! Contracts asserted:
//! - `/v1/collections/{name}/crdt/apply` refuses a blacklisted client IP
//! - the same route refuses a suspended account and a banned account
//! - the same route still serves an ordinary authorized caller
//! - the CDC poll, stream poll, status, metrics, PromQL, and drain routes all
//!   refuse a blacklisted client IP too

mod common;
use common::pgwire_harness::TestServer;

use std::sync::Arc;
use std::time::Duration;

use nodedb::config::auth::AuthMode;
use nodedb::control::security::auth_context::AuthStatus;
use nodedb::control::security::jit::auth_user::AuthUserRecord;
use nodedb::control::state::SharedState;

/// A live HTTP listener bound to loopback, so requests carry a real
/// `127.0.0.1:<ephemeral>` peer address.
struct HttpEndpoint {
    local_addr: std::net::SocketAddr,
    _server: tokio::task::JoinHandle<()>,
}

async fn serve_over(shared: Arc<SharedState>) -> HttpEndpoint {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback HTTP listener");
    let local_addr = listener.local_addr().expect("HTTP listener address");
    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let handle = tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared,
            AuthMode::Trust,
            None,
            bus,
        )
        .await
        .ok();
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    HttpEndpoint {
        local_addr,
        _server: handle,
    }
}

async fn get(endpoint: &HttpEndpoint, path: &str) -> (reqwest::StatusCode, String) {
    let response = reqwest::Client::new()
        .get(format!("http://{}{path}", endpoint.local_addr))
        .send()
        .await
        .expect("GET over loopback HTTP");
    let status = response.status();
    let body = response.text().await.expect("read HTTP response body");
    (status, body)
}

/// Like `get`, but keeps the response headers instead of discarding them —
/// for asserting on `X-RateLimit-*`, which a bare status/body pair can't see.
async fn get_with_headers(
    endpoint: &HttpEndpoint,
    path: &str,
) -> (reqwest::StatusCode, reqwest::header::HeaderMap, String) {
    let response = reqwest::Client::new()
        .get(format!("http://{}{path}", endpoint.local_addr))
        .send()
        .await
        .expect("GET over loopback HTTP");
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.text().await.expect("read HTTP response body");
    (status, headers, body)
}

async fn post_json(
    endpoint: &HttpEndpoint,
    path: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, String) {
    let response = reqwest::Client::new()
        .post(format!("http://{}{path}", endpoint.local_addr))
        .json(&body)
        .send()
        .await
        .expect("POST over loopback HTTP");
    let status = response.status();
    let text = response.text().await.expect("read HTTP response body");
    (status, text)
}

/// Blacklist the whole loopback range, so the client this test's `reqwest`
/// opens is the blacklisted one.
fn blacklist_loopback(shared: &SharedState) {
    shared
        .blacklist
        .blacklist_ip("127.0.0.0/8", "test ban", "admin", 0)
        .expect("blacklist the loopback range");
}

/// Put the configured trust identity — the principal every request in this
/// file authenticates as — into `status` in the auth-user store, which is what
/// the account-status half of the gate reads.
fn set_account_status(shared: &SharedState, status: AuthStatus) {
    let identity = nodedb::control::server::session_auth::configured_trust_identity(shared)
        .expect("trust mode must resolve a configured identity");
    let id = identity.user_id.to_string();
    shared
        .auth_users
        .upsert(AuthUserRecord {
            id: id.clone(),
            username: identity.username.clone(),
            email: String::new(),
            tenant_id: identity.tenant_id.as_u64(),
            provider: "test".into(),
            first_seen: 0,
            last_seen: 0,
            is_active: false,
            status,
            is_external: true,
            synced_claims: Default::default(),
            escalation_suspensions: 0,
        })
        .expect("upsert auth user record");
}

/// A real Loro delta in the shape `CrdtState` models: collection = root map,
/// row = a `Map` container under it, fields on the row map.
fn crdt_delta_hex(collection: &str, doc_id: &str) -> String {
    let doc = loro::LoroDoc::new();
    let coll = doc.get_map(collection);
    let row = coll
        .insert_container(doc_id, loro::LoroMap::new())
        .expect("row container");
    row.insert("name", "alice").expect("insert field");
    doc.commit();
    let delta = doc
        .export(loro::ExportMode::Snapshot)
        .expect("export loro snapshot");
    hex::encode(delta)
}

fn crdt_body(doc_id: &str, delta_hex: &str) -> serde_json::Value {
    serde_json::json!({ "doc_id": doc_id, "delta": delta_hex })
}

// ─── /v1/collections/{name}/crdt/apply ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_is_refused_on_crdt_apply() {
    const COLL: &str = "crdt_gate_blacklist";
    let srv = TestServer::start().await;
    srv.exec(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("create collection");
    blacklist_loopback(&srv.shared);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = post_json(
        &http,
        &format!("/v1/collections/{COLL}/crdt/apply"),
        crdt_body("doc1", &crdt_delta_hex(COLL, "doc1")),
    )
    .await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a CRDT apply from a blacklisted client IP must be refused; response: {body}"
    );
    assert!(
        body.contains("IP blacklisted"),
        "the refusal must come from the IP blacklist, not some later guard; response: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_account_is_refused_on_crdt_apply() {
    const COLL: &str = "crdt_gate_suspended";
    let srv = TestServer::start().await;
    srv.exec(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("create collection");
    set_account_status(&srv.shared, AuthStatus::Suspended);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = post_json(
        &http,
        &format!("/v1/collections/{COLL}/crdt/apply"),
        crdt_body("doc1", &crdt_delta_hex(COLL, "doc1")),
    )
    .await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a CRDT apply from a suspended account must be refused; response: {body}"
    );
    assert!(
        body.contains("account suspended"),
        "the refusal must name the account status; response: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn banned_account_is_refused_on_crdt_apply() {
    const COLL: &str = "crdt_gate_banned";
    let srv = TestServer::start().await;
    srv.exec(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("create collection");
    set_account_status(&srv.shared, AuthStatus::Banned);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = post_json(
        &http,
        &format!("/v1/collections/{COLL}/crdt/apply"),
        crdt_body("doc1", &crdt_delta_hex(COLL, "doc1")),
    )
    .await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a CRDT apply from a banned account must be refused; response: {body}"
    );
    assert!(
        body.contains("account banned"),
        "the refusal must name the account status; response: {body}"
    );
}

/// Regression guard for the three refusals above: adding the gate must not
/// turn every CRDT apply into a refusal. Same request shape, nothing
/// blacklisted and an active account.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_caller_still_applies_a_crdt_delta() {
    const COLL: &str = "crdt_gate_allowed";
    let srv = TestServer::start().await;
    srv.exec(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("create collection");

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = post_json(
        &http,
        &format!("/v1/collections/{COLL}/crdt/apply"),
        crdt_body("doc1", &crdt_delta_hex(COLL, "doc1")),
    )
    .await;

    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "an admitted, authorized caller must still have its delta applied; response: {body}"
    );
    assert!(
        body.contains("\"status\":\"ok\""),
        "the applied response shape must be unchanged; response: {body}"
    );
}

// ─── The rest of the gated HTTP surface ──────────────────────────────────────

/// The CDC poll route reads the caller's change data and now runs the full
/// gate before the collection is authorized or the change stream is queried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_is_refused_on_cdc_poll() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION cdc_gate_rows")
        .await
        .expect("create collection");
    blacklist_loopback(&srv.shared);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = get(&http, "/v1/cdc/cdc_gate_rows/poll").await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a CDC poll from a blacklisted client IP must be refused; response: {body}"
    );
    assert!(
        body.contains("IP blacklisted"),
        "the refusal must come from the IP blacklist; response: {body}"
    );
}

/// The named-stream poll route: refused before the stream registry is even
/// consulted, so a blacklisted caller cannot probe which streams exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_is_refused_on_stream_poll() {
    let srv = TestServer::start().await;
    blacklist_loopback(&srv.shared);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = get(&http, "/v1/streams/any_stream/poll?group=g1").await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a stream poll from a blacklisted client IP must be refused; response: {body}"
    );
    assert!(
        body.contains("IP blacklisted"),
        "the refusal must come from the IP blacklist; response: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_is_refused_on_status() {
    let srv = TestServer::start().await;
    blacklist_loopback(&srv.shared);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = get(&http, "/v1/status").await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a status read from a blacklisted client IP must be refused; response: {body}"
    );
    assert!(
        body.contains("IP blacklisted"),
        "the refusal must come from the IP blacklist; response: {body}"
    );
}

/// `/metrics` is authenticated and monitor-scoped, and now also admitted —
/// the gate runs before any internal counter is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_is_refused_on_metrics() {
    let srv = TestServer::start().await;
    blacklist_loopback(&srv.shared);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = get(&http, "/metrics").await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a metrics scrape from a blacklisted client IP must be refused; response: {body}"
    );
    assert!(
        body.contains("IP blacklisted"),
        "the refusal must come from the IP blacklist, not the monitor-role check; response: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_is_refused_on_promql_query() {
    let srv = TestServer::start().await;
    blacklist_loopback(&srv.shared);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = get(&http, "/v1/obsv/api/v1/query?query=up").await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a PromQL query from a blacklisted client IP must be refused; response: {body}"
    );
    assert!(
        body.contains("IP blacklisted"),
        "the refusal must come from the IP blacklist; response: {body}"
    );
}

/// PromQL query routes went through `admit` (the rate-limited door), not
/// `admit_without_rate_limit`, because a range/instant query is a
/// user-triggered, arbitrary-cost read fired per Grafana panel load — the
/// same shape as `/v1/query`, which already attaches these headers. Without
/// that change this request carries no `X-RateLimit-*` headers at all, so
/// this test fails on the pre-change handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promql_instant_query_carries_rate_limit_headers() {
    let srv = TestServer::start().await;

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, headers, body) = get_with_headers(&http, "/v1/obsv/api/v1/query?query=up").await;

    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "an admitted, ordinary PromQL instant query must still succeed; response: {body}"
    );
    assert!(
        headers.contains_key("x-ratelimit-limit"),
        "an admitted PromQL query must carry X-RateLimit-Limit like /v1/query does; headers: {headers:?}"
    );
    assert!(
        headers.contains_key("x-ratelimit-remaining"),
        "an admitted PromQL query must carry X-RateLimit-Remaining like /v1/query does; headers: {headers:?}"
    );
}

/// The drain hook takes a node out of rotation. It must refuse a blacklisted
/// caller *before* initiating shutdown — the server is still serving after
/// this request, which is itself the assertion that nothing was drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_is_refused_on_drain() {
    let srv = TestServer::start().await;
    blacklist_loopback(&srv.shared);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = post_json(&http, "/health/drain", serde_json::json!({})).await;

    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "a drain from a blacklisted client IP must be refused; response: {body}"
    );
    assert!(
        body.contains("IP blacklisted"),
        "the refusal must come from the IP blacklist; response: {body}"
    );
    assert!(
        !srv.shared.shutdown.is_shutdown(),
        "a refused drain must not have signalled shutdown"
    );
}

/// Liveness stays reachable to unauthenticated probes — it is deliberately
/// ungated, and adding gates elsewhere must not have changed that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_probe_stays_open_to_a_blacklisted_client() {
    let srv = TestServer::start().await;
    blacklist_loopback(&srv.shared);

    let http = serve_over(Arc::clone(&srv.shared)).await;
    let (status, body) = get(&http, "/health/live").await;

    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "the liveness probe is unauthenticated by design and must stay reachable; \
         response: {body}"
    );
}
