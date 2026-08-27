// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage: HTTP-layer authentication invariant.
//!
//! The invariant: every HTTP handler that performs tenant-scoped work,
//! admin work, or any write to shared state must go through identity
//! resolution before acting, and must source `tenant_id` from the
//! resolved identity rather than from request parameters.
//!
//! This file captures the full surface of handlers that historically
//! violated the invariant:
//!   * `/ws` (WebSocket RPC) hardcoded `tenant_id = 1`
//!   * `/v1/streams/{s}/poll`, `/v1/streams/{s}/events` read
//!     `tenant_id` from the query string
//!   * `/v1/cdc/{collection}`, `/v1/cdc/{collection}/poll` same
//!   * `/cluster/status` performed no auth at all (admin info leak)
//!   * `/obsv/api/v1/*` (PromQL) performed no auth; remote_write
//!     hardcoded `tenant_id = 1`
//!   * `wasm_upload::upload_wasm` hardcoded `tenant_id = 0` and wrote
//!     to the catalog regardless of caller
//!   * `subscribe.rs` hardcoded `TenantId::new(1)` in its default
//!     subscription path
//!
//! Every assertion is written against the correct spec: under non-Trust
//! auth modes, every such route returns 401 without a bearer token, and
//! under any mode, a `tenant_id` passed via query string must not be
//! honoured in place of the caller's identity.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::state::SharedState;
use nodedb::wal::WalManager;
use tokio_tungstenite::tungstenite::Message;

struct TestServer {
    local_addr: std::net::SocketAddr,
    shared: Arc<SharedState>,
    _server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

async fn start_http(auth_mode: AuthMode) -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("auth.wal")).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).unwrap();
    if auth_mode == AuthMode::Trust {
        shared
            .credentials
            .bootstrap_trust_superuser("nodedb")
            .expect("bootstrap trust superuser");
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

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

    tokio::time::sleep(Duration::from_millis(50)).await;

    TestServer {
        local_addr,
        shared,
        _server: handle,
        _dir: dir,
    }
}

fn is_unauthorized_ish(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::UNAUTHORIZED
        || status == reqwest::StatusCode::FORBIDDEN
        || status == reqwest::StatusCode::BAD_REQUEST
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_trust_auth_uses_configured_durable_identity() {
    let srv = start_http(AuthMode::Trust).await;
    let app_state = nodedb::control::server::http::auth::AppState {
        shared: Arc::clone(&srv.shared),
        auth_mode: AuthMode::Trust,
        shutdown_bus: nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&srv.shared.shutdown))
            .0,
        query_ctx: Arc::new(nodedb::control::planner::context::QueryContext::for_state(
            &srv.shared,
        )),
    };

    let identity = nodedb::control::server::http::auth::resolve_identity(
        &axum::http::HeaderMap::new(),
        &app_state,
        "127.0.0.1:1",
        nodedb::control::security::tls_policy::TransportSecurity::Cleartext,
    )
    .await
    .expect("configured HTTP trust identity");

    assert_eq!(identity.username, "nodedb");
    assert_ne!(identity.user_id, 0, "trust identity must be durable");
    assert!(identity.is_superuser);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_trust_auth_rejects_without_configured_identity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = Arc::new(
        WalManager::open_for_testing(&dir.path().join("unconfigured-auth.wal")).expect("open WAL"),
    );
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).expect("shared state");
    let app_state = nodedb::control::server::http::auth::AppState {
        shared: Arc::clone(&shared),
        auth_mode: AuthMode::Trust,
        shutdown_bus: nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown)).0,
        query_ctx: Arc::new(nodedb::control::planner::context::QueryContext::for_state(
            &shared,
        )),
    };

    let result = nodedb::control::server::http::auth::resolve_identity(
        &axum::http::HeaderMap::new(),
        &app_state,
        "127.0.0.1:1",
        nodedb::control::security::tls_policy::TransportSecurity::Cleartext,
    )
    .await;

    assert!(
        result.is_err(),
        "HTTP trust auth must fail closed before identity bootstrap"
    );
}

// ─── /v1/streams/{s}/poll ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_poll_rejects_missing_bearer_token() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/streams/s1/poll?group=g1", srv.local_addr);
    let resp = reqwest::Client::new().get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "poll_stream must require a bearer token under non-Trust auth modes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_poll_rejects_cross_tenant_tenant_id_parameter() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!(
        "http://{}/v1/streams/s1/poll?group=g1&tenant_id=42",
        srv.local_addr
    );
    let resp = reqwest::Client::new().get(&url).send().await.unwrap();
    assert!(
        is_unauthorized_ish(resp.status()),
        "poll_stream must reject a query-string tenant_id that does not match identity; got {}",
        resp.status()
    );
}

// ─── /v1/streams/{s}/events (SSE) ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_sse_rejects_missing_bearer_token() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/streams/s1/events?group=g1", srv.local_addr);
    let resp = reqwest::Client::new().get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "stream_sse must require a bearer token under non-Trust auth modes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_sse_rejects_cross_tenant_tenant_id_parameter() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!(
        "http://{}/v1/streams/s1/events?group=g1&tenant_id=42",
        srv.local_addr
    );
    let resp = reqwest::Client::new().get(&url).send().await.unwrap();
    assert!(
        is_unauthorized_ish(resp.status()),
        "stream_sse must reject a query-string tenant_id that does not match identity; got {}",
        resp.status()
    );
    assert_eq!(
        srv.shared.consumer_assignments.consumer_count(
            nodedb::types::DatabaseId::DEFAULT,
            42,
            "s1",
            "g1"
        ),
        0,
        "stream_sse must not register a consumer for a tenant the caller cannot act as"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_sse_unauthenticated_request_does_not_claim_consumer_slot() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!(
        "http://{}/v1/streams/s1/events?group=g1&tenant_id=42",
        srv.local_addr
    );
    let _ = tokio::time::timeout(
        Duration::from_millis(500),
        reqwest::Client::new().get(&url).send(),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        srv.shared.consumer_assignments.consumer_count(
            nodedb::types::DatabaseId::DEFAULT,
            42,
            "s1",
            "g1"
        ),
        0,
        "an unauthenticated SSE request must not register a consumer — DoS guard"
    );
}

// ─── /ws (WebSocket RPC) ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_upgrade_refused_without_bearer_token() {
    // The only way to guarantee that an unauthenticated caller cannot execute
    // SQL as tenant 1 is to refuse the WebSocket upgrade before any handler
    // state is attached. Post-upgrade "reject the first message" is not
    // sufficient: the handler is already running, the tenant is already pinned.
    let srv = start_http(AuthMode::Password).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let result = tokio_tungstenite::connect_async(&url).await;
    assert!(
        result.is_err(),
        "WS upgrade must be refused under non-Trust auth modes when no Bearer \
         token is presented; got a successful upgrade"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_live_on_unauthenticated_socket_registers_no_subscription() {
    // Defence in depth: even if the upgrade somehow succeeds (trust-mode-like
    // escape hatch, future middleware bug), a `live` method without prior
    // successful `auth` must not register a change-stream subscription.
    let srv = start_http(AuthMode::Password).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let Ok((mut ws, _)) = tokio_tungstenite::connect_async(&url).await else {
        // Upgrade refused — upstream guard caught it. Subscription count
        // must trivially still be zero.
        assert_eq!(srv.shared.change_stream.subscriber_count(), 0);
        return;
    };
    let req = serde_json::json!({
        "id": 1, "method": "live", "params": {"sql": "LIVE SELECT * FROM orders"}
    })
    .to_string();
    let _ = ws.send(Message::Text(req.into())).await;
    let _ = tokio::time::timeout(Duration::from_millis(300), ws.next()).await;

    assert_eq!(
        srv.shared.change_stream.subscriber_count(),
        0,
        "an unauthenticated `live` request must not register a change-stream subscription"
    );
}

// ─── /v1/cdc/{collection} (SSE) and /v1/cdc/{collection}/poll ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_stream_rejects_missing_bearer_token() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cdc/orders", srv.local_addr);
    let resp = reqwest::Client::new().get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "cdc::sse_stream must require a bearer token under non-Trust auth modes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_stream_rejects_cross_tenant_tenant_id_parameter() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders?tenant_id=42", srv.local_addr);
    let resp = reqwest::Client::new().get(&url).send().await.unwrap();
    assert!(
        is_unauthorized_ish(resp.status()),
        "cdc::sse_stream must reject a query-string tenant_id mismatch; got {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_changes_rejects_missing_bearer_token() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cdc/orders/poll", srv.local_addr);
    let resp = reqwest::Client::new().get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "cdc::poll_changes must require a bearer token under non-Trust auth modes"
    );
}

// ─── /v1/cluster/status (admin info leak) ───────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_status_rejects_missing_bearer_token() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cluster/status", srv.local_addr);
    let resp = reqwest::Client::new().get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "cluster_status must require a bearer token — admin metadata must not leak to \
         unauthenticated callers"
    );
}

// ─── /v1/obsv/api/v1/* (PromQL) ──────────────────────────────────────────────

mod promql {
    use super::*;

    async fn expect_401(path: &str, method: reqwest::Method) {
        let srv = start_http(AuthMode::Password).await;
        let url = format!("http://{}{}", srv.local_addr, path);
        let resp = reqwest::Client::new()
            .request(method, &url)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{path} must require a bearer token under non-Trust auth modes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promql_instant_query_rejects_missing_bearer() {
        expect_401("/v1/obsv/api/v1/query?query=up", reqwest::Method::GET).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promql_range_query_rejects_missing_bearer() {
        expect_401(
            "/v1/obsv/api/v1/query_range?query=up&start=0&end=1&step=1",
            reqwest::Method::GET,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promql_series_rejects_missing_bearer() {
        expect_401("/v1/obsv/api/v1/series", reqwest::Method::GET).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promql_label_names_rejects_missing_bearer() {
        expect_401("/v1/obsv/api/v1/labels", reqwest::Method::GET).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promql_label_values_rejects_missing_bearer() {
        expect_401("/v1/obsv/api/v1/label/job/values", reqwest::Method::GET).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promql_metadata_rejects_missing_bearer() {
        expect_401("/v1/obsv/api/v1/metadata", reqwest::Method::GET).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promql_remote_write_rejects_missing_bearer() {
        // remote_write is the worst: hardcodes tenant=1 AND accepts writes.
        expect_401("/v1/obsv/api/v1/write", reqwest::Method::POST).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promql_remote_read_rejects_missing_bearer() {
        expect_401("/v1/obsv/api/v1/read", reqwest::Method::POST).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promql_annotations_rejects_missing_bearer() {
        expect_401("/v1/obsv/api/v1/annotations", reqwest::Method::POST).await;
    }
}

// ─── Latent handlers (bug baked into source, currently unmounted) ────────────
//
// The WASM upload and subscription handlers must never use hardcoded tenant
// identities. Their tenant scope must come from authenticated request state.
//
// These source-level guards complement mounted-route tests and ensure the
// handler continues to derive tenant scope from authenticated request state.

#[test]
fn wasm_upload_handler_does_not_hardcode_tenant_identity() {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/control/server/http/routes/wasm_upload.rs"
    ))
    .expect("read wasm_upload.rs");
    assert!(
        !src.contains("let tenant_id = 0u32"),
        "upload_wasm must source tenant_id from a resolved identity, not a hardcoded 0u32"
    );
    assert!(
        src.contains("ResolvedIdentity(identity)")
            || src.contains("resolve_identity")
            || src.contains("resolve_auth"),
        "upload_wasm must extract or resolve authenticated identity before catalog access"
    );
}
