// SPDX-License-Identifier: BUSL-1.1

//! Integration test: HTTP middleware gates non-health routes on GatewayEnable.
//!
//! The test:
//! 1. Builds a minimal node with a real StartupSequencer (gate held).
//! 2. Binds and spawns the HTTP server.
//! 3. Verifies that GET /healthz returns 503 with `{"status":"starting",...}`.
//! 4. Verifies that POST /query returns 503 during startup.
//! 5. Fires the gate.
//! 6. Verifies that GET /healthz now returns 200.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::startup::{StartupPhase, StartupSequencer};
use nodedb::control::state::SharedState;

fn make_gated_state() -> (
    Arc<SharedState>,
    StartupSequencer,
    nodedb::control::startup::ReadyGate,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("gate_http_test.wal");
    let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let mut shared = SharedState::new(dispatcher, wal).unwrap();

    let (seq, gate) = StartupSequencer::new();
    let gw_gate = seq.register_gate(StartupPhase::GatewayEnable, "gateway-enable-http-test");

    Arc::get_mut(&mut shared)
        .expect("SharedState not yet cloned")
        .startup = Arc::clone(&gate);

    (shared, seq, gw_gate, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_healthz_returns_503_before_gateway_enable() {
    let (shared, _seq, _gw_gate, _dir) = make_gated_state();

    // Bind the HTTP server on an ephemeral port.
    let listen: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_http = Arc::clone(&shared);
    let bus_http = shutdown_bus.clone();
    tokio::spawn(async move {
        // Run the HTTP server. It binds immediately and serves /healthz from
        // the start, but non-health routes get 503 until GatewayEnable.
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_http,
            AuthMode::Trust,
            None,
            bus_http,
        )
        .await
        .ok();
    });

    // Give the server a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let base = format!("http://{local_addr}");
    let client = reqwest::Client::new();

    // /healthz must respond with 503 during startup.
    let resp = client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .expect("GET /healthz failed");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "/healthz should return 503 before GatewayEnable"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["status"], "starting",
        "body.status should be 'starting'"
    );

    // POST /query must also return 503 during startup.
    let resp = client
        .post(format!("{base}/query"))
        .header("content-type", "application/json")
        .body(r#"{"sql":"SELECT 1"}"#)
        .send()
        .await
        .expect("POST /query failed");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "/query should return 503 before GatewayEnable"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_healthz_returns_200_after_gateway_enable() {
    let (shared, _seq, gw_gate, _dir) = make_gated_state();

    let listen: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let (shutdown_bus2, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_http = Arc::clone(&shared);
    let bus_http2 = shutdown_bus2.clone();
    tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_http,
            AuthMode::Trust,
            None,
            bus_http2,
        )
        .await
        .ok();
    });

    // Fire the gate, then check /healthz returns 200.
    gw_gate.fire();

    tokio::time::sleep(Duration::from_millis(20)).await;

    let base = format!("http://{local_addr}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .expect("GET /healthz failed");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "/healthz should return 200 after GatewayEnable"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok", "body.status should be 'ok'");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_health_bare_returns_404() {
    // The bare /health route was removed in favour of /healthz (k8s convention).
    // Requests to /health must fall through to axum's default 404 handler.
    let (shared, _seq, gw_gate, _dir) = make_gated_state();

    let listen: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_http = Arc::clone(&shared);
    let bus_http = shutdown_bus.clone();
    tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_http,
            AuthMode::Trust,
            None,
            bus_http,
        )
        .await
        .ok();
    });

    // Fire the gate so the startup middleware doesn't interfere.
    gw_gate.fire();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let base = format!("http://{local_addr}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/health"))
        .send()
        .await
        .expect("GET /health failed");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/health (bare) must return 404 — use /healthz instead"
    );
}

/// The `nodedb healthcheck` subcommand must agree with the router about which
/// path a probe hits.
///
/// The runtime image ships no shell and no curl, so this subcommand IS the
/// container healthcheck. Probing a path the router does not serve makes every
/// container report unhealthy, and an orchestrator restarts a node that is
/// serving perfectly well.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_healthcheck_subcommand_probes_a_route_the_server_serves() {
    let (shared, _seq, gw_gate, _dir) = make_gated_state();

    let listener =
        tokio::net::TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
            .await
            .unwrap();
    let port = listener.local_addr().unwrap().port();

    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_http = Arc::clone(&shared);
    let bus_http = shutdown_bus.clone();
    tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_http,
            AuthMode::Trust,
            None,
            bus_http,
        )
        .await
        .ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Before the gate fires the node is not ready, so the probe must fail —
    // this is the signal that keeps traffic away from a starting node.
    let starting = tokio::task::spawn_blocking(move || nodedb::ctl::healthcheck::run(port))
        .await
        .unwrap();
    assert_eq!(
        starting, 1,
        "the probe must report unhealthy before GatewayEnable"
    );

    gw_gate.fire();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let ready = tokio::task::spawn_blocking(move || nodedb::ctl::healthcheck::run(port))
        .await
        .unwrap();
    assert_eq!(
        ready, 0,
        "a ready node must report healthy — a non-zero exit here means the \
         subcommand probes a path the router does not serve"
    );
}
