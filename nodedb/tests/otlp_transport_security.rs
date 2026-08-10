// SPDX-License-Identifier: BUSL-1.1

//! The OTLP receivers must consult the TLS policy.
//!
//! Both receivers — OTLP/HTTP and OTLP/gRPC — serve a bare `TcpListener` and
//! bind `0.0.0.0` by default, so every request they accept arrives in the
//! clear over whatever network the port is exposed on. Neither consulted
//! `check_transport_security`, so `reject_cleartext` was silently skipped:
//! an operator who configured "no plaintext" still got authenticated ingest
//! ports accepting writes in the clear.
//!
//! Unlike the sync WebSocket listener, these cannot be excused by a
//! TLS-terminating proxy. That listener refuses to bind anywhere but loopback,
//! which structurally guarantees the proxy topology; an OTLP receiver bound to
//! `0.0.0.0` has no way to know a proxy is in front, so under an explicit
//! `reject_cleartext` it must fail closed.
//!
//! The check lives inside `authenticate_otel` rather than beside it, so a new
//! handler cannot get the bearer gate while skipping the transport gate —
//! that is what turned the gRPC receiver's three handlers into a compile
//! error rather than a second audit.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::identity::Role;
use nodedb::control::security::tls_policy::TlsPolicyConfig;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, TenantId};
use nodedb::wal::WalManager;

/// A node whose TLS policy is built from `config`, plus a live OTLP/HTTP
/// receiver over it.
struct OtlpEndpoint {
    addr: SocketAddr,
    _server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

fn reject_cleartext() -> TlsPolicyConfig {
    TlsPolicyConfig {
        enabled: true,
        reject_cleartext: true,
        ..Default::default()
    }
}

fn permissive() -> TlsPolicyConfig {
    TlsPolicyConfig {
        enabled: true,
        reject_cleartext: false,
        ..Default::default()
    }
}

async fn serve_otlp(policy: TlsPolicyConfig) -> (OtlpEndpoint, String) {
    let dir = tempfile::tempdir().expect("create test directory");
    let wal = Arc::new(
        WalManager::open_for_testing(&dir.path().join("otlp.wal")).expect("open test WAL"),
    );
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new_with_tls_policy_config(dispatcher, wal, policy)
        .expect("construct shared state with a TLS policy");
    shared
        .credentials
        .bootstrap_trust_superuser("nodedb")
        .expect("bootstrap trust superuser");

    // A non-superuser API key: `reject_cleartext` deliberately exempts
    // superusers, so a superuser token would be admitted no matter what the
    // policy said and the test would prove nothing.
    let user_id = shared
        .credentials
        .create_service_account(
            "otlp_writer",
            TenantId::new(1),
            vec![Role::ReadWrite],
            vec![DatabaseId::DEFAULT],
        )
        .expect("create OTLP service account");
    let token = shared
        .api_keys
        .create_key(
            CreateKeyParams {
                username: "otlp_writer",
                user_id,
                tenant_id: TenantId::new(1),
                expires_secs: 0,
                scope: vec![],
                accessible_databases: vec![DatabaseId::DEFAULT],
            },
            Some(shared.credentials.catalog()),
        )
        .expect("create OTLP API key");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback OTLP listener");
    let addr = listener.local_addr().expect("OTLP listener address");
    let router = nodedb::control::otel::receiver::router(Arc::clone(&shared));
    let handle = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .ok();
    });
    tokio::time::sleep(Duration::from_millis(40)).await;

    (
        OtlpEndpoint {
            addr,
            _server: handle,
            _dir: dir,
        },
        token,
    )
}

/// POST an empty OTLP metrics body. The payload does not matter: the
/// transport gate runs during authentication, before any decode.
async fn post_metrics(endpoint: &OtlpEndpoint, token: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("http://{}/v1/metrics", endpoint.addr))
        .header("authorization", format!("Bearer {token}"))
        .body(Vec::new())
        .send()
        .await
        .expect("POST OTLP metrics")
        .status()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleartext_otlp_is_refused_when_the_policy_rejects_it() {
    let (endpoint, token) = serve_otlp(reject_cleartext()).await;

    assert_eq!(
        post_metrics(&endpoint, &token).await,
        reqwest::StatusCode::UNAUTHORIZED,
        "a cleartext OTLP request must be refused when the TLS policy rejects cleartext"
    );
}

/// The control: the same request, the same receiver, a policy that permits
/// cleartext. Without this, the test above would also pass if OTLP were
/// broken for some unrelated reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleartext_otlp_is_admitted_when_the_policy_permits_it() {
    let (endpoint, token) = serve_otlp(permissive()).await;

    assert_ne!(
        post_metrics(&endpoint, &token).await,
        reqwest::StatusCode::UNAUTHORIZED,
        "a cleartext OTLP request must pass the transport gate when the policy permits it"
    );
}
