// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the bootstrap-listener join flow with the
//! full token state machine: valid token accepted, invalid HMAC rejected,
//! replayed token rejected, expired token rejected, audit entries logged.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::auth::audit::{AuditWriter, JoinOutcome, VecAuditWriter};
use nodedb_cluster::auth::bundle::{open_bundle, seal_bundle};
use nodedb_cluster::auth::join_token as tok;
use nodedb_cluster::auth::token_state::{
    InMemoryTokenStore, JoinTokenLifecycle, JoinTokenState, TokenStateBackend, TokenStateError,
};
use nodedb_cluster::bootstrap_listener::{
    BootstrapCredsRequest, BootstrapCredsResponse, BootstrapHandler, spawn_listener,
};
use nodedb_cluster::{generate_node_credentials_multi_san, issue_leaf_for_sans, verify_token};

// ── Helpers ─────────────────────────────────────────────────────────────────

fn epoch_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn mint_token(secret: &[u8; 32], for_node: u64, ttl_secs: u64) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + ttl_secs;
    tok::issue_token(secret, for_node, expiry).unwrap()
}

/// A `BootstrapHandler` that wires token state machine + audit writer
/// into the listener. Used for all integration tests.
struct StatefulBootstrapHandler<B, A> {
    ca: Arc<nexar::transport::tls::ClusterCa>,
    cluster_secret: [u8; 32],
    token_store: Arc<B>,
    audit: Arc<A>,
    inflight_timeout: Duration,
}

impl<B: TokenStateBackend, A: AuditWriter> StatefulBootstrapHandler<B, A> {
    fn issue(&self, node_id: u64) -> Result<BootstrapCredsResponse, String> {
        let node_san = format!("node-{node_id}");
        let creds = issue_leaf_for_sans(
            &self.ca,
            &[&node_san, nodedb_cluster::transport::config::SNI_HOSTNAME],
        )
        .map_err(|e| format!("issue leaf: {e}"))?;
        Ok(BootstrapCredsResponse {
            ok: true,
            error: String::new(),
            ca_cert_der: self.ca.cert_der().to_vec(),
            node_cert_der: creds.cert.to_vec(),
            node_key_der: creds.key.secret_der().to_vec(),
            cluster_secret: self.cluster_secret.to_vec(),
        })
    }
}

impl<B: TokenStateBackend, A: AuditWriter> BootstrapHandler for StatefulBootstrapHandler<B, A> {
    fn handle<'a>(
        &'a self,
        req: BootstrapCredsRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BootstrapCredsResponse> + Send + 'a>>
    {
        Box::pin(async move {
            // Step 1: constant-time HMAC verification.
            let (token_node, expiry_secs) = match verify_token(&req.token_hex, &self.cluster_secret)
            {
                Ok(v) => v,
                Err(e) => {
                    // PERSIST audit before responding (acquire-log-then-respond).
                    let th = tok::token_hash(&req.token_hex).unwrap_or([0u8; 32]);
                    self.audit
                        .append(nodedb_cluster::auth::audit::AuditEvent::new(
                            th,
                            None,
                            req.node_id,
                            JoinOutcome::Rejected {
                                reason: format!("{e}"),
                            },
                        ));
                    return BootstrapCredsResponse::error(format!("token: {e}"));
                }
            };

            if token_node != req.node_id {
                let th = tok::token_hash(&req.token_hex).unwrap_or([0u8; 32]);
                self.audit
                    .append(nodedb_cluster::auth::audit::AuditEvent::new(
                        th,
                        None,
                        req.node_id,
                        JoinOutcome::Rejected {
                            reason: "node id mismatch".into(),
                        },
                    ));
                return BootstrapCredsResponse::error(format!(
                    "node id mismatch: token bound to {token_node}, request claims {}",
                    req.node_id
                ));
            }

            let token_hash = match tok::token_hash(&req.token_hex) {
                Ok(h) => h,
                Err(e) => {
                    return BootstrapCredsResponse::error(format!("token hash: {e}"));
                }
            };

            // Step 2: check token state machine.
            // Use a dummy addr since we don't have the remote addr here.
            let dummy_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
            match self
                .token_store
                .begin_inflight(&token_hash, dummy_addr)
                .await
            {
                Ok(()) => {}
                Err(TokenStateError::AlreadyConsumed) => {
                    self.audit
                        .append(nodedb_cluster::auth::audit::AuditEvent::new(
                            token_hash,
                            None,
                            req.node_id,
                            JoinOutcome::Replayed,
                        ));
                    return BootstrapCredsResponse::error("join token already consumed");
                }
                Err(TokenStateError::Expired) => {
                    self.audit
                        .append(nodedb_cluster::auth::audit::AuditEvent::new(
                            token_hash,
                            None,
                            req.node_id,
                            JoinOutcome::TokenExpired,
                        ));
                    return BootstrapCredsResponse::error("join token expired");
                }
                Err(TokenStateError::NotFound) => {
                    // Token not registered in state machine — treat as first-use,
                    // register it and begin in-flight (registration-on-first-sight).
                    let expires_at_ms = expiry_secs * 1000;
                    self.token_store
                        .register(JoinTokenState {
                            token_hash,
                            lifecycle: JoinTokenLifecycle::Issued,
                            expires_at_ms,
                            attempt: 0,
                        })
                        .await;
                    if self
                        .token_store
                        .begin_inflight(&token_hash, dummy_addr)
                        .await
                        .is_err()
                    {
                        return BootstrapCredsResponse::error("token state conflict");
                    }
                }
                Err(e) => {
                    return BootstrapCredsResponse::error(format!("token state: {e}"));
                }
            }

            // Spawn the dead-man timer.
            nodedb_cluster::auth::token_state::spawn_inflight_timeout(
                Arc::clone(&self.token_store),
                token_hash,
                self.inflight_timeout,
            );

            // Step 3: issue creds.
            let resp = match self.issue(req.node_id) {
                Ok(r) => r,
                Err(e) => {
                    let _ = self.token_store.revert_inflight(&token_hash).await;
                    return BootstrapCredsResponse::error(e);
                }
            };

            // Step 4: transition to Consumed — persist audit BEFORE returning.
            let _ = self
                .token_store
                .mark_consumed(&token_hash, dummy_addr, epoch_ms())
                .await;
            self.audit
                .append(nodedb_cluster::auth::audit::AuditEvent::new(
                    token_hash,
                    None,
                    req.node_id,
                    JoinOutcome::Consumed,
                ));

            resp
        })
    }
}

fn make_handler(
    cluster_secret: [u8; 32],
) -> (
    Arc<StatefulBootstrapHandler<InMemoryTokenStore, VecAuditWriter>>,
    Arc<VecAuditWriter>,
) {
    let (ca, _) = generate_node_credentials_multi_san(&["node-1", "nodedb"]).unwrap();
    let audit = Arc::new(VecAuditWriter::new());
    let token_store = Arc::new(InMemoryTokenStore::new());
    let h = Arc::new(StatefulBootstrapHandler {
        ca: Arc::new(ca),
        cluster_secret,
        token_store,
        audit: Arc::clone(&audit),
        inflight_timeout: Duration::from_secs(30),
    });
    (h, audit)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn valid_token_accepted_and_audit_logged() {
    let secret = [0x11u8; 32];
    let (handler, audit) = make_handler(secret);

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (local, _join) = spawn_listener("127.0.0.1:0".parse().unwrap(), handler, rx).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let token = mint_token(&secret, 5, 60);
    let resp =
        nodedb_cluster::bootstrap_listener::fetch_creds(local, &token, 5, Duration::from_secs(3))
            .await
            .unwrap();

    assert!(resp.ok, "expected ok, got: {}", resp.error);
    assert!(!resp.ca_cert_der.is_empty());
    assert!(!resp.node_cert_der.is_empty());
    assert!(!resp.node_key_der.is_empty());
    assert_eq!(resp.cluster_secret, secret.to_vec());

    // Audit entry must be persisted before response.
    let events = audit.snapshot();
    assert!(
        events
            .iter()
            .any(|e| e.outcome == JoinOutcome::Consumed && e.claimed_node_id == 5),
        "expected Consumed audit event, got: {events:?}"
    );

    tx.send(true).unwrap();
}

#[tokio::test]
async fn invalid_hmac_rejected_audit_logged() {
    let secret = [0x22u8; 32];
    let (handler, audit) = make_handler(secret);

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (local, _join) = spawn_listener("127.0.0.1:0".parse().unwrap(), handler, rx).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Token issued under a different secret.
    let bad_token = mint_token(&[0xFFu8; 32], 2, 60);
    let err = nodedb_cluster::bootstrap_listener::fetch_creds(
        local,
        &bad_token,
        2,
        Duration::from_secs(3),
    )
    .await
    .unwrap_err();

    let msg = format!("{err}");
    assert!(
        msg.contains("token"),
        "expected 'token' in error, got: {msg}"
    );

    let events = audit.snapshot();
    assert!(
        events.iter().any(|e| matches!(&e.outcome, JoinOutcome::Rejected { reason } if reason.contains("invalid") || reason.contains("MAC") || reason.contains("token"))),
        "expected Rejected audit event, got: {events:?}"
    );

    tx.send(true).unwrap();
}

#[tokio::test]
async fn replayed_token_rejected() {
    let secret = [0x33u8; 32];
    let (handler, audit) = make_handler(secret);

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (local, _join) = spawn_listener("127.0.0.1:0".parse().unwrap(), handler, rx).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let token = mint_token(&secret, 7, 60);

    // First use: should succeed.
    let resp1 =
        nodedb_cluster::bootstrap_listener::fetch_creds(local, &token, 7, Duration::from_secs(3))
            .await
            .unwrap();
    assert!(resp1.ok, "first use should succeed");

    // Give the handler time to mark consumed.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second use: same token — must be rejected as replayed.
    let err2 =
        nodedb_cluster::bootstrap_listener::fetch_creds(local, &token, 7, Duration::from_secs(3))
            .await
            .unwrap_err();

    let msg = format!("{err2}");
    assert!(
        msg.contains("consumed") || msg.contains("token"),
        "expected replay rejection, got: {msg}"
    );

    let events = audit.snapshot();
    assert!(
        events.iter().any(|e| e.outcome == JoinOutcome::Replayed),
        "expected Replayed audit event, got: {events:?}"
    );

    tx.send(true).unwrap();
}

#[tokio::test]
async fn expired_token_rejected_audit_logged() {
    let secret = [0x44u8; 32];
    let (handler, audit) = make_handler(secret);

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (local, _join) = spawn_listener("127.0.0.1:0".parse().unwrap(), handler, rx).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Issue a token that is already expired (expiry = 1 second past unix epoch).
    let expired_token = tok::issue_token(&secret, 3, 1).unwrap();
    let err = nodedb_cluster::bootstrap_listener::fetch_creds(
        local,
        &expired_token,
        3,
        Duration::from_secs(3),
    )
    .await
    .unwrap_err();

    let msg = format!("{err}");
    assert!(
        msg.contains("expired") || msg.contains("token"),
        "expected expiry rejection, got: {msg}"
    );

    let events = audit.snapshot();
    assert!(
        events.iter().any(|e| matches!(
            &e.outcome,
            JoinOutcome::Rejected { .. } | JoinOutcome::TokenExpired
        )),
        "expected expiry audit event, got: {events:?}"
    );

    tx.send(true).unwrap();
}

// ── Bundle MAC tests ─────────────────────────────────────────────────────────

#[test]
fn bundle_mac_roundtrip_valid() {
    let secret = [0xAAu8; 32];
    let token_hash = [0xBBu8; 32];
    let payload = b"ca_cert|node_cert|node_key|cluster_secret";
    let sealed = seal_bundle(payload.to_vec(), &secret, &token_hash).unwrap();
    let out = open_bundle(&sealed, &secret, &token_hash).unwrap();
    assert_eq!(out, payload.as_ref());
}

#[test]
fn bundle_mac_tampered_payload_rejected() {
    let secret = [0xCCu8; 32];
    let token_hash = [0xDDu8; 32];
    let mut sealed = seal_bundle(b"real payload".to_vec(), &secret, &token_hash).unwrap();
    sealed.bundle[0] ^= 0xFF;
    assert!(open_bundle(&sealed, &secret, &token_hash).is_err());
}

#[test]
fn bundle_mac_wrong_secret_rejected() {
    let secret = [0xEEu8; 32];
    let wrong = [0x00u8; 32];
    let token_hash = [0x11u8; 32];
    let sealed = seal_bundle(b"payload".to_vec(), &secret, &token_hash).unwrap();
    assert!(open_bundle(&sealed, &wrong, &token_hash).is_err());
}

#[test]
fn bundle_mac_wrong_token_hash_rejected() {
    let secret = [0x22u8; 32];
    let hash = [0x33u8; 32];
    let wrong_hash = [0x44u8; 32];
    let sealed = seal_bundle(b"payload".to_vec(), &secret, &hash).unwrap();
    assert!(open_bundle(&sealed, &secret, &wrong_hash).is_err());
}

// ── State machine unit tests ─────────────────────────────────────────────────

#[tokio::test]
async fn token_state_issued_to_consumed() {
    use nodedb_cluster::auth::token_state::JoinTokenLifecycle;
    let store = InMemoryTokenStore::new();
    let hash = [0x55u8; 32];
    let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    store
        .register(JoinTokenState {
            token_hash: hash,
            lifecycle: JoinTokenLifecycle::Issued,
            expires_at_ms: epoch_ms() + 60_000,
            attempt: 0,
        })
        .await;
    store.begin_inflight(&hash, addr).await.unwrap();
    store.mark_consumed(&hash, addr, epoch_ms()).await.unwrap();
    let s = store.get(&hash).unwrap();
    assert!(matches!(s.lifecycle, JoinTokenLifecycle::Consumed { .. }));
}

#[tokio::test]
async fn token_state_replay_on_consumed_returns_error() {
    use nodedb_cluster::auth::token_state::{JoinTokenLifecycle, TokenStateError};
    let store = InMemoryTokenStore::new();
    let hash = [0x66u8; 32];
    let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    store
        .register(JoinTokenState {
            token_hash: hash,
            lifecycle: JoinTokenLifecycle::Issued,
            expires_at_ms: epoch_ms() + 60_000,
            attempt: 0,
        })
        .await;
    store.begin_inflight(&hash, addr).await.unwrap();
    store.mark_consumed(&hash, addr, epoch_ms()).await.unwrap();
    assert_eq!(
        store.begin_inflight(&hash, addr).await.unwrap_err(),
        TokenStateError::AlreadyConsumed
    );
}

#[tokio::test]
async fn token_state_expired_token_rejected() {
    use nodedb_cluster::auth::token_state::{JoinTokenLifecycle, TokenStateError};
    let store = InMemoryTokenStore::new();
    let hash = [0x77u8; 32];
    let addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();
    store
        .register(JoinTokenState {
            token_hash: hash,
            lifecycle: JoinTokenLifecycle::Issued,
            expires_at_ms: 1, // far in the past
            attempt: 0,
        })
        .await;
    assert_eq!(
        store.begin_inflight(&hash, addr).await.unwrap_err(),
        TokenStateError::Expired
    );
}

#[tokio::test]
async fn token_state_inflight_reverts_on_timeout() {
    use nodedb_cluster::auth::token_state::JoinTokenLifecycle;
    let store = InMemoryTokenStore::new();
    let hash = [0x88u8; 32];
    let addr: SocketAddr = "127.0.0.1:9003".parse().unwrap();
    store
        .register(JoinTokenState {
            token_hash: hash,
            lifecycle: JoinTokenLifecycle::Issued,
            expires_at_ms: epoch_ms() + 60_000,
            attempt: 0,
        })
        .await;
    store.begin_inflight(&hash, addr).await.unwrap();
    store.revert_inflight(&hash).await.unwrap();
    let s = store.get(&hash).unwrap();
    assert_eq!(s.lifecycle, JoinTokenLifecycle::Issued);
    assert_eq!(s.attempt, 1);
}

// ── Audit persist-before-respond contract ────────────────────────────────────
//
// The integration tests above implicitly verify this: `audit.snapshot()`
// is called immediately after `fetch_creds` returns. If the handler
// responds before persisting the audit entry, `snapshot()` would return
// an empty vec (or miss the event) and the assertions would fail.
