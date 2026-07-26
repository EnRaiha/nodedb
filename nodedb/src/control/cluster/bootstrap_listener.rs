// SPDX-License-Identifier: BUSL-1.1

//! Host-side glue for the cluster bootstrap listener (L.4).
//!
//! Wires `nodedb-cluster`'s transport-only listener to the local
//! node's loaded CA + cluster secret so it can verify join tokens
//! and mint per-node leaf certs.

use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use nodedb_cluster::bootstrap_listener::{
    BootstrapCredsRequest, BootstrapCredsResponse, BootstrapHandler,
};

use aes_gcm::aead::{Aead as _, KeyInit as _, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use nodedb_cluster::{
    GroupAppliedWatchers, JoinTokenLifecycle, JoinTokenState, METADATA_GROUP_ID, MetadataEntry,
    RaftBackedTokenStore, TokenStateBackend, TokenStateError, WaitOutcome, encode_entry,
    token_hash, verify_token,
};
use sha2::Sha256;

type HostRaftLoop = nodedb_cluster::RaftLoop<
    crate::control::cluster::SpscCommitApplier,
    crate::control::LocalPlanExecutor,
>;

/// Async metadata proposer used only by durable bootstrap token state.
pub(crate) struct BootstrapMetadataProposer {
    raft_loop: Weak<HostRaftLoop>,
    watchers: Arc<GroupAppliedWatchers>,
}

impl BootstrapMetadataProposer {
    pub(crate) fn new(raft_loop: &Arc<HostRaftLoop>, watchers: Arc<GroupAppliedWatchers>) -> Self {
        Self {
            raft_loop: Arc::downgrade(raft_loop),
            watchers,
        }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::decommission::MetadataProposer for BootstrapMetadataProposer {
    async fn propose_and_wait(&self, entry: MetadataEntry) -> nodedb_cluster::error::Result<u64> {
        let raft_loop =
            self.raft_loop
                .upgrade()
                .ok_or_else(|| nodedb_cluster::ClusterError::Transport {
                    detail: "bootstrap metadata proposer: cluster is stopping".into(),
                })?;
        let bytes = encode_entry(&entry).map_err(|error| nodedb_cluster::ClusterError::Codec {
            detail: format!("bootstrap metadata entry: {error}"),
        })?;
        let index = raft_loop
            .propose_to_metadata_group_via_leader(bytes)
            .await?;
        let watchers = Arc::clone(&self.watchers);
        let outcome = tokio::task::spawn_blocking(move || {
            watchers.wait_for(METADATA_GROUP_ID, index, Duration::from_secs(10))
        })
        .await
        .map_err(|error| nodedb_cluster::ClusterError::Transport {
            detail: format!("bootstrap metadata apply wait failed: {error}"),
        })?;
        match outcome {
            WaitOutcome::Reached => Ok(index),
            WaitOutcome::TimedOut => Err(nodedb_cluster::ClusterError::Transport {
                detail: format!("bootstrap metadata apply timed out at index {index}"),
            }),
            WaitOutcome::GroupGone => Err(nodedb_cluster::ClusterError::GroupNotFound {
                group_id: METADATA_GROUP_ID,
            }),
        }
    }
}

/// Binds the local node's TLS material (CA key + cluster secret)
/// to the generic listener handler. Constructed in `main.rs` once
/// the node has loaded creds; passed into `spawn_listener`.
pub struct HostBootstrapHandler<B: TokenStateBackend> {
    /// Persisted CA used to issue leaf certs for new joiners. Holds
    /// the key pair via `ClusterCa::from_der`.
    ca: Arc<nexar::transport::tls::ClusterCa>,
    /// 32-byte HMAC key used to verify join tokens.
    cluster_secret: [u8; 32],
    /// SPKI of the node certificate terminating this bootstrap listener.
    issuer_spki: [u8; 32],
    token_store: Arc<B>,
    enrollment_transport: Option<Arc<nodedb_cluster::NexarTransport>>,
    metadata_proposer: Option<Arc<dyn nodedb_cluster::decommission::MetadataProposer>>,
}

const MAX_ENROLLMENT_PREAUTH_TTL: std::time::Duration = std::time::Duration::from_secs(15 * 60);

impl<B: TokenStateBackend> HostBootstrapHandler<B> {
    pub fn new(
        ca: Arc<nexar::transport::tls::ClusterCa>,
        cluster_secret: [u8; 32],
        issuer_spki: [u8; 32],
        token_store: Arc<B>,
        enrollment_transport: Option<Arc<nodedb_cluster::NexarTransport>>,
        metadata_proposer: Option<Arc<dyn nodedb_cluster::decommission::MetadataProposer>>,
    ) -> Self {
        Self {
            ca,
            cluster_secret,
            issuer_spki,
            token_store,
            enrollment_transport,
            metadata_proposer,
        }
    }

    fn issue(
        &self,
        node_id: u64,
        preauthorization_ttl: std::time::Duration,
    ) -> crate::Result<(BootstrapCredsResponse, [u8; 32])> {
        let node_san = format!("node-{node_id}");
        let enrollment_expires_at_ms = epoch_ms()
            .saturating_add(u64::try_from(preauthorization_ttl.as_millis()).unwrap_or(u64::MAX));
        let enrollment_san = format!("nodedb-enrollment-{node_id}-{enrollment_expires_at_ms}");
        let creds = nodedb_cluster::issue_leaf_for_sans(
            &self.ca,
            &[
                &node_san,
                &enrollment_san,
                nodedb_cluster::transport::config::SNI_HOSTNAME,
            ],
        )
        .map_err(|e| crate::Error::Config {
            detail: format!("issue leaf: {e}"),
        })?;
        if let Some(transport) = &self.enrollment_transport
            && !transport.preauthorize_peer_identity(creds.spki_pin, preauthorization_ttl)
        {
            return Err(crate::Error::Config {
                detail: "bootstrap enrollment preauthorization capacity exhausted".into(),
            });
        }
        // Preserve the cluster's *existing* secret. `issue_leaf_for_sans`
        // generates a fresh one for the returned bundle; overwrite so
        // the joiner shares the same MAC key as the rest of the cluster.
        Ok((
            BootstrapCredsResponse {
                ok: true,
                error: String::new(),
                ca_cert_der: self.ca.cert_der().to_vec(),
                node_cert_der: creds.cert.to_vec(),
                node_key_der: creds.key.secret_der().to_vec(),
                cluster_secret: self.cluster_secret.to_vec(),
            },
            creds.spki_pin,
        ))
    }
}

impl<B: TokenStateBackend> BootstrapHandler for HostBootstrapHandler<B> {
    fn handle<'a>(
        &'a self,
        req: BootstrapCredsRequest,
        remote_addr: SocketAddr,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BootstrapCredsResponse> + Send + 'a>>
    {
        Box::pin(async move {
            let verified = match verify_token(&req.token_hex, &self.cluster_secret) {
                Ok(value) => value,
                Err(e) => return BootstrapCredsResponse::error(format!("token: {e}")),
            };
            // verify_token uses constant-time MAC comparison via hmac::Mac::verify_slice.
            if verified.for_node != req.node_id {
                return BootstrapCredsResponse::error(format!(
                    "node id mismatch: token bound to {}, request claims {}",
                    verified.for_node, req.node_id
                ));
            }
            if verified.bootstrap_ca_der.as_slice() != self.ca.cert_der().as_ref()
                || verified.bootstrap_issuer_spki != self.issuer_spki
            {
                return BootstrapCredsResponse::error(
                    "token bootstrap CA does not match this cluster",
                );
            }
            let expiry_secs = verified.expiry_unix_secs;
            let hash = match token_hash(&req.token_hex) {
                Ok(hash) => hash,
                Err(error) => return BootstrapCredsResponse::error(format!("token: {error}")),
            };
            self.token_store
                .register(JoinTokenState {
                    token_hash: hash,
                    lifecycle: JoinTokenLifecycle::Issued,
                    expires_at_ms: expiry_secs.saturating_mul(1_000),
                    attempt: 0,
                })
                .await;
            let lease = match self.token_store.begin_inflight(&hash, remote_addr).await {
                Ok(lease) => lease,
                Err(TokenStateError::AlreadyConsumed) => {
                    let Some(JoinTokenState {
                        lifecycle:
                            JoinTokenLifecycle::Consumed {
                                recovery_bundle, ..
                            },
                        ..
                    }) = self.token_store.get(&hash)
                    else {
                        return BootstrapCredsResponse::error(
                            "consumed token has no recoverable credential bundle",
                        );
                    };
                    return decrypt_recovery_bundle(&self.cluster_secret, &hash, &recovery_bundle)
                        .unwrap_or_else(BootstrapCredsResponse::error);
                }
                Err(error) => {
                    return BootstrapCredsResponse::error(format!("token state: {error}"));
                }
            };
            let remaining =
                std::time::Duration::from_secs(expiry_secs.saturating_sub(epoch_ms() / 1_000))
                    .min(MAX_ENROLLMENT_PREAUTH_TTL);
            if remaining.is_zero() {
                let _ = self.token_store.revert_inflight(&hash, lease).await;
                return BootstrapCredsResponse::error("token has no enrollment lifetime remaining");
            }
            let (response, preauthorized_pin) = match self.issue(req.node_id, remaining) {
                Ok(issued) => issued,
                Err(error) => {
                    let _ = self.token_store.revert_inflight(&hash, lease).await;
                    return BootstrapCredsResponse::error(error.to_string());
                }
            };
            // Build the recoverable credential ciphertext before publishing the
            // durable enrollment authorization. If encryption fails, there is
            // no replicated authorization to compensate or rehydrate.
            let recovery_bundle =
                match encrypt_recovery_bundle(&self.cluster_secret, &hash, &response) {
                    Ok(bundle) => bundle,
                    Err(error) => {
                        if let Some(transport) = &self.enrollment_transport {
                            transport.revoke_peer_preauthorization(&preauthorized_pin, remaining);
                        }
                        let revert = self.token_store.revert_inflight(&hash, lease).await;
                        return BootstrapCredsResponse::error(format!(
                            "{error}; revert={revert:?}"
                        ));
                    }
                };
            let enrollment_expires_at_ms = expiry_secs
                .saturating_mul(1_000)
                .min(epoch_ms().saturating_add(remaining.as_millis() as u64));
            if let Some(proposer) = &self.metadata_proposer
                && let Err(error) = proposer
                    .propose_and_wait(MetadataEntry::EnrollmentPreauthorization {
                        spki: preauthorized_pin,
                        expires_at_ms: enrollment_expires_at_ms,
                    })
                    .await
            {
                if let Some(transport) = &self.enrollment_transport {
                    transport.revoke_peer_preauthorization(&preauthorized_pin, remaining);
                }
                let revoke = proposer
                    .propose_and_wait(MetadataEntry::EnrollmentPreauthorizationRevoke {
                        spki: preauthorized_pin,
                        expires_at_ms: enrollment_expires_at_ms,
                    })
                    .await;
                let revert = self.token_store.revert_inflight(&hash, lease).await;
                return BootstrapCredsResponse::error(format!(
                    "enrollment preauthorization: {error}; revoke={revoke:?}; revert={revert:?}"
                ));
            }
            if let Err(error) = self
                .token_store
                .mark_consumed(&hash, remote_addr, lease, epoch_ms(), recovery_bundle)
                .await
            {
                // The proposal result is indeterminate: it may commit after the
                // local apply wait times out. Keep the bounded preauthorization
                // so a later retry can decrypt the Raft-persisted bundle and use
                // the exact certificate whose token became Consumed.
                tracing::warn!(
                    token_hash = ?hash,
                    ?preauthorized_pin,
                    %error,
                    "bootstrap token consumption indeterminate; retaining recoverable bundle authorization"
                );
                let revert = self.token_store.revert_inflight(&hash, lease).await;
                return BootstrapCredsResponse::error(format!(
                    "consume token before credential delivery: {error}; revert={revert:?}"
                ));
            }
            response
        })
    }

    fn confirm_delivery<'a>(
        &'a self,
        _req: &'a BootstrapCredsRequest,
        _response: &'a BootstrapCredsResponse,
        _remote_addr: SocketAddr,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = nodedb_cluster::error::Result<()>> + Send + 'a>,
    > {
        Box::pin(async { Ok(()) })
    }

    fn abort_delivery<'a>(
        &'a self,
        _req: &'a BootstrapCredsRequest,
        _response: &'a BootstrapCredsResponse,
        remote_addr: SocketAddr,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Credential bytes may already be observable when the delivery ACK
            // is lost. Keep the bounded enrollment authorization alive until
            // topology commit or certificate expiry; revoking here would
            // strand a joiner whose token is already durably consumed.
            tracing::warn!(%remote_addr, "bootstrap delivery ACK missing; retaining bounded enrollment authorization");
        })
    }
}

fn recovery_key(cluster_secret: &[u8; 32]) -> Result<[u8; 32], String> {
    let mut key = [0u8; 32];
    Hkdf::<Sha256>::new(Some(b"nodedb-bootstrap-recovery-v1"), cluster_secret)
        .expand(b"credential-response", &mut key)
        .map_err(|_| "bootstrap recovery key derivation failed".to_string())?;
    Ok(key)
}

fn encrypt_recovery_bundle(
    cluster_secret: &[u8; 32],
    token_hash: &[u8; 32],
    response: &BootstrapCredsResponse,
) -> Result<Vec<u8>, String> {
    let plaintext = zerompk::to_msgpack_vec(response)
        .map_err(|error| format!("encode bootstrap recovery bundle: {error}"))?;
    let mut nonce = [0u8; 12];
    getrandom::fill(&mut nonce)
        .map_err(|error| format!("bootstrap recovery nonce generation failed: {error}"))?;
    let cipher = Aes256Gcm::new_from_slice(&recovery_key(cluster_secret)?)
        .map_err(|_| "bootstrap recovery cipher initialization failed".to_string())?;
    let nonce_value = Nonce::from(nonce);
    let ciphertext = cipher
        .encrypt(
            &nonce_value,
            Payload {
                msg: &plaintext,
                aad: token_hash,
            },
        )
        .map_err(|_| "bootstrap recovery bundle encryption failed".to_string())?;
    let mut encoded = Vec::with_capacity(nonce.len() + ciphertext.len());
    encoded.extend_from_slice(&nonce);
    encoded.extend_from_slice(&ciphertext);
    Ok(encoded)
}

fn decrypt_recovery_bundle(
    cluster_secret: &[u8; 32],
    token_hash: &[u8; 32],
    encoded: &[u8],
) -> Result<BootstrapCredsResponse, String> {
    let (nonce, ciphertext) = encoded
        .split_at_checked(12)
        .ok_or_else(|| "bootstrap recovery bundle is truncated".to_string())?;
    let cipher = Aes256Gcm::new_from_slice(&recovery_key(cluster_secret)?)
        .map_err(|_| "bootstrap recovery cipher initialization failed".to_string())?;
    let nonce_array: [u8; 12] = nonce
        .try_into()
        .map_err(|_| "bootstrap recovery nonce is malformed".to_string())?;
    let nonce_value = Nonce::from(nonce_array);
    let plaintext = cipher
        .decrypt(
            &nonce_value,
            Payload {
                msg: ciphertext,
                aad: token_hash,
            },
        )
        .map_err(|_| "bootstrap recovery bundle authentication failed".to_string())?;
    zerompk::from_msgpack(&plaintext)
        .map_err(|error| format!("decode bootstrap recovery bundle: {error}"))
}

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Durable enrollment dependencies shared by token consumption and transport
/// preauthorization during bootstrap credential delivery.
pub struct BootstrapEnrollment {
    pub token_store: Arc<RaftBackedTokenStore>,
    pub transport: Arc<nodedb_cluster::NexarTransport>,
    pub metadata_proposer: Arc<dyn nodedb_cluster::decommission::MetadataProposer>,
}

struct EnrollmentPublication {
    transport: Option<Arc<nodedb_cluster::NexarTransport>>,
    metadata_proposer: Option<Arc<dyn nodedb_cluster::decommission::MetadataProposer>>,
}

/// Convenience: spawn the listener with a handler built from the
/// node's loaded `TlsCredentials` + a reloaded `ClusterCa`.
pub(crate) fn spawn(
    listen_addr: SocketAddr,
    issuer: &crate::control::cluster::tls::BootstrapIssuerMaterial,
    enrollment: BootstrapEnrollment,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> crate::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    spawn_with_store(
        listen_addr,
        issuer,
        enrollment.token_store,
        EnrollmentPublication {
            transport: Some(enrollment.transport),
            metadata_proposer: Some(enrollment.metadata_proposer),
        },
        shutdown,
    )
}

fn spawn_with_store<B: TokenStateBackend>(
    listen_addr: SocketAddr,
    issuer: &crate::control::cluster::tls::BootstrapIssuerMaterial,
    token_store: Arc<B>,
    publication: EnrollmentPublication,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> crate::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let ca = nexar::transport::tls::ClusterCa::from_der(&issuer.ca_key_der, &issuer.ca_cert)
        .map_err(|e| crate::Error::Config {
            detail: format!("bootstrap listener: load CA: {e}"),
        })?;
    let ca = Arc::new(ca);
    let issuer_spki =
        nodedb_cluster::spki_pin_from_cert_der(issuer.node_cert.as_ref()).map_err(|e| {
            crate::Error::Config {
                detail: format!("bootstrap listener issuer SPKI: {e}"),
            }
        })?;
    let handler = Arc::new(HostBootstrapHandler::new(
        Arc::clone(&ca),
        issuer.cluster_secret,
        issuer_spki,
        token_store,
        publication.transport,
        publication.metadata_proposer,
    ));
    let issuer_cert = issuer.node_cert.clone();
    let issuer_key =
        nodedb_cluster::transport::pki_types::PrivatePkcs8KeyDer::from(issuer.node_key_der.clone())
            .into();
    nodedb_cluster::bootstrap_listener::spawn_listener(
        listen_addr,
        issuer_cert,
        issuer_key,
        handler,
        shutdown,
    )
    .map_err(|e| crate::Error::Config {
        detail: format!("bootstrap listener spawn: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nodedb_cluster::InMemoryTokenStore;

    fn memory_store() -> Arc<InMemoryTokenStore> {
        Arc::new(InMemoryTokenStore::new())
    }

    fn issuer_material(
        ca: &nexar::transport::tls::ClusterCa,
        credentials: &nodedb_cluster::TlsCredentials,
        cluster_secret: [u8; 32],
    ) -> crate::control::cluster::tls::BootstrapIssuerMaterial {
        crate::control::cluster::tls::BootstrapIssuerMaterial {
            ca_cert: ca.cert_der(),
            ca_key_der: ca.key_pair_pkcs8_der(),
            node_cert: credentials.cert.clone(),
            node_key_der: credentials.key.secret_der().to_vec(),
            cluster_secret,
        }
    }

    fn mint_token(
        secret: &[u8; 32],
        for_node: u64,
        ttl_secs: u64,
        issuer_spki: [u8; 32],
        ca_der: &[u8],
    ) -> String {
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + ttl_secs;
        nodedb_cluster::issue_token(secret, for_node, expiry, issuer_spki, ca_der).unwrap()
    }

    #[tokio::test]
    async fn end_to_end_fetch_creds_roundtrip() {
        // 1. Bootstrap a local CA on the "server" side.
        let (ca, creds) =
            nodedb_cluster::generate_node_credentials_multi_san(&["node-1", "nodedb"]).unwrap();
        let ca_cert = ca.cert_der();
        let issuer_spki = creds.spki_pin;
        let cluster_secret = [0x42u8; 32];
        let issuer = issuer_material(&ca, &creds, cluster_secret);

        // 2. Spawn the listener.
        let (tx, rx) = tokio::sync::watch::channel(false);
        let (local, join) = spawn_with_store(
            "127.0.0.1:0".parse().unwrap(),
            &issuer,
            memory_store(),
            EnrollmentPublication {
                transport: None,
                metadata_proposer: None,
            },
            rx,
        )
        .unwrap();

        // Give the listener a moment to start accepting.
        tokio::time::sleep(Duration::from_millis(30)).await;

        // 3. Mint a fresh token for node 7 and fetch creds.
        let token = mint_token(&cluster_secret, 7, 60, issuer_spki, ca_cert.as_ref());
        let resp = nodedb_cluster::bootstrap_listener::fetch_creds(
            local,
            &token,
            7,
            Duration::from_secs(3),
        )
        .await
        .unwrap();

        assert!(resp.ok);
        assert!(!resp.ca_cert_der.is_empty());
        assert!(!resp.node_cert_der.is_empty());
        assert!(!resp.node_key_der.is_empty());
        assert_eq!(resp.cluster_secret, cluster_secret.to_vec());
        // Delivered CA cert matches the server's.
        assert_eq!(resp.ca_cert_der, ca_cert.as_ref().to_vec());

        let recovered = nodedb_cluster::bootstrap_listener::fetch_creds(
            local,
            &token,
            7,
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        assert_eq!(recovered.node_cert_der, resp.node_cert_der);
        assert_eq!(recovered.node_key_der, resp.node_key_der);

        // 4. Shutdown.
        tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), join).await;
    }

    #[tokio::test]
    async fn credential_response_consumes_token_before_delivery_ack() {
        let (ca, creds) =
            nodedb_cluster::generate_node_credentials_multi_san(&["node-1", "nodedb"]).unwrap();
        let cluster_secret = [0x43u8; 32];
        let ca_cert = ca.cert_der();
        let issuer_spki = creds.spki_pin;
        let store = memory_store();
        let handler = HostBootstrapHandler::new(
            Arc::new(ca),
            cluster_secret,
            issuer_spki,
            Arc::clone(&store),
            None,
            None,
        );
        let token = mint_token(&cluster_secret, 8, 60, issuer_spki, ca_cert.as_ref());
        let request = BootstrapCredsRequest {
            token_hex: token,
            node_id: 8,
        };
        let remote: SocketAddr = "127.0.0.1:34008".parse().unwrap();

        let delivered = handler.handle(request.clone(), remote).await;
        assert!(delivered.ok);
        let recovered = handler.handle(request, remote).await;
        assert!(recovered.ok);
        assert_eq!(recovered.node_cert_der, delivered.node_cert_der);
        assert_eq!(recovered.node_key_der, delivered.node_key_der);
    }

    #[tokio::test]
    async fn rejects_bad_token() {
        let (ca, creds) = nodedb_cluster::generate_node_credentials_multi_san(&["nodedb"]).unwrap();
        let ca_cert = ca.cert_der();
        let issuer_spki = creds.spki_pin;
        let cluster_secret = [0x55u8; 32];
        let issuer = issuer_material(&ca, &creds, cluster_secret);

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (local, join) = spawn_with_store(
            "127.0.0.1:0".parse().unwrap(),
            &issuer,
            memory_store(),
            EnrollmentPublication {
                transport: None,
                metadata_proposer: None,
            },
            rx,
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Wrong secret → invalid MAC.
        let bad_token = mint_token(&[0xAAu8; 32], 1, 60, issuer_spki, ca_cert.as_ref());
        let err = nodedb_cluster::bootstrap_listener::fetch_creds(
            local,
            &bad_token,
            1,
            Duration::from_secs(3),
        )
        .await
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("token"), "got: {msg}");

        tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), join).await;
    }

    #[tokio::test]
    async fn rejects_node_id_mismatch() {
        let (ca, creds) = nodedb_cluster::generate_node_credentials_multi_san(&["nodedb"]).unwrap();
        let ca_cert = ca.cert_der();
        let issuer_spki = creds.spki_pin;
        let cluster_secret = [0x99u8; 32];
        let issuer = issuer_material(&ca, &creds, cluster_secret);

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (local, join) = spawn_with_store(
            "127.0.0.1:0".parse().unwrap(),
            &issuer,
            memory_store(),
            EnrollmentPublication {
                transport: None,
                metadata_proposer: None,
            },
            rx,
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Token for node 2, request claims node 3.
        let token = mint_token(&cluster_secret, 2, 60, issuer_spki, ca_cert.as_ref());
        let err = nodedb_cluster::bootstrap_listener::fetch_creds(
            local,
            &token,
            3,
            Duration::from_secs(3),
        )
        .await
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("node id mismatch"), "got: {msg}");

        tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), join).await;
    }
}
