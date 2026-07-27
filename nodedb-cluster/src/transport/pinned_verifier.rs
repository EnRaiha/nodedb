// SPDX-License-Identifier: BUSL-1.1

//! TLS-layer SPKI/SPIFFE pinning verifiers for inbound and outbound mTLS.
//!
//! Both [`PinnedClientVerifier`] (server-side, inbound) and
//! [`PinnedServerVerifier`] (client-side, outbound) wrap the standard
//! WebPki verifier and add a second layer of cert-identity pinning against
//! the cluster topology.
//!
//! # Security contract
//!
//! 1. The inner WebPki verifier runs **first** — chain building, expiry check,
//!    and CRL revocation are validated before the pin check fires.  The pin
//!    check is strictly additive; it never bypasses standard X.509 validation.
//! 2. If the verified leaf cert's SPIFFE id or SPKI fingerprint matches a
//!    topology entry, the handshake is accepted.
//! 3. An unknown cert is accepted only before initial topology installation or
//!    when its SPKI was explicitly preauthorized by credential issuance.
//! 4. After topology installation, every other unknown or mismatched identity
//!    is rejected with `rustls::Error::InvalidCertificate`.

use std::sync::Arc;

use tracing::{debug, warn};

use crate::transport::peer_identity_store::PeerIdentityStore;
use crate::transport::peer_identity_verifier::{spiffe_id_from_cert_der, spki_pin_from_cert_der};

// ──────────────────────────────────────────────────────────────────────────────
// Inbound verifier (server-side client-cert check)
// ──────────────────────────────────────────────────────────────────────────────

/// Inbound (server-side) client-cert verifier that layers SPKI/SPIFFE pinning
/// on top of the standard WebPki chain + CRL validation.
pub struct PinnedClientVerifier {
    pub(crate) inner: Arc<dyn rustls::server::danger::ClientCertVerifier>,
    pub(crate) identity_store: Arc<dyn PeerIdentityStore>,
}

impl std::fmt::Debug for PinnedClientVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedClientVerifier")
            .finish_non_exhaustive()
    }
}

impl rustls::server::danger::ClientCertVerifier for PinnedClientVerifier {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        // Step 1: standard chain + revocation + expiry (MUST run first).
        self.inner
            .verify_client_cert(end_entity, intermediates, now)?;

        // Step 2: topology SPKI/SPIFFE pin check.
        check_cert_pin(end_entity.as_ref(), &*self.identity_store)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Outbound verifier (client-side server-cert check)
// ──────────────────────────────────────────────────────────────────────────────

/// Outbound (client-side) server-cert verifier that layers SPKI/SPIFFE pinning
/// on top of standard WebPki chain validation.
pub struct PinnedServerVerifier {
    pub(crate) inner: Arc<dyn rustls::client::danger::ServerCertVerifier>,
    pub(crate) identity_store: Arc<dyn PeerIdentityStore>,
}

impl std::fmt::Debug for PinnedServerVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedServerVerifier")
            .finish_non_exhaustive()
    }
}

impl rustls::client::danger::ServerCertVerifier for PinnedServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Step 1: standard chain + expiry + OCSP (MUST run first).
        self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;

        // Step 2: topology SPKI/SPIFFE pin check.
        check_cert_pin(end_entity.as_ref(), &*self.identity_store)?;
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Shared pin-check logic
// ──────────────────────────────────────────────────────────────────────────────

/// Shared SPKI/SPIFFE topology pin check used by both verifiers.
///
/// Returns `Ok(ClientCertVerified::assertion())` when:
/// - No topology entry exists for this cert (bootstrap window — accept), or
/// - The cert's SPIFFE id or SPKI fingerprint matches the topology entry.
///
/// Returns `Err(InvalidCertificate(Other))` when:
/// - A topology entry with a SPIFFE match exists but the SPKI diverges
///   (guard against a cert borrowing a known SPIFFE id with a different key).
///
/// Note: the return type uses `ClientCertVerified` for both callers. The
/// outbound (`PinnedServerVerifier`) caller discards the value and wraps the
/// result in `ServerCertVerified::assertion()` on success.
pub(crate) fn check_cert_pin(
    cert_der: &[u8],
    store: &dyn PeerIdentityStore,
) -> std::result::Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
    // Extract SPKI fingerprint from the cert; failure → reject immediately.
    let spki = spki_pin_from_cert_der(cert_der).map_err(|_| {
        rustls::Error::InvalidCertificate(rustls::CertificateError::Other(rustls::OtherError(
            Arc::new(PinCheckError(
                "failed to extract SPKI pin from peer cert".into(),
            )),
        )))
    })?;

    // Check SPIFFE first (preferred when available on both sides).
    if let Some(peer_spiffe) = spiffe_id_from_cert_der(cert_der)
        && let Some(node) = store.find_by_spiffe(&peer_spiffe)
    {
        // Topology has an entry keyed to this SPIFFE id — verify SPKI also
        // matches to guard against a cert with a borrowed SPIFFE id but a
        // different key-pair.
        if node.spki_pin.is_none_or(|pinned| pinned == spki) {
            debug!(spiffe = %peer_spiffe, "TLS pin check: accepted via SPIFFE");
            return Ok(rustls::server::danger::ClientCertVerified::assertion());
        }
        warn!(
            spiffe = %peer_spiffe,
            "TLS pin check: SPIFFE matched topology entry but SPKI mismatch — rejecting"
        );
        return Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::Other(rustls::OtherError(Arc::new(PinCheckError(
                "SPIFFE/SPKI binding mismatch".into(),
            )))),
        ));
    }

    // SPKI lookup.
    if store.find_by_spki(&spki).is_some() {
        debug!("TLS pin check: accepted via SPKI fingerprint");
        return Ok(rustls::server::danger::ClientCertVerified::assertion());
    }

    if let Some((_node_id, expires_at_ms)) =
        super::peer_identity_verifier::enrollment_identity_from_cert_der(cert_der)
    {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(u64::MAX);
        let remaining = expires_at_ms.saturating_sub(now_ms);
        if remaining > 0
            && remaining <= 15 * 60 * 1_000
            && !store.is_enrollment_revoked(&spki)
            && store.is_preauthorized(&spki)
        {
            debug!("TLS pin check: accepted explicitly preauthorized enrollment identity");
            return Ok(rustls::server::danger::ClientCertVerified::assertion());
        }
    }
    if store.bootstrap_window_open() {
        warn!("TLS pin check: accepting initial seed while topology is not yet installed");
        return Ok(rustls::server::danger::ClientCertVerified::assertion());
    }

    warn!("TLS pin check: rejecting unknown cert after bootstrap closure");
    Err(rustls::Error::InvalidCertificate(
        rustls::CertificateError::Other(rustls::OtherError(Arc::new(PinCheckError(
            "peer identity is neither topology-pinned nor preauthorized".into(),
        )))),
    ))
}

/// Opaque error type wrapped by `rustls::OtherError` for pin-check failures.
#[derive(Debug)]
struct PinCheckError(String);

impl std::fmt::Display for PinCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PinCheckError {}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::RwLock;

    use super::*;
    use crate::topology::{NodeInfo, NodeState};
    use crate::transport::peer_identity_store::PeerIdentityStore;
    use crate::transport::peer_identity_verifier::spki_pin_from_cert_der;

    // ── Test identity store ──────────────────────────────────────────────────

    /// In-memory identity store backed by a `RwLock<HashMap>`.
    struct MapIdentityStore {
        by_id: RwLock<HashMap<u64, NodeInfo>>,
    }

    impl MapIdentityStore {
        fn new() -> Self {
            Self {
                by_id: RwLock::new(HashMap::new()),
            }
        }

        fn insert(&self, info: NodeInfo) {
            self.by_id.write().unwrap().insert(info.node_id, info);
        }
    }

    impl PeerIdentityStore for MapIdentityStore {
        fn get_node_info(&self, node_id: u64) -> Option<NodeInfo> {
            self.by_id.read().unwrap().get(&node_id).cloned()
        }

        fn find_by_spki(&self, spki: &[u8; 32]) -> Option<NodeInfo> {
            self.by_id
                .read()
                .unwrap()
                .values()
                .find(|n| n.spki_pin.as_ref() == Some(spki))
                .cloned()
        }

        fn find_by_spiffe(&self, spiffe_id: &str) -> Option<NodeInfo> {
            self.by_id
                .read()
                .unwrap()
                .values()
                .find(|n| n.spiffe_id.as_deref() == Some(spiffe_id))
                .cloned()
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn addr() -> SocketAddr {
        "127.0.0.1:9400".parse().unwrap()
    }

    fn node_with_spki(node_id: u64, pin: [u8; 32]) -> NodeInfo {
        let mut n = NodeInfo::new(node_id, addr(), NodeState::Active);
        n.spki_pin = Some(pin);
        n
    }

    fn node_with_spiffe(node_id: u64, spiffe: &str, pin: Option<[u8; 32]>) -> NodeInfo {
        let mut n = NodeInfo::new(node_id, addr(), NodeState::Active);
        n.spiffe_id = Some(spiffe.to_string());
        n.spki_pin = pin;
        n
    }

    fn gen_cert_and_pin() -> (Vec<u8>, [u8; 32]) {
        use rcgen::{CertificateParams, KeyPair};
        let key = KeyPair::generate().unwrap();
        let params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let cert_der = cert.der().to_vec();
        let pin = spki_pin_from_cert_der(&cert_der).unwrap();
        (cert_der, pin)
    }

    fn gen_enrollment_cert(expires_at_ms: u64) -> Vec<u8> {
        use rcgen::{CertificateParams, KeyPair};
        let key = KeyPair::generate().unwrap();
        let marker = format!("nodedb-enrollment-7-{expires_at_ms}");
        let params = CertificateParams::new(vec!["localhost".to_string(), marker]).unwrap();
        params.self_signed(&key).unwrap().der().to_vec()
    }

    fn gen_cert_with_spiffe(spiffe_uri: &str) -> (Vec<u8>, [u8; 32]) {
        use rcgen::{CertificateParams, Ia5String, KeyPair, SanType};
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let uri = Ia5String::try_from(spiffe_uri.to_string()).unwrap();
        params.subject_alt_names.push(SanType::URI(uri));
        let cert = params.self_signed(&key).unwrap();
        let cert_der = cert.der().to_vec();
        let pin = spki_pin_from_cert_der(&cert_der).unwrap();
        (cert_der, pin)
    }

    // ── check_cert_pin unit tests ────────────────────────────────────────────

    /// A cert whose SPKI is pinned in the topology must be accepted.
    #[test]
    fn known_spki_accepted() {
        let (cert_der, pin) = gen_cert_and_pin();
        let store = MapIdentityStore::new();
        store.insert(node_with_spki(1, pin));
        let result = check_cert_pin(&cert_der, &store);
        assert!(result.is_ok(), "expected accepted, got: {result:?}");
    }

    /// A CA-signed cert whose SPKI does NOT match any topology entry while a
    /// different entry exists should be rejected.
    #[test]
    fn wrong_spki_rejected() {
        let wrong_pin = [0xFFu8; 32];
        // Under the bootstrap-window rule, unknown certs are accepted; the
        // rejection case only fires when SPIFFE lookup finds an entry whose
        // SPKI diverges. To produce a rejection here we use SPIFFE binding.
        let spiffe_uri = "spiffe://cluster.local/ns/nodedb/node/1";
        let (cert_with_spiffe, cert_spki) = gen_cert_with_spiffe(spiffe_uri);
        // Register the SPIFFE id mapped to a DIFFERENT spki (simulating key mismatch).
        let node = node_with_spiffe(1, spiffe_uri, Some(wrong_pin));
        let store = MapIdentityStore::new();
        store.insert(node);
        let result = check_cert_pin(&cert_with_spiffe, &store);
        // cert_spki != wrong_pin → SPIFFE matched but SPKI diverged → rejected.
        assert!(
            result.is_err(),
            "expected rejection for SPIFFE/SPKI mismatch, got ok; cert_spki={cert_spki:?} wrong_pin={wrong_pin:?}"
        );
    }

    /// An unknown cert is rejected once a store reports bootstrap closure.
    #[test]
    fn unknown_spki_after_bootstrap_is_rejected() {
        let (cert_der, _pin) = gen_cert_and_pin();
        let store = MapIdentityStore::new();
        assert!(check_cert_pin(&cert_der, &store).is_err());
    }

    #[test]
    fn ca_signed_enrollment_marker_is_bounded() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let store = crate::transport::TopologyIdentityStore::new();
        store.install_topology(Arc::new(RwLock::new(
            crate::topology::ClusterTopology::new(),
        )));
        let valid = gen_enrollment_cert(now_ms + 60_000);
        let valid_pin = spki_pin_from_cert_der(&valid).unwrap();
        assert!(store.preauthorize(
            valid_pin,
            std::time::Instant::now() + std::time::Duration::from_secs(60)
        ));
        assert!(check_cert_pin(&valid, &store).is_ok());

        let expired = gen_enrollment_cert(now_ms);
        let expired_pin = spki_pin_from_cert_der(&expired).unwrap();
        assert!(store.preauthorize(
            expired_pin,
            std::time::Instant::now() + std::time::Duration::from_secs(60)
        ));
        assert!(check_cert_pin(&expired, &store).is_err());

        let excessive = gen_enrollment_cert(now_ms + 16 * 60_000);
        let excessive_pin = spki_pin_from_cert_der(&excessive).unwrap();
        assert!(store.preauthorize(
            excessive_pin,
            std::time::Instant::now() + std::time::Duration::from_secs(60)
        ));
        assert!(check_cert_pin(&excessive, &store).is_err());
    }

    #[test]
    fn revoked_enrollment_marker_is_rejected() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let cert = gen_enrollment_cert(now_ms + 60_000);
        let pin = spki_pin_from_cert_der(&cert).unwrap();
        let store = crate::transport::TopologyIdentityStore::new();
        store.install_topology(Arc::new(RwLock::new(
            crate::topology::ClusterTopology::new(),
        )));
        assert!(store.preauthorize(
            pin,
            std::time::Instant::now() + std::time::Duration::from_secs(60)
        ));
        assert!(check_cert_pin(&cert, &store).is_ok());
        store.revoke_preauthorization(
            &pin,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
        );
        assert!(check_cert_pin(&cert, &store).is_err());
    }

    #[test]
    fn explicitly_preauthorized_enrollment_pin_is_accepted() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let cert_der = gen_enrollment_cert(now_ms + 60_000);
        let pin = spki_pin_from_cert_der(&cert_der).unwrap();
        let store = crate::transport::TopologyIdentityStore::new();
        store.install_topology(Arc::new(RwLock::new(
            crate::topology::ClusterTopology::new(),
        )));
        assert!(store.preauthorize(
            pin,
            std::time::Instant::now() + std::time::Duration::from_secs(60)
        ));
        assert!(check_cert_pin(&cert_der, &store).is_ok());
    }

    /// A cert with a SPIFFE id that matches the topology entry is accepted.
    #[test]
    fn spiffe_match_accepted() {
        let spiffe_uri = "spiffe://cluster.local/ns/nodedb/node/42";
        let (cert_der, pin) = gen_cert_with_spiffe(spiffe_uri);
        let node = node_with_spiffe(42, spiffe_uri, Some(pin));
        let store = MapIdentityStore::new();
        store.insert(node);
        let result = check_cert_pin(&cert_der, &store);
        assert!(
            result.is_ok(),
            "expected accepted via SPIFFE, got: {result:?}"
        );
    }

    /// A cert's SPIFFE id matches the topology, SPKI pin field is absent
    /// (Some(pin) not set on NodeInfo) — should also be accepted.
    #[test]
    fn spiffe_match_no_spki_pin_accepted() {
        let spiffe_uri = "spiffe://cluster.local/ns/nodedb/node/7";
        let (cert_der, _pin) = gen_cert_with_spiffe(spiffe_uri);
        // node has spiffe but no spki pin → map_or(true, ...) => accepted
        let node = node_with_spiffe(7, spiffe_uri, None);
        let store = MapIdentityStore::new();
        store.insert(node);
        let result = check_cert_pin(&cert_der, &store);
        assert!(
            result.is_ok(),
            "expected accepted (SPIFFE, no pin), got: {result:?}"
        );
    }

    // ── PinnedClientVerifier integration (uses NoopIdentityStore inner) ──────

    /// PinnedClientVerifier with empty topology accepts everything that passes
    /// WebPki — bootstrap window.  We exercise the type here without a live
    /// CA by using the `NoopIdentityStore` path.
    #[test]
    fn pinned_client_verifier_noop_store_bootstrap_window() {
        use crate::transport::peer_identity_store::NoopIdentityStore;

        // With NoopIdentityStore both find_by_spki and find_by_spiffe return None,
        // so check_cert_pin always returns Ok (bootstrap window).
        let (cert_der, _pin) = gen_cert_and_pin();
        let store = Arc::new(NoopIdentityStore) as Arc<dyn PeerIdentityStore>;
        let result = check_cert_pin(&cert_der, &*store);
        assert!(result.is_ok());
    }
}
