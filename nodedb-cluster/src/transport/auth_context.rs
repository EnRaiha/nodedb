// SPDX-License-Identifier: BUSL-1.1

//! Per-transport authentication state shared across inbound and outbound
//! paths.
//!
//! The [`AuthContext`] bundles the MAC key and the per-peer sequence
//! trackers so that `send.rs`, `serve.rs`, and the spawned per-connection
//! tasks all operate on the same state. Wrapped in an `Arc` and cloned
//! into every per-connection / per-stream task.

use crate::rpc_codec::{MacKey, PeerSeqSender, PeerSeqWindow};

use super::credentials::TransportCredentials;

/// Shared auth state — node identity, MAC key, per-peer counters.
#[derive(Debug)]
pub struct AuthContext {
    /// Local node id. Used as `from_node_id` on every outbound envelope.
    pub local_node_id: u64,
    /// Cluster-wide MAC key. `MacKey::zero()` when the transport was
    /// constructed with [`TransportCredentials::Insecure`] — replay
    /// protection is cosmetic in that mode.
    pub mac_key: MacKey,
    /// Outbound sequence counters, keyed by remote peer id. `peer_id = 0`
    /// is the fallback key for bootstrap RPCs sent to an address whose
    /// node id is not yet known.
    pub peer_seq_out: PeerSeqSender,
    /// Inbound replay-detection windows, keyed by the `from_node_id`
    /// advertised in the envelope (and MAC-verified before consultation).
    pub peer_seq_in: PeerSeqWindow,
}

impl AuthContext {
    /// Build an auth context from the same credentials that drive the
    /// TLS configuration.
    pub fn from_credentials(local_node_id: u64, creds: &TransportCredentials) -> Self {
        let mac_key = match creds {
            TransportCredentials::Mtls(tls) => MacKey::from_bytes(tls.cluster_secret),
            TransportCredentials::Insecure => MacKey::zero(),
        };
        Self {
            local_node_id,
            mac_key,
            peer_seq_out: PeerSeqSender::new(),
            peer_seq_in: PeerSeqWindow::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::config::TlsCredentials;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    fn dummy_tls(secret: [u8; 32]) -> TlsCredentials {
        TlsCredentials {
            cert: CertificateDer::from(vec![1, 2, 3]),
            key: PrivateKeyDer::from(PrivatePkcs8KeyDer::from(vec![4, 5, 6])),
            ca_cert: CertificateDer::from(vec![7, 8, 9]),
            additional_ca_certs: Vec::new(),
            crls: Vec::new(),
            cluster_secret: secret,
            spki_pin: [0u8; 32],
            bootstrap_peer_spki: None,
        }
    }

    #[test]
    fn insecure_yields_zero_mac_key() {
        let ctx = AuthContext::from_credentials(1, &TransportCredentials::Insecure);
        assert!(ctx.mac_key.is_zero());
        assert_eq!(ctx.local_node_id, 1);
    }

    #[test]
    fn mtls_yields_cluster_secret() {
        let secret = [0xABu8; 32];
        let ctx = AuthContext::from_credentials(42, &TransportCredentials::Mtls(dummy_tls(secret)));
        assert!(!ctx.mac_key.is_zero());
        assert_eq!(ctx.mac_key.as_bytes(), &secret);
    }
}
