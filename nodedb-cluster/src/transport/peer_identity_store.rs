// SPDX-License-Identifier: BUSL-1.1

//! Topology-decoupled lookup for per-node identity pins.

use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::topology::{ClusterTopology, NodeInfo};

/// Topology-decoupled lookup for per-node identity pins.
///
/// The server needs to check the TLS peer cert against the pinned
/// `NodeInfo` for the MAC-verified `from_node_id`, but it must not
/// take a direct dependency on `ClusterState` or `ClusterTopology`
/// (which would create a circular crate dependency and would be hard
/// to test).  Implementors wrap whatever topology store they have.
///
/// The `NoopIdentityStore` below is used in insecure-transport mode
/// and in unit tests that do not exercise the identity layer.
///
/// The `find_by_spki` and `find_by_spiffe` methods are called by the
/// TLS-layer [`PinnedClientVerifier`] and [`PinnedServerVerifier`]
/// during the QUIC handshake (before `node_id` is known from the MAC
/// envelope). They search the topology by the cert's identity rather
/// than by node_id.
///
/// [`PinnedClientVerifier`]: crate::transport::config::PinnedClientVerifier
/// [`PinnedServerVerifier`]: crate::transport::config::PinnedServerVerifier
pub trait PeerIdentityStore: Send + Sync + 'static {
    /// Return the `NodeInfo` for the given node_id, or `None` if
    /// the node is not in the topology (treat as bootstrap window).
    fn get_node_info(&self, node_id: u64) -> Option<NodeInfo>;

    /// Return the `NodeInfo` for the node whose pinned SPKI fingerprint
    /// matches `spki`, or `None` if no node in the topology has that pin.
    ///
    /// Used by the TLS-layer verifiers during the handshake (before the
    /// MAC envelope reveals `node_id`).
    fn find_by_spki(&self, spki: &[u8; 32]) -> Option<NodeInfo>;

    /// Return the `NodeInfo` for the node whose pinned SPIFFE id matches
    /// `spiffe_id`, or `None` if no node has that id.
    ///
    /// Used by the TLS-layer verifiers during the handshake.
    fn find_by_spiffe(&self, spiffe_id: &str) -> Option<NodeInfo>;

    /// Attach the authoritative topology after bootstrap/join/restart.
    /// Implementations that are not topology-backed may ignore this hook.
    fn install_topology(&self, _topology: Arc<RwLock<ClusterTopology>>) {}

    /// Whether the application layer must reject identities not yet present
    /// in the topology except for the narrowly validated join enrollment RPC.
    fn enforces_peer_identity(&self) -> bool {
        false
    }

    /// Whether initial seed discovery is still allowed before an
    /// authoritative topology has been installed.
    fn bootstrap_window_open(&self) -> bool {
        false
    }

    /// Whether a CA-verified leaf pin was explicitly issued for enrollment.
    fn is_preauthorized(&self, _spki: &[u8; 32]) -> bool {
        false
    }

    /// Preauthorize a freshly issued joiner leaf until the bounded deadline.
    /// Returns false when the bounded enrollment set is full.
    fn preauthorize(&self, _spki: [u8; 32], _expires_at: Instant) -> bool {
        false
    }

    /// Whether a bounded enrollment credential was explicitly revoked.
    fn is_enrollment_revoked(&self, _spki: &[u8; 32]) -> bool {
        false
    }

    /// Revoke an enrollment pin until its certificate enrollment deadline.
    fn revoke_preauthorization(&self, _spki: &[u8; 32], _expires_at: Instant) {}
}

/// Always returns `None` and disables application-layer pin enforcement.
///
/// Used when mTLS is disabled (insecure transport) or in unit tests
/// that focus on HMAC / codec layers rather than identity binding.
pub struct NoopIdentityStore;

impl PeerIdentityStore for NoopIdentityStore {
    fn get_node_info(&self, _node_id: u64) -> Option<NodeInfo> {
        None
    }

    fn find_by_spki(&self, _spki: &[u8; 32]) -> Option<NodeInfo> {
        None
    }

    fn find_by_spiffe(&self, _spiffe_id: &str) -> Option<NodeInfo> {
        None
    }

    fn bootstrap_window_open(&self) -> bool {
        true
    }
}
