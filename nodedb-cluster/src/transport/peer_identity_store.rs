// SPDX-License-Identifier: BUSL-1.1

//! Topology-decoupled lookup for per-node identity pins.

use crate::topology::NodeInfo;

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
}

/// Always returns `None`, accepting every peer as a bootstrap window.
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
}
