// SPDX-License-Identifier: BUSL-1.1

//! Live peer-identity lookup backed by the authoritative cluster topology.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::topology::{ClusterTopology, NodeInfo};
use crate::transport::peer_identity_store::PeerIdentityStore;

/// Identity store installed into the mTLS transport before the listener binds.
///
/// The topology is attached atomically after bootstrap/join/restart returns and
/// before the RPC server starts. Every subsequent lookup reads the same shared
/// topology that Raft metadata application mutates.
const MAX_PREAUTHORIZED_ENROLLMENTS: usize = 1_024;

pub struct TopologyIdentityStore {
    topology: RwLock<Option<Arc<RwLock<ClusterTopology>>>>,
    preauthorized: Mutex<HashMap<[u8; 32], Instant>>,
    revoked: Mutex<HashMap<[u8; 32], Instant>>,
}

impl TopologyIdentityStore {
    pub fn new() -> Self {
        Self {
            topology: RwLock::new(None),
            preauthorized: Mutex::new(HashMap::new()),
            revoked: Mutex::new(HashMap::new()),
        }
    }

    fn topology(&self) -> Option<Arc<RwLock<ClusterTopology>>> {
        self.topology
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl Default for TopologyIdentityStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerIdentityStore for TopologyIdentityStore {
    fn get_node_info(&self, node_id: u64) -> Option<NodeInfo> {
        let topology = self.topology()?;
        topology
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_node(node_id)
            .cloned()
    }

    fn find_by_spki(&self, spki: &[u8; 32]) -> Option<NodeInfo> {
        let topology = self.topology()?;
        let found = topology
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .all_nodes()
            .find(|node| node.spki_pin.as_ref() == Some(spki))
            .cloned();
        if found.is_some() {
            self.preauthorized
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(spki);
        }
        found
    }

    fn find_by_spiffe(&self, spiffe_id: &str) -> Option<NodeInfo> {
        let topology = self.topology()?;
        topology
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .all_nodes()
            .find(|node| node.spiffe_id.as_deref() == Some(spiffe_id))
            .cloned()
    }

    fn install_topology(&self, topology: Arc<RwLock<ClusterTopology>>) {
        *self
            .topology
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(topology);
    }

    fn enforces_peer_identity(&self) -> bool {
        true
    }

    fn bootstrap_window_open(&self) -> bool {
        self.topology
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_none()
    }

    fn is_preauthorized(&self, spki: &[u8; 32]) -> bool {
        let now = Instant::now();
        let mut pins = self
            .preauthorized
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pins.retain(|_, expires_at| *expires_at > now);
        pins.contains_key(spki)
    }

    fn preauthorize(&self, spki: [u8; 32], expires_at: Instant) -> bool {
        let now = Instant::now();
        let mut pins = self
            .preauthorized
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pins.retain(|_, deadline| *deadline > now);
        if expires_at <= now {
            return false;
        }
        if !pins.contains_key(&spki) && pins.len() >= MAX_PREAUTHORIZED_ENROLLMENTS {
            return false;
        }
        pins.insert(spki, expires_at);
        self.revoked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&spki);
        true
    }

    fn is_enrollment_revoked(&self, spki: &[u8; 32]) -> bool {
        let now = Instant::now();
        let mut revoked = self
            .revoked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        revoked.retain(|_, expires_at| *expires_at > now);
        revoked.contains_key(spki)
    }

    fn revoke_preauthorization(&self, spki: &[u8; 32], expires_at: Instant) {
        self.preauthorized
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(spki);
        if expires_at > Instant::now() {
            self.revoked
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(*spki, expires_at);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::topology::NodeState;

    #[test]
    fn enrollment_preauthorization_is_bounded_and_revocable() {
        let store = TopologyIdentityStore::new();
        assert!(!store.preauthorize([1; 32], Instant::now()));
        assert!(store.preauthorize([2; 32], Instant::now() + std::time::Duration::from_secs(60)));
        assert!(store.is_preauthorized(&[2; 32]));
        store.revoke_preauthorization(
            &[2; 32],
            Instant::now() + std::time::Duration::from_secs(60),
        );
        assert!(!store.is_preauthorized(&[2; 32]));
        assert!(store.is_enrollment_revoked(&[2; 32]));
    }

    #[test]
    fn attached_topology_is_the_single_live_identity_source() {
        let store = TopologyIdentityStore::new();
        assert!(store.enforces_peer_identity());
        assert!(store.bootstrap_window_open());
        assert!(store.get_node_info(7).is_none());
        assert!(store.preauthorize([3; 32], Instant::now() + std::time::Duration::from_secs(60)));
        assert!(store.is_preauthorized(&[3; 32]));

        let topology = Arc::new(RwLock::new(ClusterTopology::new()));
        store.install_topology(Arc::clone(&topology));
        assert!(!store.bootstrap_window_open());
        let node = NodeInfo::new(
            7,
            "127.0.0.1:9400".parse::<SocketAddr>().unwrap(),
            NodeState::Active,
        )
        .with_spki_pin(Some([3; 32]));
        topology
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .add_node(node);

        assert_eq!(store.get_node_info(7).unwrap().spki_pin, Some([3; 32]));
        assert_eq!(store.find_by_spki(&[3; 32]).unwrap().node_id, 7);
        assert!(!store.is_preauthorized(&[3; 32]));
    }
}
