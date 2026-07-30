// SPDX-License-Identifier: BUSL-1.1

//! Ownership of Loro peer ids, per `LoroDoc`.
//!
//! A peer id is the identity every CRDT operation is attributed to. When two
//! replicas claim the same one they allocate overlapping `(peer, counter)`
//! ranges for different writes, and the merge discards whichever arrives
//! second — correctly, by its own rules, and invisibly, because a colliding
//! write and an idempotent replay are the same thing at that level.
//!
//! This module refuses the collision one layer up, where the two *are*
//! distinguishable: the server knows which durable producer each session
//! belongs to, so it can hold a peer id to its first owner.
//!
//! Conflicts converge on the **lower producer id**. That rule is commutative
//! and idempotent, so every replica reaches the same owner no matter what order
//! the entries apply in, and because producer ids are allocated monotonically it
//! also means the older client keeps its identity: a client registered later
//! always has a higher id and can never displace an established binding.

use crate::control::security::catalog::sync_producer::{PeerBindingKey, StoredPeerBinding};
use crate::control::sync_producer::registry::SyncProducerRegistry;

/// Who owns a peer id after a bind attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerBindOutcome {
    /// The requesting producer holds the binding — it either just claimed the
    /// peer id or already owned it.
    Owned,
    /// A different producer holds it. The requester must not write under this
    /// peer id: doing so is the collision, and the merge would absorb its
    /// operations without a trace.
    Conflict { owner_producer_id: u64 },
}

impl SyncProducerRegistry {
    /// The producer that currently owns `key`, if any.
    pub fn peer_owner(&self, key: &PeerBindingKey) -> crate::Result<Option<u64>> {
        if let Some(owner) = self
            .peer_bindings
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(key)
        {
            return Ok(Some(*owner));
        }
        let stored = self.catalog.get_peer_binding(&key.encode())?;
        if let Some(stored) = stored {
            self.memoize(key, stored.producer_id);
        }
        Ok(stored.map(|binding| binding.producer_id))
    }

    /// Claim `key` for `producer_id`, or report the producer that already has
    /// it.
    ///
    /// The durable claim is a single put-if-absent transaction, so two sessions
    /// racing on a fresh peer id cannot both see it as unowned.
    pub fn bind_peer(
        &self,
        key: &PeerBindingKey,
        producer_id: u64,
        now_ms: i64,
    ) -> crate::Result<PeerBindOutcome> {
        if let Some(owner) = self.peer_owner(key)? {
            return Ok(Self::outcome_for(owner, producer_id));
        }
        let owner = self.catalog.bind_peer_if_absent(
            &key.encode(),
            &StoredPeerBinding {
                producer_id,
                bound_ms: now_ms,
            },
        )?;
        self.memoize(key, owner.producer_id);
        Ok(Self::outcome_for(owner.producer_id, producer_id))
    }

    /// Idempotent per-node apply for a replicated binding.
    ///
    /// Called by `MetadataCommitApplier` on every node when a `SyncPeerBind`
    /// entry commits. The lower producer id wins, which makes the result
    /// independent of apply order and lets a node that optimistically claimed
    /// the peer id locally be corrected by the entry that beat it.
    pub fn apply_bind_peer(
        &self,
        key: &PeerBindingKey,
        producer_id: u64,
        bound_ms: i64,
    ) -> crate::Result<()> {
        let encoded = key.encode();
        if let Some(existing) = self.catalog.get_peer_binding(&encoded)?
            && existing.producer_id < producer_id
        {
            // An established, lower-id owner is authoritative. Re-memoize in
            // case this node only learned the row from the catalog.
            self.memoize(key, existing.producer_id);
            return Ok(());
        }
        self.catalog.put_peer_binding(
            &encoded,
            &StoredPeerBinding {
                producer_id,
                bound_ms,
            },
        )?;
        self.memoize(key, producer_id);
        Ok(())
    }

    /// Whether this process has seen `key`'s ownership replicated.
    ///
    /// Until it has, the binding is a local claim only, and admitting writes
    /// under it would trust an ownership decision the cluster never made.
    pub fn peer_binding_converged(&self, key: &PeerBindingKey) -> bool {
        self.converged_peer_bindings
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(key)
    }

    /// Record that `key`'s ownership has been replicated and applied locally.
    pub fn mark_peer_binding_converged(&self, key: &PeerBindingKey) {
        self.converged_peer_bindings
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key.clone());
    }

    fn memoize(&self, key: &PeerBindingKey, producer_id: u64) {
        self.peer_bindings
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key.clone(), producer_id);
    }

    fn outcome_for(owner_producer_id: u64, producer_id: u64) -> PeerBindOutcome {
        if owner_producer_id == producer_id {
            PeerBindOutcome::Owned
        } else {
            PeerBindOutcome::Conflict { owner_producer_id }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control::security::catalog::SystemCatalog;

    fn open_registry() -> (tempfile::TempDir, SyncProducerRegistry) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(SystemCatalog::open(&dir.path().join("system.redb")).unwrap());
        let registry = SyncProducerRegistry::open(catalog).unwrap();
        (dir, registry)
    }

    fn key(collection: &str, peer_id: u64) -> PeerBindingKey {
        PeerBindingKey::new(1, 2, collection, peer_id)
    }

    #[test]
    fn an_unclaimed_peer_id_is_owned_by_its_first_producer() {
        let (_dir, registry) = open_registry();
        assert_eq!(
            registry.bind_peer(&key("docs", 7), 10, 0).unwrap(),
            PeerBindOutcome::Owned
        );
        // The same producer reconnecting keeps its own peer id.
        assert_eq!(
            registry.bind_peer(&key("docs", 7), 10, 1).unwrap(),
            PeerBindOutcome::Owned
        );
    }

    #[test]
    fn a_second_producer_claiming_the_same_peer_id_is_refused() {
        let (_dir, registry) = open_registry();
        registry.bind_peer(&key("docs", 7), 10, 0).unwrap();
        assert_eq!(
            registry.bind_peer(&key("docs", 7), 11, 0).unwrap(),
            PeerBindOutcome::Conflict {
                owner_producer_id: 10
            }
        );
    }

    #[test]
    fn a_peer_id_reused_in_another_collection_is_not_a_collision() {
        // Different collections are different LoroDocs, so their counter ranges
        // never meet. Refusing here would break every client that derives one
        // peer id per collection from a single base.
        let (_dir, registry) = open_registry();
        registry.bind_peer(&key("docs", 7), 10, 0).unwrap();
        assert_eq!(
            registry.bind_peer(&key("notes", 7), 11, 0).unwrap(),
            PeerBindOutcome::Owned
        );
    }

    #[test]
    fn replicated_apply_converges_on_the_lower_producer_id() {
        // Two nodes each claimed the peer id locally before proposing. Whatever
        // order the entries arrive in, both must end up naming one owner.
        let (_dir_a, node_a) = open_registry();
        let (_dir_b, node_b) = open_registry();
        let k = key("docs", 7);

        node_a.bind_peer(&k, 10, 100).unwrap();
        node_b.bind_peer(&k, 11, 101).unwrap();

        node_a.apply_bind_peer(&k, 10, 100).unwrap();
        node_a.apply_bind_peer(&k, 11, 101).unwrap();
        node_b.apply_bind_peer(&k, 11, 101).unwrap();
        node_b.apply_bind_peer(&k, 10, 100).unwrap();

        assert_eq!(node_a.peer_owner(&k).unwrap(), Some(10));
        assert_eq!(
            node_b.peer_owner(&k).unwrap(),
            Some(10),
            "the node that lost the race must be corrected, not left diverged"
        );
    }

    #[test]
    fn replicated_apply_is_idempotent() {
        let (_dir, registry) = open_registry();
        let k = key("docs", 7);
        registry.apply_bind_peer(&k, 10, 100).unwrap();
        registry.apply_bind_peer(&k, 10, 100).unwrap();
        assert_eq!(registry.peer_owner(&k).unwrap(), Some(10));
    }

    #[test]
    fn bindings_are_answered_from_memory_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let k = key("docs", 7);
        {
            let catalog = Arc::new(SystemCatalog::open(&path).unwrap());
            let registry = SyncProducerRegistry::open(catalog).unwrap();
            registry.bind_peer(&k, 10, 0).unwrap();
        }
        let catalog = Arc::new(SystemCatalog::open(&path).unwrap());
        let registry = SyncProducerRegistry::open(catalog).unwrap();
        assert_eq!(registry.peer_owner(&k).unwrap(), Some(10));
        assert_eq!(
            registry.bind_peer(&k, 11, 0).unwrap(),
            PeerBindOutcome::Conflict {
                owner_producer_id: 10
            },
            "a binding must survive restart or the collision returns"
        );
    }
}
