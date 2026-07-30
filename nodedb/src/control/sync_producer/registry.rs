// SPDX-License-Identifier: BUSL-1.1

//! `SyncProducerRegistry` — durable, per-`lite_id` producer registry.
//!
//! Combines the `ProducerIdAllocator` (monotonic `u64` counter) with the
//! `SystemCatalog`-backed `_system.sync_producers` table to issue and persist
//! fenced `(producer_id, epoch)` tokens for each Lite client.
//!
//! ## Design
//!
//! This registry is the exact analogue of the surrogate allocator + catalog in
//! `crate::control::surrogate`: the allocator owns the in-memory counter; the
//! catalog owns the durable rows; this registry coordinates the two.
//!
//! `register` and `fence` are called from the Lite handshake path to make a
//! durable local fencing decision. After each successful local write the
//! handshake proposes a `SyncProducerRegister` / `SyncProducerFence` entry
//! through the metadata Raft group so every follower applies the same state
//! and the producer-id / epoch survive leader failover.
//!
//! `apply_register` and `apply_fence` are the idempotent per-node apply path
//! invoked by `MetadataCommitApplier` on every node (including the leader)
//! when those Raft entries commit.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::sync_producer::{
    PeerBindingKey, StoredProducerRegistration,
};
use crate::control::sync_producer::allocator::{ProducerHwmPersist, ProducerIdAllocator};
use crate::control::sync_producer::persist::SystemCatalogProducerHwm;

/// `ProducerRegistration` — the application-level view of a registered Lite
/// client, returned by `register` and `get`.
///
/// Producer-id `0` is reserved and will never be returned by `register`.
#[derive(Debug, Clone, PartialEq)]
pub struct ProducerRegistration {
    /// Stable, monotonic u64 identity for this Lite client's write stream.
    pub producer_id: u64,
    /// Current fencing epoch.  Any token with a lower epoch is stale.
    pub current_epoch: u64,
    /// Tenant that owns this registration.
    pub tenant_id: u64,
    /// Immutable authenticated user that owns this registration.
    pub user_id: u64,
    /// Unix-millisecond timestamp when the registration was created.
    pub created_ms: i64,
}

impl From<StoredProducerRegistration> for ProducerRegistration {
    fn from(s: StoredProducerRegistration) -> Self {
        Self {
            producer_id: s.producer_id,
            current_epoch: s.current_epoch,
            tenant_id: s.tenant_id,
            user_id: s.user_id,
            created_ms: s.created_ms,
        }
    }
}

impl From<&ProducerRegistration> for StoredProducerRegistration {
    fn from(r: &ProducerRegistration) -> Self {
        Self {
            producer_id: r.producer_id,
            current_epoch: r.current_epoch,
            tenant_id: r.tenant_id,
            user_id: r.user_id,
            created_ms: r.created_ms,
        }
    }
}

/// Durable, per-`lite_id` producer registry.
///
/// `Send + Sync` — Control Plane only; no io_uring, no Data Plane types.
pub struct SyncProducerRegistry {
    pub(super) catalog: Arc<SystemCatalog>,
    alloc: ProducerIdAllocator,
    registration_lock: Mutex<()>,
    /// Which producer owns each Loro peer id, mirrored from the catalog.
    ///
    /// Every inbound delta consults this, so it is answered from memory; the
    /// catalog rows remain the durable authority and are re-read whenever the
    /// key is not already known.
    pub(super) peer_bindings: RwLock<HashMap<PeerBindingKey, u64>>,
    /// Bindings this process has seen replicated, deliberately not persisted.
    ///
    /// A durable row proves this node claimed the peer id, not that the cluster
    /// agreed: a proposal can fail after the local write, and after a restart
    /// nothing distinguishes the two. Starting empty makes the first delta of
    /// each process re-propose an already-durable claim — idempotent, and the
    /// only thing that keeps an unreplicated claim from being trusted forever.
    pub(super) converged_peer_bindings: RwLock<std::collections::HashSet<PeerBindingKey>>,
}

impl SyncProducerRegistry {
    /// Open a registry backed by `catalog`.  The allocator is seeded from the
    /// persisted hwm so post-restart allocations cannot collide with pre-crash
    /// ones.
    pub fn open(catalog: Arc<SystemCatalog>) -> crate::Result<Self> {
        let hwm = catalog.get_producer_hwm()?;
        let alloc = ProducerIdAllocator::from_persisted_hwm(hwm);
        let peer_bindings = catalog
            .list_peer_bindings()?
            .into_iter()
            .map(|(key, binding)| (key, binding.producer_id))
            .collect();
        Ok(Self {
            catalog,
            alloc,
            registration_lock: Mutex::new(()),
            peer_bindings: RwLock::new(peer_bindings),
            converged_peer_bindings: RwLock::new(std::collections::HashSet::new()),
        })
    }

    /// Look up the registration for `lite_id`.  Returns `None` if the Lite
    /// client has never registered.
    pub fn get(&self, lite_id: &str) -> crate::Result<Option<ProducerRegistration>> {
        Ok(self
            .catalog
            .get_producer_registration(lite_id)?
            .map(ProducerRegistration::from))
    }

    /// Atomically load or create a Lite producer registration.
    ///
    /// The returned boolean is `true` only when this call created the durable
    /// row. Existing ownership is immutable: a different authenticated tenant
    /// or user receives a typed error rather than replacing the registration.
    pub fn get_or_register(
        &self,
        lite_id: &str,
        tenant_id: u64,
        user_id: u64,
        epoch: u64,
        now_ms: i64,
    ) -> crate::Result<(ProducerRegistration, bool)> {
        let _guard = self
            .registration_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = self.catalog.get_producer_registration(lite_id)? {
            if existing.tenant_id != tenant_id || existing.user_id != user_id {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "lite_id '{lite_id}' is already owned by another authenticated principal"
                    ),
                });
            }
            return Ok((ProducerRegistration::from(existing), false));
        }

        let producer_id = self.alloc.alloc_one().map_err(crate::Error::from)?;
        let hwm_persist = SystemCatalogProducerHwm::new(self.catalog.clone());
        if self.alloc.should_flush() {
            self.alloc.flush(&hwm_persist).map_err(crate::Error::from)?;
        } else {
            hwm_persist.checkpoint(self.alloc.current_hwm())?;
        }

        let registration = ProducerRegistration {
            producer_id,
            current_epoch: epoch,
            tenant_id,
            user_id,
            created_ms: now_ms,
        };
        self.catalog
            .put_producer_registration(lite_id, &StoredProducerRegistration::from(&registration))?;
        Ok((registration, true))
    }

    /// Register a Lite client or return its immutable existing registration.
    pub fn register(
        &self,
        lite_id: &str,
        tenant_id: u64,
        user_id: u64,
        epoch: u64,
        now_ms: i64,
    ) -> crate::Result<ProducerRegistration> {
        self.get_or_register(lite_id, tenant_id, user_id, epoch, now_ms)
            .map(|(registration, _created)| registration)
    }

    /// Advance the fencing epoch for an existing registration.
    ///
    /// Returns `crate::Error::BadRequest` if `lite_id` has no registration.
    /// After this call any token issued with an epoch below `new_epoch` must
    /// be rejected by the write handler.
    pub fn fence(&self, lite_id: &str, new_epoch: u64) -> crate::Result<()> {
        let existing = self
            .catalog
            .get_producer_registration(lite_id)?
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("no producer registration for lite_id '{lite_id}'"),
            })?;

        let updated = StoredProducerRegistration {
            current_epoch: new_epoch,
            ..existing
        };
        self.catalog.put_producer_registration(lite_id, &updated)
    }

    /// Idempotent per-node apply for a replicated registration.
    ///
    /// Called by `MetadataCommitApplier` on every node (including the leader)
    /// when a `SyncProducerRegister` Raft entry commits. Advances the
    /// allocator HWM so a future leader never reissues `producer_id`, then
    /// writes the registration row verbatim. Re-applying the same entry
    /// overwrites identical state — safe for duplicate delivery.
    pub fn apply_register(
        &self,
        lite_id: &str,
        producer_id: u64,
        tenant_id: u64,
        user_id: u64,
        epoch: u64,
        created_ms: i64,
    ) -> crate::Result<()> {
        let _guard = self
            .registration_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.alloc
            .restore_hwm(producer_id)
            .map_err(crate::Error::from)?;

        let hwm_persist = SystemCatalogProducerHwm::new(self.catalog.clone());
        hwm_persist.checkpoint(self.alloc.current_hwm())?;

        let row = StoredProducerRegistration {
            producer_id,
            current_epoch: epoch,
            tenant_id,
            user_id,
            created_ms,
        };
        if let Some(existing) = self.catalog.get_producer_registration(lite_id)? {
            let is_identical = existing.producer_id == producer_id
                && existing.tenant_id == tenant_id
                && existing.user_id == user_id
                && existing.created_ms == created_ms;
            if !is_identical {
                return Err(crate::Error::BadRequest {
                    detail: format!("conflicting replicated registration for lite_id '{lite_id}'"),
                });
            }
            return Ok(());
        }
        self.catalog.put_producer_registration(lite_id, &row)
    }

    /// Idempotent per-node apply for a replicated epoch fence.
    ///
    /// Called by `MetadataCommitApplier` on every node when a
    /// `SyncProducerFence` Raft entry commits. Uses max-wins: sets
    /// `current_epoch = max(stored, new_epoch)`. If no registration row
    /// exists for `lite_id`, returns `Ok(())` — a fence with no prior
    /// register entry is tolerated for reordering or missing-entry cases.
    pub fn apply_fence(&self, lite_id: &str, new_epoch: u64) -> crate::Result<()> {
        let Some(existing) = self.catalog.get_producer_registration(lite_id)? else {
            return Ok(());
        };
        let updated = StoredProducerRegistration {
            current_epoch: existing.current_epoch.max(new_epoch),
            ..existing
        };
        self.catalog.put_producer_registration(lite_id, &updated)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn open_registry() -> (tempfile::TempDir, SyncProducerRegistry) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(SystemCatalog::open(&dir.path().join("system.redb")).unwrap());
        let reg = SyncProducerRegistry::open(catalog).unwrap();
        (dir, reg)
    }

    #[test]
    fn register_returns_nonzero_producer_id() {
        let (_dir, reg) = open_registry();
        let r = reg
            .register("device-1", 42, 99, 0, 1_700_000_000_000)
            .unwrap();
        assert!(r.producer_id > 0, "producer_id must be > 0");
    }

    #[test]
    fn get_after_register_returns_same_record() {
        let (_dir, reg) = open_registry();
        let created = reg
            .register("device-1", 42, 99, 0, 1_700_000_000_000)
            .unwrap();
        let loaded = reg.get("device-1").unwrap().unwrap();
        assert_eq!(loaded.producer_id, created.producer_id);
        assert_eq!(loaded.tenant_id, 42);
        assert_eq!(loaded.current_epoch, 0);
        assert_eq!(loaded.created_ms, 1_700_000_000_000);
    }

    #[test]
    fn get_unknown_lite_id_returns_none() {
        let (_dir, reg) = open_registry();
        assert!(reg.get("nobody").unwrap().is_none());
    }

    #[test]
    fn distinct_lite_ids_get_distinct_producer_ids() {
        let (_dir, reg) = open_registry();
        let a = reg.register("device-a", 1, 99, 0, 0).unwrap();
        let b = reg.register("device-b", 1, 99, 0, 0).unwrap();
        assert_ne!(
            a.producer_id, b.producer_id,
            "each lite_id must get a unique producer_id"
        );
    }

    #[test]
    fn producer_ids_are_monotonically_increasing() {
        let (_dir, reg) = open_registry();
        let ids: Vec<u64> = (0..20)
            .map(|i| {
                reg.register(&format!("device-{i}"), 1, 99, 0, 0)
                    .unwrap()
                    .producer_id
            })
            .collect();
        for w in ids.windows(2) {
            assert!(w[1] > w[0], "producer_ids must be monotonic");
        }
    }

    #[test]
    fn fence_advances_epoch() {
        let (_dir, reg) = open_registry();
        reg.register("device-1", 1, 99, 0, 0).unwrap();
        reg.fence("device-1", 5).unwrap();
        let loaded = reg.get("device-1").unwrap().unwrap();
        assert_eq!(loaded.current_epoch, 5);
    }

    #[test]
    fn fence_unknown_lite_id_errors() {
        let (_dir, reg) = open_registry();
        let err = reg.fence("nobody", 1).unwrap_err();
        assert!(
            matches!(err, crate::Error::BadRequest { .. }),
            "expected BadRequest, got {err:?}"
        );
    }

    #[test]
    fn registrations_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        let producer_id = {
            let catalog = Arc::new(SystemCatalog::open(&path).unwrap());
            let reg = SyncProducerRegistry::open(catalog).unwrap();
            let r = reg.register("device-1", 7, 99, 0, 12345).unwrap();
            reg.fence("device-1", 3).unwrap();
            r.producer_id
        };

        let catalog = Arc::new(SystemCatalog::open(&path).unwrap());
        let reg = SyncProducerRegistry::open(catalog).unwrap();
        let loaded = reg.get("device-1").unwrap().unwrap();
        assert_eq!(loaded.producer_id, producer_id);
        assert_eq!(loaded.current_epoch, 3);
        assert_eq!(loaded.tenant_id, 7);
    }

    #[test]
    fn allocator_hwm_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let catalog = Arc::new(SystemCatalog::open(&path).unwrap());
            let reg = SyncProducerRegistry::open(catalog).unwrap();
            reg.register("d1", 1, 99, 0, 0).unwrap();
            reg.register("d2", 1, 99, 0, 0).unwrap();
        }

        // On reopen the allocator must start above the previous hwm so ids
        // from the new session never collide with ids from the first session.
        let catalog = Arc::new(SystemCatalog::open(&path).unwrap());
        let reg = SyncProducerRegistry::open(catalog).unwrap();
        let r = reg.register("d3", 1, 99, 0, 0).unwrap();
        assert!(
            r.producer_id > 2,
            "post-restart ids must be above pre-crash hwm"
        );
    }

    #[test]
    fn apply_register_writes_row_and_advances_hwm() {
        // Mirror a follower applying a replicated registration: the row is
        // written verbatim (leader-assigned producer_id), and the allocator
        // hwm advances so a future local allocation never reissues the id.
        let (_dir, reg) = open_registry();
        reg.apply_register("device-x", 42, 7, 99, 3, 1_700_000_000_000)
            .unwrap();

        let loaded = reg.get("device-x").unwrap().unwrap();
        assert_eq!(loaded.producer_id, 42);
        assert_eq!(loaded.current_epoch, 3);
        assert_eq!(loaded.tenant_id, 7);

        // A subsequent local allocation must be strictly above the applied id.
        let next = reg.register("device-y", 7, 99, 0, 0).unwrap();
        assert!(
            next.producer_id > 42,
            "local allocation must not reissue an applied producer_id"
        );
    }

    #[test]
    fn apply_register_rejects_conflicting_owner_and_preserves_first_row() {
        let (_dir, reg) = open_registry();
        reg.apply_register("device-x", 42, 7, 99, 3, 100).unwrap();
        let error = reg
            .apply_register("device-x", 43, 8, 100, 3, 101)
            .unwrap_err();
        assert!(matches!(error, crate::Error::BadRequest { .. }));
        let loaded = reg.get("device-x").unwrap().unwrap();
        assert_eq!(loaded.producer_id, 42);
        assert_eq!(loaded.tenant_id, 7);
        assert_eq!(loaded.user_id, 99);
    }

    #[test]
    fn atomic_registration_rejects_cross_principal_reuse() {
        let (_dir, reg) = open_registry();
        let (first, created) = reg.get_or_register("device-x", 7, 99, 3, 100).unwrap();
        assert!(created);
        let error = reg.get_or_register("device-x", 8, 100, 3, 101).unwrap_err();
        assert!(matches!(error, crate::Error::BadRequest { .. }));
        assert_eq!(reg.get("device-x").unwrap().unwrap(), first);
    }

    #[test]
    fn local_registration_and_raft_apply_cannot_overwrite_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(SystemCatalog::open(&dir.path().join("system.redb")).unwrap());
        let reg = Arc::new(SyncProducerRegistry::open(catalog).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(3));

        let local = {
            let reg = Arc::clone(&reg);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                reg.get_or_register("raced-device", 7, 70, 1, 100)
            })
        };
        let replicated = {
            let reg = Arc::clone(&reg);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                reg.apply_register("raced-device", 900, 8, 80, 1, 101)
            })
        };
        barrier.wait();
        let local = local.join().unwrap();
        let replicated = replicated.join().unwrap();
        assert_ne!(local.is_ok(), replicated.is_ok());

        let stored = reg.get("raced-device").unwrap().unwrap();
        if local.is_ok() {
            assert_eq!((stored.tenant_id, stored.user_id), (7, 70));
        } else {
            assert_eq!((stored.tenant_id, stored.user_id), (8, 80));
            assert_eq!(stored.producer_id, 900);
        }
    }

    #[test]
    fn apply_register_is_idempotent() {
        let (_dir, reg) = open_registry();
        reg.apply_register("device-x", 42, 7, 99, 3, 100).unwrap();
        // Duplicate / reordered delivery re-applies identical state.
        reg.apply_register("device-x", 42, 7, 99, 3, 100).unwrap();
        let loaded = reg.get("device-x").unwrap().unwrap();
        assert_eq!(loaded.producer_id, 42);
        assert_eq!(loaded.current_epoch, 3);
    }

    #[test]
    fn failed_fence_retry_converges_follower_before_failover() {
        let (_leader_dir, leader) = open_registry();
        let (_follower_dir, follower) = open_registry();

        leader
            .apply_register("device-x", 42, 7, 99, 3, 100)
            .unwrap();
        follower
            .apply_register("device-x", 42, 7, 99, 3, 100)
            .unwrap();

        // The leader persisted epoch 9 locally, but its first fence proposal
        // failed before reaching the follower.
        leader.fence("device-x", 9).unwrap();
        assert_eq!(leader.get("device-x").unwrap().unwrap().current_epoch, 9);
        assert_eq!(follower.get("device-x").unwrap().unwrap().current_epoch, 3);

        // A retry replays the identical registration and then the max-wins
        // fence. The follower is safe to become leader only after both apply.
        follower
            .apply_register("device-x", 42, 7, 99, 9, 100)
            .unwrap();
        assert_eq!(follower.get("device-x").unwrap().unwrap().current_epoch, 3);
        follower.apply_fence("device-x", 9).unwrap();
        assert_eq!(follower.get("device-x").unwrap().unwrap().current_epoch, 9);
    }

    #[test]
    fn apply_fence_is_max_wins() {
        let (_dir, reg) = open_registry();
        reg.apply_register("device-x", 42, 7, 99, 5, 100).unwrap();

        // A lower epoch is ignored (out-of-order delivery cannot regress).
        reg.apply_fence("device-x", 3).unwrap();
        assert_eq!(reg.get("device-x").unwrap().unwrap().current_epoch, 5);

        // A higher epoch advances the fence.
        reg.apply_fence("device-x", 9).unwrap();
        assert_eq!(reg.get("device-x").unwrap().unwrap().current_epoch, 9);
    }

    #[test]
    fn apply_fence_missing_registration_is_noop() {
        let (_dir, reg) = open_registry();
        // A fence whose register entry has not yet applied is tolerated.
        reg.apply_fence("never-registered", 4).unwrap();
        assert!(reg.get("never-registered").unwrap().is_none());
    }
}
