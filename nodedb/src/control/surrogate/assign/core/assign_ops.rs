// SPDX-License-Identifier: BUSL-1.1

//! The keyed read/allocate/bind operations on [`super::SurrogateAssigner`]:
//! `assign`, `assign_bound`, `bind`, and `lookup_many`. Fresh draws live in
//! [`super::draw`].
//!
//! Every operation that can draw or ask a key's home is async: an empty
//! cluster batch and a home request are awaited, never blocked on.

use nodedb_types::{CollectionKey, TenantId};

use nodedb_types::Surrogate;

use super::home::HomeSurrogateAuthority;
use super::types::SurrogateAssigner;

impl SurrogateAssigner {
    /// Resolve `(collection, pk_bytes)` to the surrogate a write plans with.
    ///
    /// On a single node this is [`Self::assign_bound`]: the node is every
    /// key's home.
    ///
    /// In a cluster only the key's collection home binds a key, in its Raft
    /// log (see [`super::home`]). A binding this node's catalog holds is that
    /// bound value. Otherwise the home authority returns the value the home
    /// bound, and this node keeps it in its catalog. A cluster node never
    /// plans with a value of its own.
    pub async fn assign(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pk_bytes: &[u8],
    ) -> crate::Result<Surrogate> {
        let mut bound = self.assign_many(key, tenant_id, &[pk_bytes]).await?;
        bound.pop().ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "surrogate assign of a key in '{}' returned no surrogate",
                key.name()
            ),
        })
    }

    /// [`Self::assign`] for many keys of one collection, in `pks` order. In a
    /// cluster every key this node's catalog does not bind goes to the home
    /// authority in one batch.
    pub async fn assign_many(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pks: &[&[u8]],
    ) -> crate::Result<Vec<Surrogate>> {
        if !self.is_cluster()? {
            let mut bound = Vec::with_capacity(pks.len());
            for pk in pks {
                bound.push(self.assign_bound(key, tenant_id, pk).await?);
            }
            return Ok(bound);
        }
        let catalog = self.credential_store.catalog();
        let mut out: Vec<Option<Surrogate>> = Vec::with_capacity(pks.len());
        let mut missing: Vec<&[u8]> = Vec::new();
        let mut asked: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
        for &pk in pks {
            let hit = catalog.get_surrogate_for_pk(key, tenant_id, pk)?;
            if hit.is_none() && asked.insert(pk) {
                missing.push(pk);
            }
            out.push(hit);
        }
        if !missing.is_empty() {
            let winners = self
                .home_authority()?
                .assign(key, tenant_id, &missing)
                .await?;
            if winners.len() != missing.len() {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "the home of '{}' answered {} of {} keys",
                        key.name(),
                        winners.len(),
                        missing.len()
                    ),
                });
            }
            let mut kept: std::collections::HashMap<&[u8], Surrogate> =
                std::collections::HashMap::with_capacity(missing.len());
            for (&pk, winner) in missing.iter().zip(winners) {
                // The winner is final: every replica of the home binds it in
                // log order, so keeping it here agrees with them.
                kept.insert(pk, self.bind(key, tenant_id, pk, winner)?);
            }
            for (slot, &pk) in out.iter_mut().zip(pks) {
                if slot.is_none() {
                    *slot = kept.get(pk).copied();
                }
            }
        }
        out.into_iter()
            .map(|slot| {
                slot.ok_or_else(|| crate::Error::Internal {
                    detail: format!("a key in '{}' stayed unresolved", key.name()),
                })
            })
            .collect()
    }

    /// The installed home authority. A cluster node installs it before it
    /// plans anything.
    pub(super) fn home_authority(
        &self,
    ) -> crate::Result<&std::sync::Arc<dyn HomeSurrogateAuthority>> {
        self.home_authority
            .get()
            .ok_or_else(|| crate::Error::Internal {
                detail: "no surrogate home authority is installed on this cluster node yet; \
                     retry once the node has started"
                    .into(),
            })
    }

    /// Whether this node allocates from a cluster reservation.
    fn is_cluster(&self) -> crate::Result<bool> {
        let registry = self.registry.read().map_err(|_| crate::Error::Internal {
            detail: "surrogate registry lock poisoned".into(),
        })?;
        Ok(matches!(
            registry.mode(),
            crate::control::surrogate::registry::SurrogateRegistryMode::Cluster(_)
        ))
    }

    /// Resolve `(collection, pk_bytes)` to a stable surrogate and bind it in
    /// this node's catalog.
    ///
    /// - If a binding already exists, return it (no allocation, no flush).
    /// - Else: allocate one surrogate, persist the binding, and check
    ///   the registry's flush threshold; flush durably if tripped.
    ///
    /// Allocation + catalog write happen inside one critical section
    /// on the registry write-lock so the registry hwm and the
    /// persisted PK row cannot diverge under concurrent assigners.
    ///
    /// Only a node that is the key's home binds this way: a single node, or
    /// an apply that has no carried value to bind.
    pub async fn assign_bound(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pk_bytes: &[u8],
    ) -> crate::Result<Surrogate> {
        let catalog = self.credential_store.catalog();

        // Fast-path: existing binding. Done under a read lock — most
        // production calls land here once the per-collection working
        // set has been observed.
        if let Some(s) = catalog.get_surrogate_for_pk(key, tenant_id, pk_bytes)? {
            return Ok(s);
        }

        // Slow path: allocate + persist + maybe flush. The write lock
        // guards the (allocate, write-pk-row) pair so two concurrent
        // assigners can't both observe "missing", both allocate, and
        // both write — the second silently overwrites the
        // first's binding with a different surrogate.
        //
        // In cluster mode the allocation source is the node's reserved
        // batch (`try_alloc_reserved`). An empty batch is awaited from the
        // background refiller with the registry write lock released: the
        // applier installs the batch under a read guard, so a wait that held
        // the write lock deadlocks.
        loop {
            if let Some(surrogate) = self.try_assign_bound(catalog, key, tenant_id, pk_bytes)? {
                return Ok(surrogate);
            }
            self.await_reserved_batch().await?;
        }
    }

    /// One [`Self::assign_bound`] attempt under the registry write lock.
    /// `None` when this node's reserved batch is empty, after nudging the
    /// refill loop.
    fn try_assign_bound(
        &self,
        catalog: &crate::control::security::catalog::SystemCatalog,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Surrogate>> {
        let registry = self.registry_write()?;
        // Re-check inside the lock: another assigner can have raced us
        // between the fast-path read and the lock acquisition.
        if let Some(s) = catalog.get_surrogate_for_pk(key, tenant_id, pk_bytes)? {
            return Ok(Some(s));
        }
        let Some(surrogate) = self.alloc_locked(&registry)? else {
            drop(registry);
            self.refill_notify.notify_one();
            return Ok(None);
        };
        // Proactive top-up: a low batch nudges the background refiller so
        // the next reservation lands before the pool drains.
        self.nudge_refill_if_low(&registry);
        catalog.put_surrogate(key, tenant_id, pk_bytes, surrogate)?;
        // Emit a durable WAL bind before the lock releases. Order is
        // load-bearing: a crash between catalog write and bind append
        // is invisible (the catalog row is already on disk via redb's
        // own WAL); a crash before the catalog write leaves nothing
        // to recover; a crash between bind append and lock release is
        // recovered by replaying the bind into the catalog (idempotent
        // via the two-table overwrite).
        self.wal_appender
            .record_bind_to_wal(key, tenant_id, surrogate.as_u32(), pk_bytes)?;
        self.maybe_flush(&registry, catalog)?;
        Ok(Some(surrogate))
    }

    /// Read-only lookup of many keys of one collection, in `pks` order:
    /// the surrogate previously bound to each key, or `None` when the key
    /// names no row. Never allocates. Used by point-read/update/delete
    /// planning, where a missing binding means the row does not exist.
    ///
    /// This node's catalog answers first. In a cluster every key the catalog
    /// does not bind goes to the home authority in one batch, and every
    /// value the home answers is kept in this node's catalog. A single node
    /// is every key's home, so its catalog miss is the answer.
    pub async fn lookup_many(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pks: &[&[u8]],
    ) -> crate::Result<Vec<Option<Surrogate>>> {
        let mut out: Vec<Option<Surrogate>> = Vec::with_capacity(pks.len());
        let mut missing: Vec<&[u8]> = Vec::new();
        let mut asked: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
        for &pk in pks {
            let hit = self.lookup_bound(key, tenant_id, pk)?;
            if hit.is_none() && asked.insert(pk) {
                missing.push(pk);
            }
            out.push(hit);
        }
        if missing.is_empty() || !self.is_cluster()? {
            return Ok(out);
        }
        let answers = self
            .home_authority()?
            .lookup_many(key, tenant_id, &missing)
            .await?;
        if answers.len() != missing.len() {
            return Err(crate::Error::Internal {
                detail: format!(
                    "the home of '{}' answered {} of {} lookups",
                    key.name(),
                    answers.len(),
                    missing.len()
                ),
            });
        }
        let mut kept: std::collections::HashMap<&[u8], Surrogate> =
            std::collections::HashMap::new();
        for (&pk, answer) in missing.iter().zip(answers) {
            if let Some(winner) = answer {
                kept.insert(pk, self.bind(key, tenant_id, pk, winner)?);
            }
        }
        for (slot, &pk) in out.iter_mut().zip(pks) {
            if slot.is_none() {
                *slot = kept.get(pk).copied();
            }
        }
        Ok(out)
    }

    /// Whether this node obtains unbound keys from their collection home: a
    /// cluster node. A single node is every key's home.
    pub fn resolves_at_home(&self) -> crate::Result<bool> {
        self.is_cluster()
    }

    /// The binding this node's catalog holds for `(collection, pk_bytes)`,
    /// never a planned value. An apply and a collection home read this.
    pub fn lookup_bound(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Surrogate>> {
        let catalog = self.credential_store.catalog();
        catalog.get_surrogate_for_pk(key, tenant_id, pk_bytes)
    }

    /// Bind `(collection, pk_bytes)` to a *carried* surrogate without ever
    /// allocating, resolving concurrent carried values **first-wins** and
    /// returning the *authoritative* surrogate the caller must use.
    ///
    /// Used on the Raft apply path: a coordinator assigned the surrogate at
    /// plan time, embedded it in the plan, and carried it on the wire; the
    /// owner installs that identity rather than drawing a fresh (divergent)
    /// one from its own allocator. Because two different non-owner
    /// coordinators can each assign a *different* surrogate (from disjoint
    /// HiLo batches) for the *same* key, the owner must resolve this
    /// deterministically: the FIRST binding wins and every later carried
    /// value is discarded. The returned `Surrogate` is the authoritative one
    /// (the already-bound value when one exists, else the carried value
    /// persisted) and MUST be used as the storage key by the caller —
    /// otherwise the owner creates duplicate rows under different
    /// surrogates for the same key.
    ///
    /// - No catalog (in-memory test fixture): returns `Ok(surrogate)` — the
    ///   carried value is authoritative, nothing to persist (mirrors
    ///   `assign`'s catalog-less branch).
    /// - Binding already exists: returns `Ok(existing)` — first-wins, never
    ///   overwrites, discards the carried value even if it differs.
    /// - Otherwise: persist the binding + emit the durable WAL bind under the
    ///   registry write lock (same order as `assign`), `restore_hwm` so the
    ///   global watermark stays ahead of the carried value, and return the
    ///   now-bound `surrogate`.
    ///
    /// Replay/retry is idempotent: re-applying the same entry finds the
    /// existing binding in the pre-check and returns it without writing.
    ///
    /// Crucially this never touches `alloc_locked`/`maybe_flush`: the
    /// allocator counter must NOT advance on a bind — that burns a
    /// surrogate and diverge from the coordinator.
    pub fn bind(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pk_bytes: &[u8],
        surrogate: Surrogate,
    ) -> crate::Result<Surrogate> {
        let catalog = self.credential_store.catalog();

        // First-wins pre-check under a read lock: if any binding is already
        // installed (replay, retry, or a competing coordinator's carried
        // value applied first) it is authoritative — return it, never
        // overwrite, discard the carried value even if it differs.
        if let Some(existing) = catalog.get_surrogate_for_pk(key, tenant_id, pk_bytes)? {
            return Ok(existing);
        }

        // Hold the registry write lock across (re-check, persist binding,
        // WAL bind, hwm advance) so it is one critical section — same lock
        // discipline as `assign`, which also serializes on this write lock.
        // `restore_hwm` itself is atomic on the counter; we call it through
        // the held guard rather than re-locking (which deadlocks on
        // this std `RwLock`).
        let registry = self.registry_write()?;
        // Re-check under the lock (TOCTOU): a concurrent `assign`/`bind` on
        // this node can have written between the pre-check and the lock.
        // First-wins still applies — return the existing value.
        if let Some(existing) = catalog.get_surrogate_for_pk(key, tenant_id, pk_bytes)? {
            return Ok(existing);
        }
        catalog.put_surrogate(key, tenant_id, pk_bytes, surrogate)?;
        self.wal_appender
            .record_bind_to_wal(key, tenant_id, surrogate.as_u32(), pk_bytes)?;
        // Advance the local watermark past the carried value so a later
        // LOCAL `assign`/`assign_anonymous` on this node can never re-issue
        // it. Idempotent and monotonic — never lowers, never advances the
        // allocator's draw position (only the hwm floor).
        registry
            .restore_hwm(surrogate.as_u32())
            .map_err(|e| crate::Error::Internal {
                detail: format!("surrogate bind restore_hwm failed: {e}"),
            })?;
        Ok(surrogate)
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::SurrogateAssigner;
    use nodedb_types::{DatabaseId, Surrogate, TenantId};
    use std::sync::{Arc, RwLock};

    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::registry::SurrogateRegistry;
    use crate::control::surrogate::wal_appender::{NoopWalAppender, SurrogateWalAppender};

    fn open_test() -> (tempfile::TempDir, Arc<SurrogateAssigner>) {
        let dir = tempfile::tempdir().unwrap();
        let credentials = Arc::new(CredentialStore::open(&dir.path().join("system.redb")).unwrap());
        let reg = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        let a = Arc::new(SurrogateAssigner::new(reg, credentials, wal));
        (dir, a)
    }

    const T0: TenantId = TenantId::new(0);

    #[tokio::test]
    async fn assign_is_idempotent_for_same_pk() {
        let (_dir, a) = open_test();
        let s1 = a
            .assign(
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                T0,
                b"alice",
            )
            .await
            .unwrap();
        let s2 = a
            .assign(
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                T0,
                b"alice",
            )
            .await
            .unwrap();
        assert_eq!(s1, s2);
        assert_eq!(s1, Surrogate::new(1));
    }

    #[tokio::test]
    async fn assign_distinct_tenants_do_not_collide() {
        let (_dir, a) = open_test();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        let s1 = a
            .assign(
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                t1,
                b"alice",
            )
            .await
            .unwrap();
        let s2 = a
            .assign(
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                t2,
                b"alice",
            )
            .await
            .unwrap();
        assert_ne!(s1, s2);
    }

    #[tokio::test]
    async fn assign_distinct_pks_returns_distinct_surrogates() {
        let (_dir, a) = open_test();
        let s1 = a
            .assign(
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                T0,
                b"alice",
            )
            .await
            .unwrap();
        let s2 = a
            .assign(
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                T0,
                b"bob",
            )
            .await
            .unwrap();
        assert_ne!(s1, s2);
    }

    #[tokio::test]
    async fn assign_writes_reverse_binding() {
        let (_dir, a) = open_test();
        let s = a
            .assign(
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                T0,
                b"alice",
            )
            .await
            .unwrap();
        let cat = a.credential_store.catalog();
        assert_eq!(
            cat.get_pk_for_surrogate(
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                T0,
                s
            )
            .unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[tokio::test]
    async fn assign_persists_hwm_at_flush_threshold() {
        let (_dir, a) = open_test();
        // Allocate up to and across the 1024 ops threshold.
        let n = crate::control::surrogate::registry::FLUSH_OPS_THRESHOLD as usize;
        for i in 0..n {
            let pk = format!("u{i}");
            let _ = a
                .assign(
                    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                    T0,
                    pk.as_bytes(),
                )
                .await
                .unwrap();
        }
        // Either threshold (1024 ops or 200 ms elapsed) can fire
        // first; assert only that the catalog persisted *some*
        // checkpoint inside the (0, n] band.
        let cat = a.credential_store.catalog();
        let persisted = cat.get_surrogate_hwm().unwrap();
        assert!(persisted > 0 && persisted <= n as u32);
    }
}
