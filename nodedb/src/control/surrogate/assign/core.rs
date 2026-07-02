// SPDX-License-Identifier: BUSL-1.1

//! CP-side helper that turns a `(collection, pk_bytes)` into a stable
//! `Surrogate`, allocating from the registry on the first call and
//! returning the persisted value on every subsequent call (UPSERT
//! preserves the surrogate).
//!
//! Cross-cutting flush trigger: every successful allocation runs the
//! registry's `should_flush()` check; if true, we persist the new
//! high-watermark to both the catalog row (`_system.surrogate_hwm`)
//! and the WAL (`SurrogateAlloc` record) before returning. The two
//! writes form one logical checkpoint — if either fails we surface
//! the error to the caller rather than silently letting the registry
//! advance past a non-durable hwm.
//!
//! The cross-node HiLo reservation path (multi-node batch reservation +
//! the background refill loop that keeps the blocking metadata-Raft
//! round-trip OFF this hot path) lives in the sibling
//! [`super::cluster_reserve`] module.

use nodedb_types::{DatabaseId, TenantId};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock, Weak};

use tokio::sync::{Notify, oneshot};

use nodedb_types::Surrogate;

use super::super::persist::SurrogateHwmPersist;
use super::super::registry::SurrogateRegistry;
use super::super::wal_appender::SurrogateWalAppender;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Shared handle to the surrogate registry. Lives on `SharedState`
/// and is cloned (cheaply) into every CP path that allocates
/// surrogates.
///
/// The inner `RwLock` is held only for the duration of one
/// `assign_surrogate` call (write lock) — the registry's hot-path
/// `alloc_one` uses atomics, so the lock is uncontended.
pub type SurrogateRegistryHandle = Arc<RwLock<SurrogateRegistry>>;

/// CP-side surrogate assigner. Owning shape — bundles the registry,
/// the credential store (which exposes the catalog), and the WAL
/// appender so call sites only need to pass `(collection, pk_bytes)`.
///
/// Stored as `Arc<SurrogateAssigner>` on `SharedState`.
///
/// Fields are `pub(super)` so the sibling [`super::cluster_reserve`]
/// module's `impl` block (the cross-node reservation methods) can reach
/// them; they remain private outside the `assign` module.
pub struct SurrogateAssigner {
    pub(super) registry: SurrogateRegistryHandle,
    pub(super) credential_store: Arc<CredentialStore>,
    pub(super) wal_appender: Arc<dyn SurrogateWalAppender>,
    /// Weak handle to SharedState for Raft-mediated HWM proposals.
    /// Set after SharedState construction to break the Arc cycle.
    /// When set and a Raft cluster is active, the flush path proposes
    /// `MetadataEntry::SurrogateAlloc { hwm }` in addition to the
    /// local WAL record so all followers advance their HWM.
    pub(super) shared: std::sync::OnceLock<Weak<SharedState>>,
    /// Pending cluster-mode batch reservations keyed by `request_id`.
    /// `ensure_batch` registers a oneshot here before proposing; the
    /// metadata applier removes + fires it via `complete_reservation`
    /// once the carved `[start, end)` range is known at apply time.
    pub(super) pending_reservations: Mutex<HashMap<u64, oneshot::Sender<(u32, u32)>>>,
    /// Monotonic source of unique `request_id`s for reservations on
    /// this node. Only ever read/incremented locally.
    pub(super) next_request_id: AtomicU64,
    /// Serializes in-flight reservations so at most one batch is being
    /// reserved at a time per node. Without this, a burst of allocators
    /// that all observe an empty batch would each propose a reservation,
    /// over-reserving and wasting surrogate space.
    pub(super) reserve_gate: tokio::sync::Mutex<()>,
    /// Monotonic cache for `should_use_reservation`: set once the node
    /// first observes a multi-member metadata group, after which the
    /// per-row hot path skips the contended `cluster_topology` /
    /// `cluster_routing` RwLock reads.
    pub(super) reservation_latched: std::sync::atomic::AtomicBool,
    /// Wakes the background refill loop. The hot path nudges it (via
    /// `notify_one`) whenever a draw fails or the batch dips below the
    /// low-watermark; the refiller then performs the blocking reservation
    /// OFF the latency-critical insert path. `Notify` coalesces: a nudge
    /// while the refiller is already running is remembered as one pending
    /// permit, so no top-up is ever lost.
    pub(super) refill_notify: Arc<Notify>,
}

impl SurrogateAssigner {
    pub fn new(
        registry: SurrogateRegistryHandle,
        credential_store: Arc<CredentialStore>,
        wal_appender: Arc<dyn SurrogateWalAppender>,
    ) -> Self {
        Self {
            registry,
            credential_store,
            wal_appender,
            shared: std::sync::OnceLock::new(),
            pending_reservations: Mutex::new(HashMap::new()),
            next_request_id: AtomicU64::new(1),
            reserve_gate: tokio::sync::Mutex::new(()),
            reservation_latched: std::sync::atomic::AtomicBool::new(false),
            refill_notify: Arc::new(Notify::new()),
        }
    }

    /// Install a weak SharedState handle so the flush path can
    /// propose to Raft when in cluster mode. Called by `start_raft`
    /// after SharedState is fully wired.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }

    /// Highest surrogate ever issued by this assigner.  Used by `CLONE
    /// DATABASE` to capture the source's surrogate high-water at the
    /// AS-OF point — every binding allocated *after* this value belongs
    /// strictly to source-side writes that must NOT be visible from the
    /// resulting clone.  Returns `0` on a fresh assigner.
    pub fn current_hwm(&self) -> u32 {
        self.registry
            .read()
            .map(|reg| reg.current_hwm())
            .unwrap_or_else(|p| p.into_inner().current_hwm())
    }

    /// Resolve `(collection, pk_bytes)` to a stable surrogate.
    ///
    /// - If the credential store has no catalog (in-memory test fixture),
    ///   returns `Surrogate::ZERO`. Production state always wires a
    ///   redb-backed `CredentialStore::open` so this branch never fires.
    /// - If a binding already exists, return it (no allocation, no flush).
    /// - Else: allocate one surrogate, persist the binding, and check
    ///   the registry's flush threshold; flush durably if tripped.
    ///
    /// Allocation + catalog write happen inside one critical section
    /// on the registry write-lock so the registry hwm and the
    /// persisted PK row cannot diverge under concurrent assigners.
    pub fn assign(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<Surrogate> {
        let catalog = match self.credential_store.catalog().as_ref() {
            Some(c) => c,
            None => return Ok(Surrogate::ZERO),
        };

        // Fast-path: existing binding. Done under a read lock — most
        // production calls land here once the per-collection working
        // set has been observed.
        if let Some(s) =
            catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)?
        {
            return Ok(s);
        }

        // Slow path: allocate + persist + maybe flush. The write lock
        // guards the (allocate, write-pk-row) pair so two concurrent
        // assigners can't both observe "missing", both allocate, and
        // both write — the second would silently overwrite the
        // first's binding with a different surrogate.
        //
        // In cluster mode the allocation source is the node's reserved
        // batch (`try_alloc_reserved`); when the batch is empty the
        // background refiller normally has the next batch ready, so the
        // lock-free draw simply succeeds. The synchronous `ensure_batch`
        // refill remains only as a rare safety net — see
        // `cluster_reserve` for the full hot-path contract. It MUST run
        // WITHOUT the registry write lock held (the applier installs the
        // batch under a read guard; holding the write lock across the
        // wait would deadlock), so we drop the lock, refill, and retry.
        loop {
            let registry = self.registry_write()?;
            // Re-check inside the lock: another assigner may have raced
            // us between the read above and the lock acquisition.
            if let Some(s) =
                catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)?
            {
                return Ok(s);
            }
            let surrogate = match self.alloc_locked(&registry)? {
                Some(s) => {
                    // Proactive top-up: if the batch is running low, nudge
                    // the background refiller so the next reservation lands
                    // before the pool drains — keeping the blocking Raft
                    // round-trip OFF this latency-critical insert path.
                    self.nudge_refill_if_low(&registry);
                    s
                }
                None => {
                    // Cluster mode, empty batch: the background refiller
                    // hasn't caught up. Release the lock, nudge it, and fall
                    // back to a synchronous reservation as a rare safety net
                    // so liveness is preserved even if the refiller stalled.
                    drop(registry);
                    self.refill_notify.notify_one();
                    self.ensure_batch()?;
                    continue;
                }
            };
            catalog.put_surrogate(database_id, tenant_id, collection, pk_bytes, surrogate)?;
            // Emit a durable WAL bind before the lock releases. Order is
            // load-bearing: a crash between catalog write and bind append
            // is invisible (the catalog row is already on disk via redb's
            // own WAL); a crash before the catalog write leaves nothing
            // to recover; a crash between bind append and lock release is
            // recovered by replaying the bind into the catalog (idempotent
            // via the two-table overwrite).
            self.wal_appender.record_bind_to_wal(
                database_id,
                tenant_id,
                surrogate.as_u32(),
                collection,
                pk_bytes,
            )?;
            self.maybe_flush(&registry, catalog)?;
            return Ok(surrogate);
        }
    }

    /// Allocate a FRESH surrogate for a row with no content primary key — a
    /// collection whose primary key is the auto-generated `_rowid` (no
    /// `PRIMARY KEY` was declared). Unlike [`assign`](Self::assign), there is
    /// no fast-path lookup: every call allocates a new value, so N rows get N
    /// distinct surrogates instead of collapsing onto the binding for an empty
    /// key.
    ///
    /// The surrogate is self-bound (pk = its own decimal string). The Data
    /// Plane sets the row's `_rowid` field equal to this surrogate, so the
    /// self-binding makes a later `WHERE _rowid = N` point lookup resolve back
    /// to it, and reuses the same durable bind/flush machinery as `assign` so
    /// the hwm advance is persisted and Raft-proposed identically.
    pub fn assign_fresh(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> crate::Result<Surrogate> {
        let catalog = match self.credential_store.catalog().as_ref() {
            Some(c) => c,
            None => return Ok(Surrogate::ZERO),
        };

        // Allocate + self-bind + maybe-flush under the registry write lock,
        // with the same empty-batch refill fallback as the `assign` slow path.
        loop {
            let registry = self.registry_write()?;
            let surrogate = match self.alloc_locked(&registry)? {
                Some(s) => {
                    self.nudge_refill_if_low(&registry);
                    s
                }
                None => {
                    drop(registry);
                    self.refill_notify.notify_one();
                    self.ensure_batch()?;
                    continue;
                }
            };
            // Self-bind: pk is the surrogate's own decimal string, matching the
            // `_rowid` value the Data Plane writes (surrogate as i64) once run
            // through `sql_value_to_string`, so `WHERE _rowid = N` resolves.
            let pk = surrogate.as_u32().to_string();
            let pk_bytes = pk.as_bytes();
            catalog.put_surrogate(database_id, tenant_id, collection, pk_bytes, surrogate)?;
            self.wal_appender.record_bind_to_wal(
                database_id,
                tenant_id,
                surrogate.as_u32(),
                collection,
                pk_bytes,
            )?;
            self.maybe_flush(&registry, catalog)?;
            return Ok(surrogate);
        }
    }

    /// Local flush trigger: durably checkpoint the new hwm if the ops or
    /// elapsed-time threshold has tripped. This runs whenever the node is
    /// NOT using the cross-node reservation path — i.e. on a single-node
    /// (no Raft) deployment OR a single-member-with-Raft deployment. In
    /// the latter case the flush's `CombinedPersist` also proposes
    /// `SurrogateAlloc { hwm }` so the metadata watermark `G` stays in
    /// sync with the locally-allocated hwm; this gives a future node-join
    /// a correct base to advance past (see `should_use_reservation`
    /// follow-up (1)).
    ///
    /// When the reservation path IS in use (multi-member metadata group)
    /// this is a no-op — the global watermark is advanced and persisted
    /// by the `SurrogateReserve` apply path, so running the local flush
    /// here would double-advance `counter` (which is `G` in that mode)
    /// and corrupt determinism.
    pub(super) fn maybe_flush(
        &self,
        registry: &SurrogateRegistry,
        catalog: &SystemCatalog,
    ) -> crate::Result<()> {
        if self.should_use_reservation() {
            return Ok(());
        }
        if registry.should_flush() {
            let raft_shared = self.shared.get().and_then(|w| w.upgrade());
            let combined = CombinedPersist {
                catalog,
                wal_appender: self.wal_appender.as_ref(),
                raft_shared: raft_shared.as_deref(),
            };
            registry.flush(&combined)?;
        }
        Ok(())
    }

    /// Acquire a write lock on the registry, converting a poisoned-lock
    /// error into the crate's typed `Internal` error.
    pub(super) fn registry_write(
        &self,
    ) -> crate::Result<std::sync::RwLockWriteGuard<'_, SurrogateRegistry>> {
        self.registry.write().map_err(|_| crate::Error::Internal {
            detail: "surrogate registry lock poisoned".into(),
        })
    }

    /// Read-only lookup: return the surrogate previously bound to
    /// `(collection, pk_bytes)` without ever allocating or writing.
    /// Used by point-read/update/delete planning where a missing
    /// binding means the row does not exist (semantic no-op).
    ///
    /// When the credential store has no catalog (in-memory test
    /// fixture), returns `Some(Surrogate::ZERO)` — mirroring the
    /// `Surrogate::ZERO` allocation `assign` performs in the same
    /// catalog-less mode, so a write/read pair against an unwired
    /// catalog still resolves to the same identity.
    pub fn lookup(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Surrogate>> {
        let catalog = match self.credential_store.catalog().as_ref() {
            Some(c) => c,
            None => return Ok(Some(Surrogate::ZERO)),
        };
        catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)
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
    /// (the already-bound value when one exists, else the carried value just
    /// persisted) and MUST be used as the storage key by the caller —
    /// otherwise the owner would create duplicate rows under different
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
    /// allocator counter must NOT advance on a bind — that would burn a
    /// surrogate and diverge from the coordinator.
    pub fn bind(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
        surrogate: Surrogate,
    ) -> crate::Result<Surrogate> {
        let catalog = match self.credential_store.catalog().as_ref() {
            Some(c) => c,
            None => return Ok(surrogate),
        };

        // First-wins pre-check under a read lock: if any binding is already
        // installed (replay, retry, or a competing coordinator's carried
        // value applied first) it is authoritative — return it, never
        // overwrite, discard the carried value even if it differs.
        if let Some(existing) =
            catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)?
        {
            return Ok(existing);
        }

        // Hold the registry write lock across (re-check, persist binding,
        // WAL bind, hwm advance) so it is one critical section — same lock
        // discipline as `assign`, which also serializes on this write lock.
        // `restore_hwm` itself is atomic on the counter; we call it through
        // the held guard rather than re-locking (which would deadlock on
        // this std `RwLock`).
        let registry = self.registry_write()?;
        // Re-check under the lock (TOCTOU): a concurrent `assign`/`bind` on
        // this node could have written between the pre-check and the lock.
        // First-wins still applies — return the existing value.
        if let Some(existing) =
            catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)?
        {
            return Ok(existing);
        }
        catalog.put_surrogate(database_id, tenant_id, collection, pk_bytes, surrogate)?;
        self.wal_appender.record_bind_to_wal(
            database_id,
            tenant_id,
            surrogate.as_u32(),
            collection,
            pk_bytes,
        )?;
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

    /// Expose the registry handle for read access by the Raft applier.
    ///
    /// The returned `Arc<RwLock<SurrogateRegistry>>` is used by
    /// `MetadataCommitApplier` to call `restore_hwm` when a
    /// `SurrogateAlloc` entry commits on a follower.
    pub fn registry_handle(&self) -> &SurrogateRegistryHandle {
        &self.registry
    }

    /// Allocate a fresh surrogate for an entity that has no user-facing
    /// primary key (e.g. headless vector inserts). The surrogate is
    /// self-keyed in the catalog (`pk_bytes = surrogate.as_u32().to_be_bytes()`)
    /// so the binding round-trips homogeneously with named-PK rows: a
    /// later lookup via the self-bytes returns the same surrogate, and
    /// the reverse lookup returns the self-bytes back. Keeps the
    /// catalog single-shaped — no special-case "unbound" rows.
    pub fn assign_anonymous(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> crate::Result<Surrogate> {
        let catalog = match self.credential_store.catalog().as_ref() {
            Some(c) => c,
            None => return Ok(Surrogate::ZERO),
        };

        loop {
            let registry = self.registry_write()?;
            let surrogate = match self.alloc_locked(&registry)? {
                Some(s) => {
                    self.nudge_refill_if_low(&registry);
                    s
                }
                None => {
                    drop(registry);
                    self.refill_notify.notify_one();
                    self.ensure_batch()?;
                    continue;
                }
            };
            let self_bytes = surrogate.as_u32().to_be_bytes();
            catalog.put_surrogate(database_id, tenant_id, collection, &self_bytes, surrogate)?;
            self.wal_appender.record_bind_to_wal(
                database_id,
                tenant_id,
                surrogate.as_u32(),
                collection,
                &self_bytes,
            )?;
            self.maybe_flush(&registry, catalog)?;
            return Ok(surrogate);
        }
    }
}

/// `SurrogateHwmPersist` impl that writes the catalog row AND emits
/// the WAL record on every checkpoint. When `raft_shared` is set and
/// the node is in cluster mode, also proposes `SurrogateAlloc { hwm }`
/// to the metadata Raft group so followers advance their in-memory HWM.
struct CombinedPersist<'a> {
    catalog: &'a SystemCatalog,
    wal_appender: &'a dyn SurrogateWalAppender,
    /// Present when the Raft cluster is active; drives the Raft propose.
    raft_shared: Option<&'a SharedState>,
}

impl SurrogateHwmPersist for CombinedPersist<'_> {
    fn checkpoint(&self, hwm: u32) -> crate::Result<()> {
        self.catalog.put_surrogate_hwm(hwm)?;
        self.wal_appender.record_alloc_to_wal(hwm)?;
        // Propose to Raft when in cluster mode so followers advance
        // their in-memory HWM. Failure is non-fatal for the local
        // write (which is already durable via the catalog and WAL);
        // the follower will catch up on the next flush cycle or via
        // snapshot. We log at warn so operators can detect systemic
        // issues without breaking the local write path.
        if let Some(shared) = self.raft_shared
            && let Err(e) = crate::control::metadata_proposer::propose_surrogate_hwm(shared, hwm)
        {
            tracing::warn!(hwm, error = %e, "surrogate hwm raft propose failed; followers may lag");
        }
        Ok(())
    }

    fn load(&self) -> crate::Result<u32> {
        self.catalog.get_surrogate_hwm()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::wal_appender::NoopWalAppender;

    fn open_test() -> (tempfile::TempDir, Arc<SurrogateAssigner>) {
        let dir = tempfile::tempdir().unwrap();
        let credentials = Arc::new(CredentialStore::open(&dir.path().join("system.redb")).unwrap());
        let reg = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        let a = Arc::new(SurrogateAssigner::new(reg, credentials, wal));
        (dir, a)
    }

    const T0: TenantId = TenantId::new(0);

    #[test]
    fn assign_is_idempotent_for_same_pk() {
        let (_dir, a) = open_test();
        let s1 = a
            .assign(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
        let s2 = a
            .assign(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
        assert_eq!(s1, s2);
        assert_eq!(s1, Surrogate::new(1));
    }

    #[test]
    fn assign_distinct_tenants_do_not_collide() {
        let (_dir, a) = open_test();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        let s1 = a
            .assign(DatabaseId::DEFAULT, t1, "users", b"alice")
            .unwrap();
        let s2 = a
            .assign(DatabaseId::DEFAULT, t2, "users", b"alice")
            .unwrap();
        assert_ne!(s1, s2);
    }

    #[test]
    fn assign_distinct_pks_returns_distinct_surrogates() {
        let (_dir, a) = open_test();
        let s1 = a
            .assign(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
        let s2 = a.assign(DatabaseId::DEFAULT, T0, "users", b"bob").unwrap();
        assert_ne!(s1, s2);
    }

    #[test]
    fn assign_writes_reverse_binding() {
        let (_dir, a) = open_test();
        let s = a
            .assign(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
        let cat = a.credential_store.catalog().as_ref().unwrap();
        assert_eq!(
            cat.get_pk_for_surrogate(DatabaseId::DEFAULT, T0, "users", s)
                .unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[test]
    fn assign_persists_hwm_at_flush_threshold() {
        let (_dir, a) = open_test();
        // Allocate just up to and across the 1024 ops threshold.
        let n = crate::control::surrogate::registry::FLUSH_OPS_THRESHOLD as usize;
        for i in 0..n {
            let pk = format!("u{i}");
            let _ = a
                .assign(DatabaseId::DEFAULT, T0, "users", pk.as_bytes())
                .unwrap();
        }
        // Either threshold (1024 ops or 200 ms elapsed) may fire
        // first; assert only that the catalog persisted *some*
        // checkpoint inside the (0, n] band.
        let cat = a.credential_store.catalog().as_ref().unwrap();
        let persisted = cat.get_surrogate_hwm().unwrap();
        assert!(persisted > 0 && persisted <= n as u32);
    }
}
