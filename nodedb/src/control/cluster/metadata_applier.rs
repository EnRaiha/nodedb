// SPDX-License-Identifier: BUSL-1.1

//! Production metadata-group commit applier.
//!
//! Single branch for DDL: decode the opaque `CatalogDdl { payload }`
//! as a host-side [`CatalogEntry`], write through to `SystemCatalog`
//! redb via [`catalog_entry::apply_to`], and spawn the post-apply
//! side effects (Data Plane register, sequence registry sync, etc.).
//! All 16 per-DDL-object types are handled by adding a variant to
//! `CatalogEntry` — nothing in this file changes per type.
//!
//! The applier broadcasts `CatalogChangeEvent` (for future
//! prepared-statement / catalog cache invalidation). The per-group
//! apply watermark is maintained by the Raft tick loop directly via
//! [`nodedb_cluster::GroupAppliedWatchers`] — the applier no longer
//! owns its own watcher because that primitive is now keyed by
//! `group_id` and shared across every Raft group on the node.

use std::sync::{Arc, OnceLock, RwLock, Weak};

use tokio::sync::broadcast;
use tracing::{debug, warn};

use nodedb_cluster::{MetadataApplier, MetadataCache, MetadataEntry, decode_entry};

use crate::control::catalog_entry;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

use metadata_applier_audit::{apply_ca_trust_change, emit_ddl_audit};

#[path = "metadata_applier_audit.rs"]
mod metadata_applier_audit;

/// Broadcast channel capacity — small, because consumers are
/// internal subsystems that keep up or are lagged intentionally.
pub const CATALOG_CHANNEL_CAPACITY: usize = 64;

/// Event published on every committed metadata entry.
#[derive(Debug, Clone)]
pub struct CatalogChangeEvent {
    pub applied_index: u64,
}

/// Production `MetadataApplier` installed on the `RaftLoop`.
pub struct MetadataCommitApplier {
    cache: Arc<RwLock<MetadataCache>>,
    catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
    credentials: Arc<CredentialStore>,
    /// Weak handle to `SharedState`. Installed by `start_raft` after
    /// construction so the applier can spawn async post-apply side
    /// effects (Data Plane register on `PutCollection`,
    /// `sequence_registry.create` on `PutSequence`, etc.). Weak to
    /// break the Arc cycle (SharedState → raft loop → applier →
    /// SharedState). `None` in unit tests.
    shared: OnceLock<Weak<SharedState>>,
}

impl MetadataCommitApplier {
    pub fn new(
        cache: Arc<RwLock<MetadataCache>>,
        catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
        credentials: Arc<CredentialStore>,
    ) -> Self {
        Self {
            cache,
            catalog_change_tx,
            credentials,
            shared: OnceLock::new(),
        }
    }

    /// Install a weak handle to `SharedState` so the applier can
    /// spawn post-apply side effects. Must be called **before** the
    /// raft loop starts ticking; `start_raft` does this as part of
    /// its construction sequence.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }

    /// Apply a single decoded `MetadataEntry`'s host-side effects.
    ///
    /// - `CatalogDdl` → decode payload as `CatalogEntry`, write
    ///   through to redb via `catalog_entry::apply_to`, spawn async
    ///   post-apply side effects if `SharedState` is reachable.
    /// - Non-DDL variants (topology, routing, lease, version) have
    ///   no host-side redb effects in this crate — the cluster crate
    ///   already tracks them in the `MetadataCache`.
    ///
    /// `Ok(())` means the entry is fully applied (or its only failure was a
    /// best-effort durability shortcut whose source of truth is the replicated
    /// log). `Err` means a durable, replicated-state write failed — the caller
    /// MUST NOT advance the apply watermark past this entry, so Raft re-delivers
    /// it and the apply is retried. This is the canonical "never advance the
    /// state machine past an entry you couldn't apply" rule: a transient I/O
    /// failure clears on retry; a persistent one leaves the watermark loudly
    /// stuck (proposer waiters time out) rather than silently diverging from the
    /// quorum with a false-success ACK.
    fn apply_host_side_effects(
        &self,
        entry: &MetadataEntry,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        // Atomic batches unpack one level: the sub-entries are
        // applied individually so each gets its own audit record
        // stamped with the same raft_index (they committed at the
        // same log position).
        if let MetadataEntry::Batch { entries } = entry {
            for sub in entries {
                self.apply_host_side_effects(sub, raft_index)?;
            }
            return Ok(());
        }

        // Handle non-CatalogDdl variants that still have host-side
        // effects. Drain start/end land on `shared.lease_drain` on
        // every node so the next `force_refresh_lease` check sees
        // the replicated drain state.
        match entry {
            MetadataEntry::DescriptorDrainStart {
                descriptor_id,
                up_to_version,
                expires_at,
            } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    shared.lease_drain.install_start(
                        descriptor_id.clone(),
                        *up_to_version,
                        *expires_at,
                    );
                    debug!(
                        descriptor = ?descriptor_id,
                        up_to_version,
                        "drain_start applied to host tracker"
                    );
                }
                return Ok(());
            }
            MetadataEntry::DescriptorDrainEnd { descriptor_id } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    shared.lease_drain.install_end(descriptor_id);
                    debug!(
                        descriptor = ?descriptor_id,
                        "drain_end applied to host tracker"
                    );
                }
                return Ok(());
            }
            MetadataEntry::CaTrustChange {
                add_ca_cert,
                remove_ca_fingerprint,
            } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    apply_ca_trust_change(
                        &shared,
                        add_ca_cert.as_deref(),
                        remove_ca_fingerprint.as_ref(),
                        raft_index,
                    );
                }
                return Ok(());
            }
            MetadataEntry::SurrogateAlloc { hwm } => {
                // Advance the in-memory surrogate high-watermark on every
                // node. `restore_hwm` is idempotent and monotonic: calling
                // it with a value at or below the current HWM is a no-op,
                // so duplicate or reordered delivery cannot push the
                // counter backwards. Also persist the hwm to the catalog so
                // the local node survives a restart without re-reading the
                // full log.
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    let reg = shared
                        .surrogate_assigner
                        .registry_handle()
                        .read()
                        .unwrap_or_else(|p| p.into_inner());
                    let restored = reg.restore_hwm(*hwm);
                    drop(reg);
                    // The in-memory HWM advance is correctness-critical: if it
                    // fails this replica could re-issue a surrogate the cluster
                    // already allocated. Do not advance past this entry — retry.
                    if let Err(e) = restored {
                        warn!(hwm, error = %e, "surrogate_alloc apply: restore_hwm failed — halting watermark for retry");
                        return Err(crate::Error::Internal {
                            detail: format!("surrogate_alloc apply: restore_hwm failed: {e}"),
                        });
                    }
                    // Best-effort catalog persist: a failure means the
                    // next restart will re-derive the HWM from the log
                    // (the log is the source of truth), which is correct —
                    // just slightly slower. Tolerate and continue.
                    if let Some(catalog) = self.credentials.catalog()
                        && let Err(e) = catalog.put_surrogate_hwm(*hwm)
                    {
                        warn!(
                            hwm,
                            error = %e,
                            "surrogate_alloc apply: failed to persist hwm to catalog (tolerable; log is authoritative)"
                        );
                    }
                    debug!(hwm, raft_index, "surrogate hwm advanced via raft");
                }
                return Ok(());
            }
            MetadataEntry::SurrogateReserve {
                node_id,
                request_id,
                batch_size,
            } => {
                // HiLo batch reservation. The carved range is computed
                // HERE — deterministically — by advancing the global
                // watermark on EVERY node in identical Raft log order, so
                // all nodes agree which `[start, end)` this reservation
                // owns and no two nodes ever mint the same surrogate.
                //
                // RESTART SAFETY (critical): the metadata Raft group has no
                // snapshot, so on every (re)start `last_applied` resets to 0
                // and the FULL committed log is replayed from index 1. This
                // arm therefore runs once per historical reservation on each
                // start. Three consequences drive the design:
                //
                //   1. `G` must advance EXACTLY ONCE per reservation across the
                //      lifetime of the node — not once per replay. The carved
                //      hwm AND the applied-reserve cursor (`raft_index`) are
                //      persisted to the catalog ATOMICALLY on first
                //      application; on restart the registry is seeded with both
                //      via `from_persisted`, and `reserve_at_index` skips every
                //      reservation whose index `<= cursor` (already folded into
                //      the seeded `G`). Entries committed-but-not-yet-persisted
                //      before a crash have index `> cursor` and are re-applied
                //      (correct — they were not in the seed). Because the carve
                //      is computed identically on every node, the persisted hwm
                //      is EQUAL cluster-wide.
                //
                //   2. The reserved batch must NOT be installed during replay.
                //      A node that crashed mid-batch already consumed part of
                //      its pre-crash `[start, end)`; re-installing it on replay
                //      would hand those surrogates out AGAIN. So `G` advances
                //      (deterministic, every node) but the batch install is
                //      gated on a LIVE pending waiter, which only exists during
                //      a genuine in-process reservation (`pending_reservations`
                //      is empty after restart). On replay no waiter exists → no
                //      batch is installed → the node reserves a fresh batch on
                //      first alloc; the crashed node's pre-crash batch tail is
                //      abandoned (the declared gap-tolerant design).
                //
                //   3. A replayed/duplicate reservation (`reserve_at_index`
                //      returns `None`) is a strict no-op: `G` is not advanced,
                //      nothing is persisted, no batch is installed.
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    // Read guard is sufficient: `reserve_at_index` mutates via
                    // interior atomics (counter + last_reserve_index). Taking a
                    // write guard here would risk deadlocking the allocation
                    // path, which holds no registry lock across the propose+wait
                    // but does re-take it to retry.
                    let reg = shared
                        .surrogate_assigner
                        .registry_handle()
                        .read()
                        .unwrap_or_else(|p| p.into_inner());
                    // Advancing the global watermark is correctness-critical and
                    // must be deterministic across nodes incl. replay: an
                    // exhaustion error must NOT advance the apply watermark past
                    // this entry, or replicas would diverge on `G`. Surface it
                    // so Raft re-delivers.
                    let reserved = match reg.reserve_at_index(raft_index, *batch_size) {
                        Ok(r) => r,
                        Err(e) => {
                            drop(reg);
                            warn!(
                                node_id,
                                request_id,
                                batch_size,
                                error = %e,
                                "surrogate_reserve apply: reserve_at_index failed — halting watermark for retry"
                            );
                            return Err(crate::Error::Internal {
                                detail: format!(
                                    "surrogate_reserve apply: reserve_at_index failed: {e}"
                                ),
                            });
                        }
                    };
                    drop(reg);

                    let Some((start, end)) = reserved else {
                        // Already applied (full-log replay / duplicate
                        // delivery): do NOT advance `G`, do NOT persist, do NOT
                        // install a batch. Advancing the apply watermark past a
                        // replayed entry is correct — its effect is already in
                        // the seeded state.
                        debug!(
                            node_id,
                            request_id,
                            raft_index,
                            "surrogate_reserve apply: index already applied (replay/dup) — skipped"
                        );
                        return Ok(());
                    };

                    // First application: persist `(hwm = end - 1, cursor =
                    // raft_index)` ATOMICALLY so a restart can skip this
                    // reservation on replay (no double-count) and seed an
                    // already-equal `G` on every node. Best-effort (warn on
                    // fail) like the `SurrogateAlloc` arm: if the persist fails,
                    // the log is still authoritative and the next restart
                    // re-derives `G` by replaying from the last durable cursor —
                    // correct, just slightly slower. The hwm and cursor are
                    // written together in one redb txn, so a crash can never
                    // leave them inconsistent.
                    if let Some(catalog) = self.credentials.catalog()
                        && let Err(e) = catalog.put_surrogate_reserve_state(end - 1, raft_index)
                    {
                        warn!(
                            node_id,
                            request_id,
                            hwm = end - 1,
                            raft_index,
                            error = %e,
                            "surrogate_reserve apply: failed to persist reserve state to catalog \
                             (tolerable; log is authoritative)"
                        );
                    }

                    if *node_id == shared.node_id {
                        // Install the batch + wake the waiter ONLY when a live
                        // pending reservation exists for this `request_id`.
                        // During replay there is no waiter, so
                        // `complete_reservation` is a no-op — but replay never
                        // reaches here anyway (it returns `None` above). The
                        // install happens BEFORE the wake (inside
                        // `complete_reservation`) so the woken allocator
                        // immediately observes a non-empty batch.
                        shared
                            .surrogate_assigner
                            .complete_reservation(*request_id, start, end);
                    }
                    debug!(
                        node_id,
                        request_id, start, end, raft_index, "surrogate batch reserved via raft"
                    );
                }
                return Ok(());
            }
            MetadataEntry::SyncProducerRegister {
                lite_id,
                producer_id,
                tenant_id,
                epoch,
                created_ms,
            } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    let Some(registry) = shared.producer_registry.as_deref() else {
                        return Ok(());
                    };
                    // The registration row is durable replicated state. A write
                    // failure must not advance the watermark — Raft re-delivers
                    // and `apply_register` is idempotent, so the retry is safe.
                    if let Err(e) = registry.apply_register(
                        lite_id,
                        *producer_id,
                        *tenant_id,
                        *epoch,
                        *created_ms,
                    ) {
                        warn!(
                            lite_id = %lite_id,
                            producer_id,
                            error = %e,
                            "sync_producer_register apply failed — halting watermark for retry"
                        );
                        return Err(crate::Error::Internal {
                            detail: format!("sync_producer_register apply failed: {e}"),
                        });
                    }
                    debug!(lite_id = %lite_id, producer_id, raft_index, "sync producer registered via raft");
                }
                return Ok(());
            }
            MetadataEntry::SyncProducerFence { lite_id, new_epoch } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    let Some(registry) = shared.producer_registry.as_deref() else {
                        return Ok(());
                    };
                    // Durable epoch advance; `apply_fence` is idempotent
                    // (max-wins) so re-delivery on failure is safe.
                    if let Err(e) = registry.apply_fence(lite_id, *new_epoch) {
                        warn!(
                            lite_id = %lite_id,
                            new_epoch,
                            error = %e,
                            "sync_producer_fence apply failed — halting watermark for retry"
                        );
                        return Err(crate::Error::Internal {
                            detail: format!("sync_producer_fence apply failed: {e}"),
                        });
                    }
                    debug!(lite_id = %lite_id, new_epoch, raft_index, "sync producer fenced via raft");
                }
                return Ok(());
            }
            _ => {}
        }

        let Some(catalog) = self.credentials.catalog() else {
            return Ok(());
        };
        let (payload, audit) = match entry {
            MetadataEntry::CatalogDdl { payload } => (payload, None),
            MetadataEntry::CatalogDdlAudited {
                payload,
                auth_user_id,
                auth_user_name,
                sql_text,
            } => (
                payload,
                Some((
                    auth_user_id.clone(),
                    auth_user_name.clone(),
                    sql_text.clone(),
                )),
            ),
            _ => return Ok(()),
        };
        let catalog_entry = match catalog_entry::decode(payload) {
            Ok(e) => e,
            Err(e) => {
                // Deterministic poison: a corrupt payload will not decode on
                // retry either, so skip it (advance) rather than wedge the
                // group. Loud because a committed-but-undecodable entry is a
                // serious version-skew / corruption signal.
                warn!(error = %e, "metadata applier: failed to decode CatalogEntry payload");
                return Ok(());
            }
        };

        // Stamp `descriptor_version` + `modification_hlc` for every
        // `Put*` variant that carries them. Gated on the rolling
        // upgrade flag — in mixed-version clusters the older nodes
        // do not have the stamp logic, so we leave the entry's
        // sentinel `0` / `Hlc::ZERO` in place and let resolvers
        // treat it as "unknown, always re-fetch". Only the proposing
        // node's apply path observes the gate; followers run the
        // same applier and will reach the same conclusion because
        // every node observes the same live topology (same
        // `wire_version` on every NodeInfo, replicated via the
        // gossip path).
        let stamped = if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            let compat = !shared.cluster_version_view().can_activate_feature(
                crate::control::rolling_upgrade::DESCRIPTOR_VERSIONING_VERSION,
            );
            if compat {
                catalog_entry
            } else {
                catalog_entry::descriptor_stamp::stamp(catalog_entry, &shared.hlc_clock, catalog)
            }
        } else {
            // Unit tests construct the applier without a SharedState.
            // They don't care about descriptor versioning — leave
            // the entry unstamped so existing assertions on
            // `descriptor_version == 0` still hold.
            catalog_entry
        };

        debug!(kind = stamped.kind(), "catalog_entry: applying to redb");
        catalog_entry::apply::apply_to(&stamped, catalog);
        // Implicit drain clear: if the entry is a `Put*` for one
        // of the six stamped descriptor types, the DDL that was
        // waiting on drain has now committed — remove the drain
        // entry from every node's host tracker. Happens before
        // post_apply so a subsequent `acquire_lease` fired from
        // post_apply doesn't see a stale drain.
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            if let Some(drained_id) =
                crate::control::lease::drain_propose::descriptor_id_for_implicit_clear(&stamped)
            {
                shared.lease_drain.install_end(&drained_id);
            }
            // Run synchronous post-apply side effects INLINE so every
            // in-memory cache update (install_replicated_user,
            // install_replicated_owner, etc.) is visible before the
            // watcher bump. Any reader that observes `applied_index`
            // moving past `last` is guaranteed to see the sync side
            // effects of every entry up to `last`.
            //
            // `PutCollection` Register dispatch runs synchronously
            // (block_in_place) inside spawn_post_apply_async_side_effects
            // and IS part of the applied-index contract: the watcher
            // only bumps after doc_configs is populated on every core,
            // so subsequent scans always find the schema.
            catalog_entry::post_apply::apply_post_apply_side_effects_sync(&stamped, &shared);

            // Emit a DdlChange audit record on every replica.
            // Executed BEFORE spawning async post-apply side effects
            // so the audit entry lands synchronously with the rest of
            // the commit.
            emit_ddl_audit(&shared, raft_index, &stamped, audit.as_ref());

            catalog_entry::post_apply::spawn_post_apply_async_side_effects(
                stamped, shared, raft_index,
            );
        }
        Ok(())
    }
}

impl MetadataApplier for MetadataCommitApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        // `last` is the highest index whose state is GUARANTEED visible. We
        // only advance it past an entry that fully applied — a durable apply
        // failure stops the batch here so Raft re-delivers the entry and the
        // apply is retried (never a silent divergence with a false-success ACK).
        let mut last = 0u64;
        for (index, data) in entries {
            if data.is_empty() {
                // Raft no-op: nothing to apply, safe to advance.
                last = *index;
                continue;
            }
            let entry = match decode_entry(data) {
                Ok(e) => e,
                Err(e) => {
                    // Undecodable committed entry: deterministic poison, won't
                    // decode on retry — skip (advance) rather than wedge.
                    warn!(index = *index, error = %e, "metadata decode failed");
                    last = *index;
                    continue;
                }
            };
            // 1. Cluster-owned cache state (topology, routing,
            //    leases, catalog_entries_applied counter).
            {
                let mut guard = self.cache.write().unwrap_or_else(|p| p.into_inner());
                guard.apply(*index, &entry);
            }
            // 2. Host side effects (redb writeback + async post-apply). A
            //    durable failure halts the watermark at the last good index.
            if let Err(e) = self.apply_host_side_effects(&entry, *index) {
                warn!(
                    index = *index,
                    last_applied = last,
                    error = %e,
                    "metadata apply: durable host-side effect failed; not advancing \
                     watermark — Raft will re-deliver and retry"
                );
                break;
            }
            last = *index;
        }
        if last > 0 {
            // The Raft tick loop bumps the per-group apply watcher
            // directly after `advance_applied`; this applier only
            // owns the catalog-change broadcast.
            let _ = self.catalog_change_tx.send(CatalogChangeEvent {
                applied_index: last,
            });
            debug!(
                applied_index = last,
                "metadata applier broadcast catalog-change event"
            );
        }
        last
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::DatabaseId;

    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::security::catalog::StoredCollection;
    use nodedb_cluster::encode_entry;

    fn make_applier() -> (
        MetadataCommitApplier,
        Arc<RwLock<MetadataCache>>,
        Arc<CredentialStore>,
        tempfile::TempDir,
    ) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let credentials =
            Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let (tx, _rx) = broadcast::channel(16);
        let applier = MetadataCommitApplier::new(cache.clone(), tx, credentials.clone());
        (applier, cache, credentials, tmp)
    }

    fn put_collection_entry(name: &str) -> MetadataEntry {
        let stored = StoredCollection::new(7, name, "tester");
        let catalog_entry = CatalogEntry::PutCollection(Box::new(stored));
        MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&catalog_entry).unwrap(),
        }
    }

    #[test]
    fn apply_put_collection_writes_through_to_redb() {
        let (applier, cache, credentials, _tmp) = make_applier();
        let bytes = encode_entry(&put_collection_entry("orders")).unwrap();
        assert_eq!(applier.apply(&[(11, bytes)]), 11);

        let cache_guard = cache.read().unwrap();
        assert_eq!(cache_guard.applied_index, 11);
        assert_eq!(cache_guard.catalog_entries_applied, 1);
        drop(cache_guard);

        let loaded = credentials
            .catalog()
            .as_ref()
            .unwrap()
            .get_collection(DatabaseId::DEFAULT, 7, "orders")
            .unwrap()
            .expect("present");
        assert_eq!(loaded.name, "orders");
        assert_eq!(loaded.owner, "tester");
    }

    #[test]
    fn apply_deactivate_preserves_record() {
        let (applier, _cache, credentials, _tmp) = make_applier();

        // Seed.
        applier.apply(&[(1, encode_entry(&put_collection_entry("archived")).unwrap())]);

        let drop_entry = MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&CatalogEntry::DeactivateCollection {
                tenant_id: 7,
                name: "archived".into(),
            })
            .unwrap(),
        };
        applier.apply(&[(2, encode_entry(&drop_entry).unwrap())]);

        let loaded = credentials
            .catalog()
            .as_ref()
            .unwrap()
            .get_collection(DatabaseId::DEFAULT, 7, "archived")
            .unwrap()
            .expect("preserved");
        assert!(!loaded.is_active);
    }

    #[test]
    fn apply_empty_batch_is_noop() {
        let (applier, _cache, _credentials, _tmp) = make_applier();
        assert_eq!(applier.apply(&[]), 0);
    }
}
