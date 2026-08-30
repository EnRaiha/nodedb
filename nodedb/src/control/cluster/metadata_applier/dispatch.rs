// SPDX-License-Identifier: BUSL-1.1

//! Top-level `apply_host_side_effects` dispatcher and the
//! `impl MetadataApplier for MetadataCommitApplier` trait entry point.

use tracing::{debug, error, warn};

use nodedb_cluster::{MetadataApplier, MetadataEntry, RoutingChange, TopologyChange, decode_entry};

use super::types::{CatalogChangeEvent, MetadataCommitApplier};

impl MetadataCommitApplier {
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
    pub(super) fn apply_host_side_effects(
        &self,
        entry: &MetadataEntry,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        // A prepared DDL is conditionally applied under the replicated owner
        // token. A superseded proposal is a deterministic no-op: rejecting a
        // committed stale token would wedge the Raft apply watermark forever.
        if let MetadataEntry::DdlPrepared { token, entry } = entry {
            let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) else {
                return Ok(());
            };
            let owns_lease = shared
                .metadata_ddl_owner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_some_and(|(current, _)| current == *token);
            if !owns_lease {
                debug!(token, raft_index, "skipping superseded prepared DDL");
                return Ok(());
            }
            self.apply_host_side_effects(entry.as_ref(), raft_index)?;
            shared
                .metadata_ddl_applied_token
                .store(*token, std::sync::atomic::Ordering::Release);
            return Ok(());
        }

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
            } => return self.apply_drain_start(descriptor_id, *up_to_version, *expires_at),
            MetadataEntry::DescriptorDrainEnd { descriptor_id } => {
                return self.apply_drain_end(descriptor_id);
            }
            MetadataEntry::DdlPrepareAcquire { token } => {
                if let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) {
                    let mut owner = shared
                        .metadata_ddl_owner
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if owner.is_none() || owner.is_some_and(|(current, _)| current == *token) {
                        *owner = Some((*token, std::time::Instant::now()));
                    }
                }
                return Ok(());
            }
            MetadataEntry::DdlPendingPropose {
                token,
                objects,
                proposed_at,
            } => {
                return self.apply_ddl_pending_propose(*token, objects, *proposed_at);
            }
            MetadataEntry::DdlPendingFinalize { token } => {
                return self.apply_ddl_pending_finalize(*token, raft_index);
            }
            MetadataEntry::DdlPendingCancel { token } => {
                return self.apply_ddl_pending_cancel(*token);
            }
            MetadataEntry::DdlPrepareRelease { token } => {
                if let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) {
                    let mut owner = shared
                        .metadata_ddl_owner
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if owner.is_some_and(|(current, _)| current == *token) {
                        *owner = None;
                    }
                }
                return Ok(());
            }
            MetadataEntry::CaTrustChange {
                add_ca_cert,
                remove_ca_fingerprint,
            } => {
                return self.apply_ca_trust(
                    add_ca_cert.as_deref(),
                    remove_ca_fingerprint.as_ref(),
                    raft_index,
                );
            }
            MetadataEntry::SurrogateAlloc { hwm } => {
                return self.apply_surrogate_alloc(*hwm, raft_index);
            }
            MetadataEntry::SurrogateReserve {
                node_id,
                request_id,
                batch_size,
            } => {
                return self.apply_surrogate_reserve(
                    *node_id,
                    *request_id,
                    *batch_size,
                    raft_index,
                );
            }
            MetadataEntry::SyncProducerRegister {
                lite_id,
                producer_id,
                tenant_id,
                user_id,
                epoch,
                created_ms,
            } => {
                return self.apply_sync_producer_register(
                    super::sync_and_routing::SyncProducerRegistrationApply {
                        lite_id,
                        producer_id: *producer_id,
                        tenant_id: *tenant_id,
                        user_id: *user_id,
                        epoch: *epoch,
                        created_ms: *created_ms,
                    },
                    raft_index,
                );
            }
            MetadataEntry::SyncProducerFence { lite_id, new_epoch } => {
                return self.apply_sync_producer_fence(lite_id, *new_epoch, raft_index);
            }
            MetadataEntry::SyncPeerBind {
                database_id,
                tenant_id,
                collection,
                peer_id,
                producer_id,
                bound_ms,
            } => {
                return self.apply_sync_peer_bind(
                    super::sync_and_routing::SyncPeerBindApply {
                        database_id: *database_id,
                        tenant_id: *tenant_id,
                        collection,
                        peer_id: *peer_id,
                        producer_id: *producer_id,
                        bound_ms: *bound_ms,
                    },
                    raft_index,
                );
            }
            MetadataEntry::JoinTokenTransition {
                token_hash,
                transition,
                ts_ms,
            } => {
                nodedb_cluster::apply_token_transition_to_mirror(
                    &self.token_state,
                    *token_hash,
                    transition,
                    *ts_ms,
                );
                let state = self
                    .token_state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(token_hash)
                    .cloned();
                if let Some(state) = state {
                    self.credentials.catalog().put_join_token_state(&state)?;
                }
                return Ok(());
            }
            MetadataEntry::EnrollmentPreauthorization {
                spki,
                expires_at_ms,
            } => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_millis() as u64)
                    .unwrap_or(u64::MAX);
                if *expires_at_ms <= now_ms {
                    return Ok(());
                }
                self.credentials
                    .catalog()
                    .put_enrollment_preauthorization(spki, *expires_at_ms)?;
                let ttl = std::time::Duration::from_millis(expires_at_ms - now_ms);
                let transport = self.transport.get().ok_or_else(|| crate::Error::Internal {
                    detail: "metadata enrollment apply has no cluster transport".into(),
                })?;
                if !transport.preauthorize_peer_identity(*spki, ttl) {
                    // Admission remains fail-closed, but replicated metadata
                    // application must never wedge on a bounded runtime cache.
                    // The issuer reserves capacity before proposing, so this is
                    // only a defensive path for stale/corrupt excess entries.
                    tracing::error!(
                        ?spki,
                        "metadata enrollment preauthorization capacity exhausted; entry persisted but not admitted"
                    );
                }
                return Ok(());
            }
            MetadataEntry::EnrollmentPreauthorizationRevoke {
                spki,
                expires_at_ms,
            } => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_millis() as u64)
                    .unwrap_or(u64::MAX);
                if *expires_at_ms <= now_ms {
                    return Ok(());
                }
                self.credentials
                    .catalog()
                    .remove_enrollment_preauthorization(spki)?;
                let transport = self.transport.get().ok_or_else(|| crate::Error::Internal {
                    detail: "metadata enrollment revoke has no cluster transport".into(),
                })?;
                transport.revoke_peer_preauthorization(
                    spki,
                    std::time::Duration::from_millis(expires_at_ms - now_ms),
                );
                return Ok(());
            }
            MetadataEntry::RoutingChange(RoutingChange::SetPlacement {
                group_id,
                placement,
            }) => {
                return self.apply_set_placement(*group_id, placement, raft_index);
            }
            MetadataEntry::TopologyChange(TopologyChange::Leave { node_id }) => {
                // Lease GC: a node that left the topology can never release
                // its own leases. Spawn (do NOT propose-and-wait inline —
                // apply runs on the raft loop task; blocking here would
                // deadlock the applied-index watcher).
                if let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) {
                    let shared = std::sync::Arc::clone(&shared);
                    let left_node_id = *node_id;
                    tokio::spawn(async move {
                        if !shared.is_singleton_worker() {
                            return;
                        }
                        if let Err(e) =
                            crate::control::lease::gc::gc_leases_for_node(&shared, left_node_id)
                        {
                            tracing::warn!(
                                node_id = left_node_id,
                                error = %e,
                                "lease GC after Leave failed; periodic sweep will retry"
                            );
                        }
                    });
                }
                return Ok(());
            }
            _ => {}
        }

        self.apply_catalog_ddl(entry, raft_index)
    }

    /// Publish a permanent apply failure on the node-wide readiness marker.
    ///
    /// Best-effort by construction: unit-test appliers are built without a
    /// `SharedState`, and a node that has already torn its shared state down
    /// has nothing left to report readiness to. The structured faultbox report
    /// and the error log at the call site are unconditional, so the failure is
    /// never lost when this cannot land.
    fn record_permanent_wedge(
        &self,
        error: &crate::Error,
        entry: &MetadataEntry,
        raft_index: u64,
        last_applied_watermark: u64,
    ) {
        let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) else {
            return;
        };
        shared
            .metadata_apply_wedge
            .record(super::wedge::WedgeReport {
                raft_index,
                last_applied_watermark,
                entry_kind: crate::diag::entry_kind(entry),
                error: error.to_string(),
            });
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
                // Raft no-op: nothing to apply, but advance the cache watermark
                // in lockstep with the Raft applied index the tick loop reports
                // from our return value. Skipping this leaves `cache.applied_index`
                // behind the watcher and the startup applied-index sanity check
                // fails the boot with a spurious gap (every group's first
                // committed entry on a fresh start is an election no-op).
                self.cache
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .advance_applied_index(*index);
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
                // Both classes stop the batch — skipping a committed metadata
                // entry is silent divergence from the quorum and is strictly
                // worse than halting. What differs is whether waiting for a
                // re-delivery is an honest plan.
                let class = super::wedge::classify(&e);
                // A deterministic failure here re-fails on every re-delivery and
                // wedges this node's applier forever while /healthz stays green,
                // so it is filed as a structured report — not just a log line —
                // at the one site that detects it.
                crate::diag::metadata_apply_wedged(&e, &entry, *index, last, class.is_permanent());
                if class.is_permanent() {
                    // Retrying cannot help, so the node must stop advertising
                    // readiness rather than serve queries that will all die on
                    // an unrelated-looking descriptor-lease timeout.
                    self.record_permanent_wedge(&e, &entry, *index, last);
                    error!(
                        index = *index,
                        last_applied = last,
                        error = %e,
                        "metadata apply: PERMANENT host-side effect failure; watermark halted \
                         and this node is no longer ready — re-delivery cannot clear this, \
                         operator intervention is required"
                    );
                } else {
                    error!(
                        index = *index,
                        last_applied = last,
                        error = %e,
                        "metadata apply: durable host-side effect failed; not advancing \
                         watermark — Raft will re-deliver and retry"
                    );
                }
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
    use std::sync::{Arc, Mutex, RwLock};

    use tokio::sync::broadcast;

    use nodedb_cluster::{MetadataCache, PendingDdlObject, encode_entry};
    use nodedb_types::{DatabaseId, Hlc};

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::catalog_entry;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

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
        let token_state = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let applier =
            MetadataCommitApplier::new(cache.clone(), tx, credentials.clone(), token_state);
        (applier, cache, credentials, tmp)
    }

    fn put_collection_entry(name: &str) -> MetadataEntry {
        let stored = StoredCollection::new(7, name, "tester");
        let catalog_entry = CatalogEntry::PutCollection(Box::new(stored));
        MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&catalog_entry).unwrap(),
        }
    }

    fn pending_create_object(name: &str) -> PendingDdlObject {
        PendingDdlObject::Create {
            entry: Box::new(put_collection_entry(name)),
        }
    }

    /// An applier wired to a real `SharedState` (weak handle installed), the
    /// only shape under which `DdlPendingPropose` / `DdlPendingFinalize` /
    /// `DdlPendingCancel` do anything — they are no-ops without it, matching
    /// every other `self.shared`-gated apply path in this module.
    fn make_applier_with_shared() -> (MetadataCommitApplier, Arc<SharedState>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&tmp.path().join("test.wal")).expect("open wal"));
        let credentials =
            Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open catalog"));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new_with_credentials(dispatcher, wal, credentials, false)
            .expect("construct shared state");
        // `_data_sides` is dropped with this fixture, so the schema-register
        // barrier can never be answered. Keep its deadline short: the test
        // covers finalize semantics, not the production deadline. Sole
        // reference here, so `get_mut` always succeeds.
        Arc::get_mut(&mut state)
            .expect("sole reference to the fixture's SharedState")
            .tuning
            .network
            .default_deadline_secs = 1;
        let (tx, _rx) = broadcast::channel(16);
        let token_state = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let applier = MetadataCommitApplier::new(
            state.metadata_cache.clone(),
            tx,
            state.credentials.clone(),
            token_state,
        );
        applier.install_shared(Arc::downgrade(&state));
        (applier, state, tmp)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn propose_then_finalize_applies_and_clears_the_record() {
        let (applier, state, _tmp) = make_applier_with_shared();
        let token = 1;
        let propose = MetadataEntry::DdlPendingPropose {
            token,
            objects: vec![pending_create_object("pending_orders")],
            proposed_at: Hlc::default(),
        };
        assert_eq!(applier.apply(&[(1, encode_entry(&propose).unwrap())]), 1);
        assert!(
            state.pending_ddl.contains(token),
            "propose reserves the record"
        );
        assert!(
            state
                .credentials
                .catalog()
                .get_collection(DatabaseId::DEFAULT, 7, "pending_orders")
                .unwrap()
                .is_none(),
            "propose alone must not write the catalog"
        );

        let finalize = MetadataEntry::DdlPendingFinalize { token };
        assert_eq!(applier.apply(&[(2, encode_entry(&finalize).unwrap())]), 2);
        assert!(
            !state.pending_ddl.contains(token),
            "finalize clears the record"
        );
        assert!(
            state
                .credentials
                .catalog()
                .get_collection(DatabaseId::DEFAULT, 7, "pending_orders")
                .unwrap()
                .is_some(),
            "finalize replays the reserved object's host-side effects"
        );

        // Double-apply (Raft re-delivery): no record left, so this must be a
        // silent no-op rather than an error or a repeat write.
        assert_eq!(applier.apply(&[(3, encode_entry(&finalize).unwrap())]), 3);
        assert!(!state.pending_ddl.contains(token));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn propose_then_cancel_clears_without_touching_the_catalog() {
        let (applier, state, _tmp) = make_applier_with_shared();
        let token = 2;
        let propose = MetadataEntry::DdlPendingPropose {
            token,
            objects: vec![pending_create_object("pending_widgets")],
            proposed_at: Hlc::default(),
        };
        assert_eq!(applier.apply(&[(1, encode_entry(&propose).unwrap())]), 1);
        assert!(state.pending_ddl.contains(token));

        let cancel = MetadataEntry::DdlPendingCancel { token };
        assert_eq!(applier.apply(&[(2, encode_entry(&cancel).unwrap())]), 2);
        assert!(
            !state.pending_ddl.contains(token),
            "cancel clears the record"
        );
        assert!(
            state
                .credentials
                .catalog()
                .get_collection(DatabaseId::DEFAULT, 7, "pending_widgets")
                .unwrap()
                .is_none(),
            "cancel must never write the catalog"
        );

        // Double-apply (Raft re-delivery): no record left, must stay a no-op.
        assert_eq!(applier.apply(&[(3, encode_entry(&cancel).unwrap())]), 3);
        assert!(!state.pending_ddl.contains(token));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn finalize_and_cancel_for_an_unknown_token_are_noops() {
        let (applier, state, _tmp) = make_applier_with_shared();
        let unknown = 999;
        assert_eq!(
            applier.apply(&[(
                1,
                encode_entry(&MetadataEntry::DdlPendingFinalize { token: unknown }).unwrap()
            )]),
            1,
            "finalize with no matching propose must not wedge the watermark"
        );
        assert_eq!(
            applier.apply(&[(
                2,
                encode_entry(&MetadataEntry::DdlPendingCancel { token: unknown }).unwrap()
            )]),
            2,
            "cancel with no matching propose must not wedge the watermark"
        );
        assert!(!state.pending_ddl.contains(unknown));
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
                database_id: 0,
                tenant_id: 7,
                name: "archived".into(),
                descriptor_version: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
            })
            .unwrap(),
        };
        applier.apply(&[(2, encode_entry(&drop_entry).unwrap())]);

        let loaded = credentials
            .catalog()
            .get_collection(DatabaseId::DEFAULT, 7, "archived")
            .unwrap()
            .expect("preserved");
        assert!(!loaded.is_active);
    }

    #[test]
    fn join_token_transition_updates_and_persists_shared_mirror() {
        let (applier, _cache, credentials, _tmp) = make_applier();
        let hash = [0x44; 32];
        let entries = [
            MetadataEntry::JoinTokenTransition {
                token_hash: hash,
                transition: nodedb_cluster::JoinTokenTransitionKind::Register {
                    expires_at_ms: 10_000,
                },
                ts_ms: 1,
            },
            MetadataEntry::JoinTokenTransition {
                token_hash: hash,
                transition: nodedb_cluster::JoinTokenTransitionKind::BeginInFlight {
                    node_addr: "127.0.0.1:9000".into(),
                    lease_id: [0x55; 16],
                },
                ts_ms: 2,
            },
            MetadataEntry::JoinTokenTransition {
                token_hash: hash,
                transition: nodedb_cluster::JoinTokenTransitionKind::MarkConsumed {
                    node_addr: "127.0.0.1:9000".into(),
                    lease_id: [0x55; 16],
                    recovery_bundle: vec![1, 2, 3],
                },
                ts_ms: 3,
            },
        ];
        for (offset, entry) in entries.iter().enumerate() {
            let index = offset as u64 + 1;
            assert_eq!(
                applier.apply(&[(index, encode_entry(entry).expect("encode"))]),
                index
            );
        }
        let persisted = credentials
            .catalog()
            .list_join_token_states()
            .expect("load token state");
        assert!(matches!(
            persisted.as_slice(),
            [nodedb_cluster::JoinTokenState {
                lifecycle: nodedb_cluster::JoinTokenLifecycle::Consumed { ts_ms: 3, .. },
                ..
            }]
        ));
    }

    #[test]
    fn apply_empty_batch_is_noop() {
        let (applier, _cache, _credentials, _tmp) = make_applier();
        assert_eq!(applier.apply(&[]), 0);
    }

    #[test]
    fn apply_noop_entry_advances_cache_watermark() {
        let (applier, cache, _credentials, _tmp) = make_applier();
        // A committed Raft no-op (empty payload) at index 1 — the shape of every
        // group's first entry on a fresh single-node start. It mutates nothing, but
        // the cache watermark must advance in lockstep with the Raft applied index
        // the tick loop takes from the return value; otherwise the startup
        // applied-index sanity check reads a spurious gap and fails the boot.
        assert_eq!(applier.apply(&[(1, Vec::new())]), 1);
        assert_eq!(cache.read().unwrap().applied_index, 1);
        assert_eq!(
            cache.read().unwrap().catalog_entries_applied,
            0,
            "a no-op applies no catalog entry"
        );
    }
}
