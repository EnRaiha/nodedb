// SPDX-License-Identifier: BUSL-1.1

//! Top-level `apply_host_side_effects` dispatcher and the
//! `impl MetadataApplier for MetadataCommitApplier` trait entry point.

use tracing::{debug, error, warn};

use nodedb_cluster::{MetadataApplier, MetadataEntry, RoutingChange, decode_entry};

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
