// SPDX-License-Identifier: BUSL-1.1

//! Top-level `apply_host_side_effects` dispatcher and the
//! `impl MetadataApplier for MetadataCommitApplier` trait entry point.

use tracing::{debug, warn};

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
                epoch,
                created_ms,
            } => {
                return self.apply_sync_producer_register(
                    lite_id,
                    *producer_id,
                    *tenant_id,
                    *epoch,
                    *created_ms,
                    raft_index,
                );
            }
            MetadataEntry::SyncProducerFence { lite_id, new_epoch } => {
                return self.apply_sync_producer_fence(lite_id, *new_epoch, raft_index);
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
