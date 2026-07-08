// SPDX-License-Identifier: BUSL-1.1

//! Lite sync-producer registration/fencing and live routing-table
//! placement host-side effects.

use tracing::{debug, warn};

use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    pub(super) fn apply_sync_producer_register(
        &self,
        lite_id: &str,
        producer_id: u64,
        tenant_id: u64,
        epoch: u64,
        created_ms: i64,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            let Some(registry) = shared.producer_registry.as_deref() else {
                return Ok(());
            };
            // The registration row is durable replicated state. A write
            // failure must not advance the watermark — Raft re-delivers
            // and `apply_register` is idempotent, so the retry is safe.
            if let Err(e) =
                registry.apply_register(lite_id, producer_id, tenant_id, epoch, created_ms)
            {
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
        Ok(())
    }

    pub(super) fn apply_sync_producer_fence(
        &self,
        lite_id: &str,
        new_epoch: u64,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            let Some(registry) = shared.producer_registry.as_deref() else {
                return Ok(());
            };
            // Durable epoch advance; `apply_fence` is idempotent
            // (max-wins) so re-delivery on failure is safe.
            if let Err(e) = registry.apply_fence(lite_id, new_epoch) {
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
        Ok(())
    }

    /// Group membership and leadership converge through the Raft
    /// conf-change path (which mutates the shared routing table on
    /// every node). `SetPlacement` carries the *intended* voter set
    /// for a group and has no conf-change equivalent, so it must be
    /// written through to the live shared routing table here — the
    /// same `RwLock<RoutingTable>` the reconciler and the
    /// learner-promotion gate read. Without this write the placement
    /// never leaves the metadata log and N>RF voter-cap convergence
    /// is inert. The other `RoutingChange` variants are intentionally
    /// not handled here to avoid double-applying the conf-change path.
    pub(super) fn apply_set_placement(
        &self,
        group_id: u64,
        placement: &[u64],
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
            && let Some(routing) = shared.cluster_routing.as_ref()
        {
            routing
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .set_placement(group_id, placement.to_vec());
            debug!(
                group_id,
                raft_index, "set_placement applied to live routing table"
            );
        }
        Ok(())
    }
}
