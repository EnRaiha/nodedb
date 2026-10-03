// SPDX-License-Identifier: BUSL-1.1

//! Lite sync-producer registration/fencing and live routing-table
//! placement host-side effects.

use tracing::{debug, warn};

use super::types::MetadataCommitApplier;
use crate::control::state::SharedState;
use crate::control::sync_producer::registry::SyncProducerRegistry;

/// The durable producer registry. Production boot always opens it, so a
/// missing one is transient: the entry is re-delivered.
fn producer_registry(shared: &SharedState) -> Result<&SyncProducerRegistry, crate::Error> {
    shared
        .producer_registry
        .as_deref()
        .ok_or_else(|| crate::Error::Internal {
            detail: "sync producer registry is not open; the entry is re-delivered".into(),
        })
}

pub(super) struct SyncPeerBindApply<'a> {
    pub(super) database_id: u64,
    pub(super) tenant_id: u64,
    pub(super) collection: &'a str,
    pub(super) peer_id: u64,
    pub(super) producer_id: u64,
    pub(super) bound_ms: i64,
}

pub(super) struct SyncProducerRegistrationApply<'a> {
    pub(super) lite_id: &'a str,
    pub(super) producer_id: u64,
    pub(super) tenant_id: u64,
    pub(super) user_id: u64,
    pub(super) epoch: u64,
    pub(super) created_ms: i64,
}

impl MetadataCommitApplier {
    pub(super) fn apply_sync_producer_register(
        &self,
        registration: SyncProducerRegistrationApply<'_>,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let SyncProducerRegistrationApply {
            lite_id,
            producer_id,
            tenant_id,
            user_id,
            epoch,
            created_ms,
        } = registration;
        let shared = self.shared_state()?;
        let registry = producer_registry(&shared)?;
        // The registration row is durable replicated state. A write
        // failure must not advance the watermark — Raft re-delivers
        // and `apply_register` is idempotent, so the retry is safe.
        if let Err(e) =
            registry.apply_register(lite_id, producer_id, tenant_id, user_id, epoch, created_ms)
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
        Ok(())
    }

    pub(super) fn apply_sync_producer_fence(
        &self,
        lite_id: &str,
        new_epoch: u64,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        let registry = producer_registry(&shared)?;
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
        Ok(())
    }

    pub(super) fn apply_sync_peer_bind(
        &self,
        binding: SyncPeerBindApply<'_>,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let SyncPeerBindApply {
            database_id,
            tenant_id,
            collection,
            peer_id,
            producer_id,
            bound_ms,
        } = binding;
        let shared = self.shared_state()?;
        let registry = producer_registry(&shared)?;
        let key = crate::control::security::catalog::sync_producer::PeerBindingKey::new(
            database_id,
            tenant_id,
            collection,
            peer_id,
        );
        // Lowest-producer-id-wins, so re-delivery and reordering both
        // converge; a write failure must not advance the watermark.
        if let Err(e) = registry.apply_bind_peer(&key, producer_id, bound_ms) {
            warn!(
                collection = %collection,
                peer_id,
                producer_id,
                error = %e,
                "sync_peer_bind apply failed — halting watermark for retry"
            );
            return Err(crate::Error::Internal {
                detail: format!("sync_peer_bind apply failed: {e}"),
            });
        }
        debug!(
            collection = %collection,
            peer_id,
            producer_id,
            raft_index,
            "loro peer id bound to producer via raft"
        );
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
    /// is inert. `ReassignVShard` has no conf-change equivalent either, and
    /// is written through by `apply_reassign_vshard`. The other
    /// `RoutingChange` variants are not handled here, so the conf-change path
    /// applies them once.
    pub(super) fn apply_set_placement(
        &self,
        group_id: u64,
        placement: &[u64],
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        // A node without a live routing table (single-node origin) has no
        // placement to update: the placement lives in the metadata cache.
        if let Some(routing) = shared.cluster_routing.as_ref() {
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
    /// Write a committed vShard reassignment through to the live routing
    /// table. `raft_index` becomes the vShard's epoch in the new group, so
    /// change-data-capture positions of the vShard keep rising across the
    /// move on every node.
    pub(super) fn apply_reassign_vshard(
        &self,
        vshard_id: u32,
        new_group_id: u64,
        new_leaseholder_node_id: u64,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        if let Some(routing) = shared.cluster_routing.as_ref() {
            let mut routing = routing.write().unwrap_or_else(|p| p.into_inner());
            routing.reassign_vshard(vshard_id, new_group_id, raft_index);
            // The entry names a planned leaseholder with no term. It fills
            // only a hint that holds no term.
            routing.set_leader(new_group_id, new_leaseholder_node_id);
            debug!(
                vshard_id,
                new_group_id, raft_index, "vShard reassignment applied to live routing table"
            );
        }
        Ok(())
    }
}
