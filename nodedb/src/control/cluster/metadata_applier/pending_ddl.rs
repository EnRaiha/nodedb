// SPDX-License-Identifier: BUSL-1.1

//! Host-side apply logic for `DdlPendingPropose` / `DdlPendingFinalize` /
//! `DdlPendingCancel`.
//!
//! Applies the entries `ddl_flush::begin_commit` / `finalize_pending` propose
//! at COMMIT, and the ones `metadata_proposer::acquire_ddl_prepare_lease`
//! proposes to reclaim a dead owner's stranded record. Finalize and cancel
//! are idempotent: applying either twice, or applying either for a token
//! with no pending record, is a no-op. Raft replay relies on exactly that
//! shape.

use tracing::{debug, error};

use nodedb_cluster::{MetadataEntry, PendingDdlObject};
use nodedb_types::Hlc;

use crate::control::catalog_entry;

use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    /// `DdlPendingPropose`: insert the pending record. Re-delivery of the
    /// same propose overwrites with an identical record, so no ordering
    /// hazard exists.
    pub(super) fn apply_ddl_pending_propose(
        &self,
        token: u64,
        objects: &[PendingDdlObject],
        proposed_at: Hlc,
    ) -> Result<(), crate::Error> {
        let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) else {
            return Ok(());
        };
        // `proposed_at` is the only remote HLC observation the metadata group
        // carries — every other `Hlc` on a `MetadataEntry` is a future
        // deadline, and folding one would jump this node's clock forward.
        //
        // The entry is already committed, so a refused fold must not stop the
        // apply: refusing to move the clock IS the protection. Applying still
        // has to happen or the state machine wedges.
        if let Err(skew) = shared.hlc_clock.update_checked(proposed_at) {
            error!(
                token,
                skew_ms = skew.skew_ns / 1_000_000,
                remote_wall_ns = skew.remote_wall_ns,
                local_wall_ns = skew.local_wall_ns,
                "refusing to fold a proposer's HLC: {skew}"
            );
        }
        shared
            .pending_ddl
            .insert(token, objects.to_vec(), proposed_at);
        Ok(())
    }

    /// `DdlPendingFinalize`: replay every reserved object's host-side
    /// effects, then drop the pending record. The record is peeked rather
    /// than removed up front, so a mid-replay failure leaves it in place
    /// for the next re-delivery instead of silently skipping the rest.
    pub(super) fn apply_ddl_pending_finalize(
        &self,
        token: u64,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) else {
            return Ok(());
        };
        let Some(record) = shared.pending_ddl.get(token) else {
            debug!(token, "pending DDL finalize: no pending record, no-op");
            return Ok(());
        };
        for object in &record.objects {
            self.apply_host_side_effects(object_entry(object), raft_index)?;
        }
        shared.pending_ddl.take(token);
        Ok(())
    }

    /// `DdlPendingCancel`: tear down the Data Plane engine registered for
    /// every `Create`-shaped reserved object, then drop the pending
    /// record. A collection's engine is registered eagerly at CREATE
    /// statement time, independent of buffering, so an abandoned create
    /// still needs the same `UnregisterCollection` teardown a real purge
    /// uses. The dispatch is spawned rather than awaited inline — apply
    /// runs on the raft loop task, and blocking here would deadlock the
    /// applied-index watcher (same reasoning as the `TopologyChange::Leave`
    /// lease-GC spawn in `dispatch.rs`).
    pub(super) fn apply_ddl_pending_cancel(&self, token: u64) -> Result<(), crate::Error> {
        let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) else {
            return Ok(());
        };
        let Some(record) = shared.pending_ddl.take(token) else {
            debug!(token, "pending DDL cancel: no pending record, no-op");
            return Ok(());
        };
        for object in &record.objects {
            let PendingDdlObject::Create { entry } = object else {
                continue;
            };
            let Some((database_id, tenant_id, name)) = created_collection_target(entry.as_ref())
            else {
                continue;
            };
            let shared = std::sync::Arc::clone(&shared);
            tokio::spawn(async move {
                let purge_lsn = shared.wal.next_lsn().as_u64();
                if let Err(error) = crate::control::server::shared::ddl::neutral::collection::purge::dispatch_unregister_collection(
                    &shared, database_id, tenant_id, &name, purge_lsn,
                )
                .await
                {
                    tracing::warn!(
                        collection = %name,
                        tenant = tenant_id,
                        error = %error,
                        "pending DDL cancel: Data Plane teardown failed"
                    );
                }
            });
        }
        Ok(())
    }
}

/// The `MetadataEntry` wrapped by a pending object, regardless of shape.
fn object_entry(object: &PendingDdlObject) -> &MetadataEntry {
    match object {
        PendingDdlObject::Create { entry } | PendingDdlObject::Alter { entry, .. } => {
            entry.as_ref()
        }
    }
}

/// `(database_id, tenant_id, name)` when `entry` is a collection create —
/// the only shape that registers a Data Plane engine eagerly at DDL time.
fn created_collection_target(entry: &MetadataEntry) -> Option<(u64, u64, String)> {
    let payload = match entry {
        MetadataEntry::CatalogDdl { payload }
        | MetadataEntry::CatalogDdlAudited { payload, .. } => payload,
        _ => return None,
    };
    match catalog_entry::decode(payload).ok()? {
        catalog_entry::CatalogEntry::PutCollection(stored)
        | catalog_entry::CatalogEntry::PutCollectionIfAbsent(stored) => {
            Some((stored.database_id.as_u64(), stored.tenant_id, stored.name))
        }
        _ => None,
    }
}
