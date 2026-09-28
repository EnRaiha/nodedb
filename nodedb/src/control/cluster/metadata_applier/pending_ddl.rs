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
    /// uses. A name with any committed row keeps its engine.
    /// The dispatch is spawned rather than awaited inline — apply
    /// runs on the raft loop task, and blocking here would deadlock the
    /// applied-index watcher (same reasoning as the `TopologyChange::Leave`
    /// lease-GC spawn in `dispatch.rs`).
    pub(super) fn apply_ddl_pending_cancel(&self, token: u64) -> Result<(), crate::Error> {
        let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) else {
            return Ok(());
        };
        let Some(record) = shared.pending_ddl.get(token) else {
            debug!(token, "pending DDL cancel: no pending record, no-op");
            return Ok(());
        };
        // Decide every teardown before dropping the record, so a failed catalog
        // read leaves the record for the re-delivered cancel.
        let catalog = self.credentials.catalog();
        let mut teardown = Vec::new();
        for object in &record.objects {
            let PendingDdlObject::Create { entry } = object else {
                continue;
            };
            let Some(target) = created_collection_target(entry.as_ref()) else {
                continue;
            };
            if cancel_owns_engine(&target, catalog)? {
                teardown.push(target);
            } else {
                debug!(
                    collection = %target.name,
                    tenant = target.tenant_id,
                    "pending DDL cancel: a later incarnation holds the name, teardown skipped"
                );
            }
        }
        shared.pending_ddl.take(token);
        for CreatedCollection {
            database_id,
            tenant_id,
            name,
        } in teardown
        {
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

/// A collection a pending create registered.
struct CreatedCollection {
    database_id: u64,
    tenant_id: u64,
    name: String,
}

/// Whether the engine registered under `target`'s name still belongs to the
/// cancelled create. A cancelled create never commits its row, so any
/// committed row under the name belongs to a finalized create or a later
/// incarnation, and its engine stays.
fn cancel_owns_engine(
    target: &CreatedCollection,
    catalog: &crate::control::security::catalog::SystemCatalog,
) -> crate::Result<bool> {
    let row = catalog.get_committed_collection(
        crate::types::DatabaseId::new(target.database_id),
        target.tenant_id,
        &target.name,
    )?;
    Ok(row.is_none())
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
fn created_collection_target(entry: &MetadataEntry) -> Option<CreatedCollection> {
    let payload = match entry {
        MetadataEntry::CatalogDdl { payload }
        | MetadataEntry::CatalogDdlAudited { payload, .. } => payload,
        _ => return None,
    };
    match catalog_entry::decode(payload).ok()? {
        catalog_entry::CatalogEntry::PutCollection(stored)
        | catalog_entry::CatalogEntry::PutCollectionIfAbsent(stored) => Some(CreatedCollection {
            database_id: stored.database_id.as_u64(),
            tenant_id: stored.tenant_id,
            name: stored.name,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::DatabaseId;

    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;

    fn target() -> CreatedCollection {
        CreatedCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "orders".to_string(),
        }
    }

    fn open() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    fn seed(store: &CredentialStore, hlc: Hlc) {
        let mut row = StoredCollection::new(1, "orders", "tester");
        row.descriptor_version = 1;
        row.modification_hlc = hlc;
        store
            .catalog()
            .put_collection(DatabaseId::DEFAULT, &row)
            .expect("seed collection");
    }

    /// A cancel replayed after the same name was created for real must leave
    /// the later collection's engine registered.
    #[test]
    fn replayed_cancel_spares_a_later_same_name_collection() {
        let (store, _tmp) = open();
        seed(&store, Hlc::new(30, 0));
        assert!(!cancel_owns_engine(&target(), store.catalog()).expect("read"));
    }

    #[test]
    fn cancel_tears_down_when_no_row_holds_the_name() {
        let (store, _tmp) = open();
        assert!(cancel_owns_engine(&target(), store.catalog()).expect("read"));
    }

    /// A committed row at the create's own clock means the create was
    /// finalized. Its engine is live.
    #[test]
    fn cancel_spares_a_finalized_create() {
        let (store, _tmp) = open();
        seed(&store, Hlc::new(10, 0));
        assert!(!cancel_owns_engine(&target(), store.catalog()).expect("read"));
    }
}
