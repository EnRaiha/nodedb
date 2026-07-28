// SPDX-License-Identifier: BUSL-1.1

//! Cutover phase for `MOVE TENANT`.
//!
//! Issues a single Raft proposal (`CatalogEntry::MoveTenantCutover`) that
//! atomically re-keys all of the tenant's collections from the source database
//! to the target database.
//!
//! The atomicity guarantee comes from the Raft log: either the entire entry
//! is applied on every node, or none of it is.  A partial failure (e.g. one
//! node applies while another is down) is resolved by Raft log replay on
//! restart.
//!
//! After the catalog entry applies, a `MetaOp::RenameCollection` is dispatched
//! to the Data Plane for each moved collection.  This re-keys all documents
//! and secondary indexes from the old db-qualified collection name to the new
//! one, making physical data accessible under the target database context.

use std::time::Duration;

use bytes::Bytes;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::catalog_entry::apply::apply_to;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::MetaOp;
use nodedb_types::NodeDbError;

/// Timeout for each Data Plane rename dispatch.
const RENAME_DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Run the cutover phase.
///
/// Loads all active collections in `source_db_id`, proposes
/// `CatalogEntry::MoveTenantCutover` as a single Raft entry, then dispatches
/// `MetaOp::RenameCollection` for each moved collection so the Data Plane
/// re-keys physical storage from the source db-qualified name to the target
/// db-qualified name.
///
/// The `_snapshot_bytes` argument carries the backup envelope produced by the
/// snapshot phase.  In the offline v1 implementation it is not consumed here —
/// the Raft proposal re-uses the live catalog state.  It is kept as a
/// parameter so the call site can hold it as the rollback artifact.
pub async fn run(
    state: &SharedState,
    catalog: &SystemCatalog,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    target_db_id: DatabaseId,
    _snapshot_bytes: &Bytes,
) -> Result<(), NodeDbError> {
    // Load every active collection in the source database.  The entire source
    // database namespace is transferred atomically to the target database in
    // this single Raft proposal; soft-deleted (inactive) collections are
    // excluded because they are pending GC and not part of the live namespace.
    let collections: Vec<_> = catalog
        .load_all_collections(source_db_id)
        .map_err(|e| {
            NodeDbError::move_tenant_cutover_failed(
                tenant_id.as_u64().to_string(),
                format!("failed to enumerate source collections: {e}"),
            )
        })?
        .into_iter()
        .filter(|c| c.is_active)
        .collect();

    let entry = CatalogEntry::MoveTenantCutover {
        tenant_id: tenant_id.as_u64(),
        source_db_id: source_db_id.as_u64(),
        target_db_id: target_db_id.as_u64(),
        collections: collections.clone(),
    };

    let proposed = propose_catalog_entry(state, &entry).map_err(|e| {
        NodeDbError::move_tenant_cutover_failed(
            tenant_id.as_u64().to_string(),
            format!("Raft proposal failed: {e}"),
        )
    })?;

    // Single-node path (proposed == 0): Raft is absent; apply directly.
    // Clustered path: the entry was applied after quorum commit.
    if proposed == 0 {
        let catalog = state.credentials.catalog();
        apply_to(&entry, catalog);
    }

    // Dispatch physical storage re-keying to the Data Plane for each moved
    // collection.  Each collection's documents and secondary indexes are stored
    // under the db-qualified collection name (e.g. `"2/orders"` for database 2).
    // After the catalog entry moved the namespace, queries route to the new
    // db-qualified name; we must migrate physical storage to match.
    dispatch_rename_ops(state, tenant_id, source_db_id, target_db_id, &collections).await?;

    Ok(())
}

/// Dispatch `MetaOp::RenameCollection` for each moved collection so the Data
/// Plane re-keys physical storage.
async fn dispatch_rename_ops(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    target_db_id: DatabaseId,
    collections: &[StoredCollection],
) -> Result<(), NodeDbError> {
    for coll in collections {
        let old_collection = db_qualified(source_db_id, &coll.name);
        let new_collection = db_qualified(target_db_id, &coll.name);

        let plan = PhysicalPlan::Meta(MetaOp::RenameCollection {
            tenant_id: coll.tenant_id,
            old_database_id: source_db_id.as_u64(),
            new_database_id: target_db_id.as_u64(),
            old_collection: old_collection.clone(),
            new_collection: new_collection.clone(),
        });

        // Route to the destination database: cutover materializes the
        // re-keyed namespace under target_db_id, so the rename dispatch
        // targets the shard owning the new db-qualified storage.
        sync_dispatch::dispatch_system(
            state,
            sync_dispatch::SystemTask::new(
                sync_dispatch::SystemReason::TenantLifecycle,
                tenant_id,
                target_db_id,
                "__system",
                plan,
            ),
            RENAME_DISPATCH_TIMEOUT,
        )
        .await
        .map_err(|e| {
            NodeDbError::move_tenant_cutover_failed(
                tenant_id.as_u64().to_string(),
                format!("rename_collection dispatch ({old_collection} -> {new_collection}): {e}"),
            )
        })?;
    }
    Ok(())
}
