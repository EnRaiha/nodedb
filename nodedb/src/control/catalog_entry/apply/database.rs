// SPDX-License-Identifier: BUSL-1.1

//! Apply database catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::database_types::DatabaseDescriptor;
use nodedb_types::DatabaseId;

/// Apply a `PutDatabase` entry — upsert the descriptor into
/// `_system.databases` and `_system.databases_by_name`.
pub fn put(descriptor: &DatabaseDescriptor, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_database(descriptor) {
        warn!(
            db_id = descriptor.id.as_u64(),
            name = %descriptor.name,
            error = %e,
            "catalog_entry: put_database failed"
        );
    }
}

/// Apply a `DeleteDatabase` entry — remove the descriptor, its
/// reverse-lookup row, and the quota rows of the dropped scope.
pub fn delete(db_id: u64, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_database(DatabaseId::new(db_id)) {
        warn!(
            db_id,
            error = %e,
            "catalog_entry: delete_database failed"
        );
    }
    // A stale quota row keeps consuming the sum-of-quotas ceiling.
    super::quota::purge_database_scope(db_id, catalog);
}

/// Apply a `PutDatabaseGrant` entry.
pub fn put_grant(db_id: u64, user_id: u64, privilege: &str, catalog: &SystemCatalog) {
    let db = DatabaseId::new(db_id);
    if let Err(e) = catalog.put_database_grant(db, user_id, privilege) {
        warn!(
            db_id,
            user_id,
            privilege,
            error = %e,
            "catalog_entry: put_database_grant failed"
        );
    }
}

/// Apply a `CloneDatabase` entry — write the target descriptor, update the
/// clone lineage table, and stamp every source collection into the target
/// database with `cloned_from` set so the SQL planner can resolve queries
/// against the clone without a source-side lookup at plan time.
pub fn clone_apply(
    target_descriptor: &DatabaseDescriptor,
    source_db_id: u64,
    catalog: &SystemCatalog,
) {
    if let Err(e) = catalog.put_database(target_descriptor) {
        warn!(
            target_db_id = target_descriptor.id.as_u64(),
            name = %target_descriptor.name,
            error = %e,
            "catalog_entry: clone_database put_database failed"
        );
        return;
    }
    let source = DatabaseId::new(source_db_id);
    let child = target_descriptor.id;
    if let Err(e) = catalog.add_clone_child(source, child) {
        warn!(
            source_db_id,
            child_db_id = child.as_u64(),
            error = %e,
            "catalog_entry: clone_database add_clone_child failed"
        );
    }

    // Determine the as_of and clone_created_at LSN values from the target
    // descriptor's parent_clone reference.
    let (as_of_lsn, clone_created_at, kv_surrogate_ceiling) = match &target_descriptor.parent_clone
    {
        Some(pc) => (
            nodedb_types::Lsn::new(pc.as_of_lsn),
            nodedb_types::Lsn::new(target_descriptor.created_at_lsn),
            pc.kv_surrogate_ceiling,
        ),
        None => {
            // No parent clone ref — nothing to stamp. Descriptor was written
            // above; non-clone databases are complete.
            return;
        }
    };

    // Enumerate every active collection in the source database and write a
    // shadow descriptor into the target database so the SQL planner can
    // resolve collection names without knowing about clone indirection.
    //
    // Each shadow collection carries `cloned_from` pointing back to the
    // source, so the read/write planner applies CoW delegation at dispatch
    // time. The engines never see this field.
    //
    // We enumerate all tenants visible in the source by walking every
    // collection row under the source database_id. The tenant_id is encoded
    // in the inner key prefix, so we collect it from the row itself.
    let source_colls = match catalog.load_all_collections(source) {
        Ok(cs) => cs,
        Err(e) => {
            warn!(
                source_db_id,
                error = %e,
                "catalog_entry: clone_database: failed to enumerate source collections"
            );
            return;
        }
    };

    for mut coll in source_colls.into_iter().filter(|c| c.is_active) {
        coll.database_id = child;
        coll.cloned_from = Some(nodedb_types::CloneOrigin {
            source_database: source,
            source_collection: coll.name.clone(),
            as_of_lsn,
            clone_created_at,
            kv_surrogate_ceiling,
        });
        coll.clone_status = nodedb_types::CloneStatus::Shadowed;
        // Reset versioning so the new clone descriptor starts fresh.
        coll.descriptor_version = 0;
        match catalog.put_collection(child, &coll) {
            Ok(()) => super::owner::put_parent_owner_in_database(
                object_type::COLLECTION,
                child.as_u64(),
                coll.tenant_id,
                &coll.name,
                &coll.owner,
                catalog,
            ),
            Err(e) => warn!(
                target_db_id = child.as_u64(),
                collection = %coll.name,
                error = %e,
                "catalog_entry: clone_database: failed to stamp shadow collection"
            ),
        }
    }
}

/// Apply a `DeleteDatabaseGrant` entry.
pub fn delete_grant(db_id: u64, user_id: u64, privilege: &str, catalog: &SystemCatalog) {
    let db = DatabaseId::new(db_id);
    if let Err(e) = catalog.delete_database_grant(db, user_id, privilege) {
        warn!(
            db_id,
            user_id,
            privilege,
            error = %e,
            "catalog_entry: delete_database_grant failed"
        );
    }
}
