// SPDX-License-Identifier: BUSL-1.1

//! Apply database catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::database_types::DatabaseDescriptor;
use crate::control::security::catalog::{SystemCatalog, catalog_err};
use nodedb_types::DatabaseId;

/// Apply a `PutDatabase` entry — upsert the descriptor into
/// `_system.databases` and `_system.databases_by_name`.
pub fn put(descriptor: &DatabaseDescriptor, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_database(descriptor).map_err(|e| {
        catalog_err(
            &format!(
                "put_database '{}' (database {})",
                descriptor.name,
                descriptor.id.as_u64()
            ),
            e,
        )
    })
}

/// Apply a `DeleteDatabase` entry — remove the descriptor, its
/// reverse-lookup row, and the quota rows of the dropped scope.
pub fn delete(db_id: u64, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_database(DatabaseId::new(db_id))
        .map_err(|e| catalog_err(&format!("delete_database (database {db_id})"), e))?;
    // A stale quota row keeps consuming the sum-of-quotas ceiling.
    super::quota::purge_database_scope(db_id, catalog)
}

/// Apply a `PutDatabaseGrant` entry.
pub fn put_grant(
    db_id: u64,
    user_id: u64,
    privilege: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .put_database_grant(DatabaseId::new(db_id), user_id, privilege)
        .map_err(|e| {
            catalog_err(
                &format!("put_database_grant '{privilege}' (database {db_id}, user {user_id})"),
                e,
            )
        })
}

/// Apply a `CloneDatabase` entry — write the target descriptor, update the
/// clone lineage table, and stamp every source collection into the target
/// database with `cloned_from` set so the SQL planner can resolve queries
/// against the clone without a source-side lookup at plan time.
///
/// Every step raises on failure: a half-stamped clone answers queries this
/// node's peers answer differently.
pub fn clone_apply(
    target_descriptor: &DatabaseDescriptor,
    source_db_id: u64,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let child = target_descriptor.id;
    catalog.put_database(target_descriptor).map_err(|e| {
        catalog_err(
            &format!(
                "clone_database descriptor write of '{}' (database {})",
                target_descriptor.name,
                child.as_u64()
            ),
            e,
        )
    })?;
    let source = DatabaseId::new(source_db_id);
    catalog.add_clone_child(source, child).map_err(|e| {
        catalog_err(
            &format!(
                "clone_database lineage edge (source {source_db_id}, child {})",
                child.as_u64()
            ),
            e,
        )
    })?;

    // Determine the as_of and clone_created_at LSN values from the target
    // descriptor's parent_clone reference.
    let Some(parent_clone) = &target_descriptor.parent_clone else {
        // No parent clone ref — nothing to stamp. Descriptor was written
        // above; non-clone databases are complete.
        return Ok(());
    };
    let as_of_lsn = nodedb_types::Lsn::new(parent_clone.as_of_lsn);
    let clone_created_at = nodedb_types::Lsn::new(target_descriptor.created_at_lsn);
    let kv_surrogate_ceiling = parent_clone.kv_surrogate_ceiling;

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
    let source_colls = catalog.load_all_collections(source).map_err(|e| {
        catalog_err(
            &format!("clone_database enumeration of source database {source_db_id}"),
            e,
        )
    })?;

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
        catalog.put_collection(child, &coll).map_err(|e| {
            catalog_err(
                &format!(
                    "clone_database shadow stamp of '{}' into database {}",
                    coll.name,
                    child.as_u64()
                ),
                e,
            )
        })?;
        super::owner::put_parent_owner_in_database(
            object_type::COLLECTION,
            child.as_u64(),
            coll.tenant_id,
            &coll.name,
            &coll.owner,
            catalog,
        )?;
    }
    Ok(())
}

/// Apply a `DeleteDatabaseGrant` entry.
pub fn delete_grant(
    db_id: u64,
    user_id: u64,
    privilege: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_database_grant(DatabaseId::new(db_id), user_id, privilege)
        .map_err(|e| {
            catalog_err(
                &format!("delete_database_grant '{privilege}' (database {db_id}, user {user_id})"),
                e,
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control::catalog_entry::apply::apply_to;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::catalog::database_types::{DatabaseStatus, ParentCloneRef};
    use crate::control::security::credential::store::CredentialStore;

    fn open_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(
            CredentialStore::open(&tmp.path().join("system.redb")).expect("open credential store"),
        );
        (store, tmp)
    }

    fn clone_descriptor(source: DatabaseId, child: DatabaseId) -> DatabaseDescriptor {
        DatabaseDescriptor {
            id: child,
            name: "clone_target".into(),
            status: DatabaseStatus::Cloning,
            created_at_lsn: 20,
            quota_ref: 0,
            parent_clone: Some(ParentCloneRef {
                source_db_id: source,
                as_of_lsn: 10,
                as_of_ms: 0,
                kv_surrogate_ceiling: None,
            }),
            mirror_origin: None,
            audit_dml: nodedb_types::AuditDmlMode::None,
            idle_session_timeout_secs: 0,
        }
    }

    /// A shadow-stamp failure aborts the whole clone. Finishing the remaining
    /// collections would leave this node answering queries its peers cannot.
    #[test]
    fn clone_apply_raises_instead_of_stamping_the_rest() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let source = DatabaseId::new(1);
        let child = DatabaseId::new(2);
        for name in ["orders", "invoices"] {
            let mut coll = StoredCollection::new(5, name, "cloner");
            coll.database_id = source;
            apply_to(&CatalogEntry::PutCollection(Box::new(coll)), catalog)
                .expect("seed source collection");
        }

        catalog.fail_next_collection_write_for_test();
        let error = clone_apply(&clone_descriptor(source, child), source.as_u64(), catalog)
            .expect_err("a failed shadow stamp must raise");
        assert!(error.to_string().contains("clone_database"), "{error}");

        let stamped = catalog.load_all_collections(child).expect("load target");
        assert!(
            stamped.is_empty(),
            "a raised clone leaves no partially stamped target: {stamped:?}"
        );
    }
}
