// SPDX-License-Identifier: BUSL-1.1

//! Apply ContinuousAggregate catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredContinuousAggregate, SystemCatalog};

pub fn put(stored: &StoredContinuousAggregate, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_continuous_aggregate(stored) {
        warn!(
            cagg = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_continuous_aggregate failed"
        );
    }
    super::owner::put_parent_owner_in_database(
        object_type::CONTINUOUS_AGGREGATE,
        stored.database_id,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(database_id: u64, tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_continuous_aggregate(database_id, tenant_id, name) {
        warn!(
            cagg = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_continuous_aggregate failed"
        );
    }
    super::owner::delete_parent_owner_in_database(
        object_type::CONTINUOUS_AGGREGATE,
        database_id,
        tenant_id,
        name,
        catalog,
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control::catalog_entry::apply::apply_to;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::security::credential::store::CredentialStore;

    /// Shared helper: open a fresh temp-dir-backed credential store
    /// and return it alongside the TempDir (kept alive for the test).
    fn open_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(
            CredentialStore::open(&tmp.path().join("system.redb")).expect("open credential store"),
        );
        (store, tmp)
    }

    fn stored(database_id: u64, owner: &str) -> StoredContinuousAggregate {
        StoredContinuousAggregate {
            database_id,
            tenant_id: 1,
            name: "shared".into(),
            source: "events".into(),
            def_bytes: Vec::new(),
            owner: owner.into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: Default::default(),
        }
    }

    #[test]
    fn delete_is_scoped_to_database() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        apply_to(
            &CatalogEntry::PutContinuousAggregate(Box::new(stored(0, "default_owner"))),
            catalog,
        )
        .expect("apply put_continuous_aggregate");
        apply_to(
            &CatalogEntry::PutContinuousAggregate(Box::new(stored(9, "other_owner"))),
            catalog,
        )
        .expect("apply put_continuous_aggregate");

        apply_to(
            &CatalogEntry::DeleteContinuousAggregate {
                database_id: 9,
                tenant_id: 1,
                name: "shared".into(),
            },
            catalog,
        )
        .expect("apply delete_continuous_aggregate");

        assert!(
            catalog
                .get_continuous_aggregate(9, 1, "shared")
                .unwrap()
                .is_none()
        );
        assert!(
            catalog
                .get_continuous_aggregate(0, 1, "shared")
                .unwrap()
                .is_some()
        );
        let owners = catalog.load_all_owners().unwrap();
        assert!(!owners.iter().any(|owner| owner.database_id == 9));
        assert!(owners.iter().any(|owner| owner.database_id == 0));
    }
}
