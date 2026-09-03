// SPDX-License-Identifier: BUSL-1.1

//! Apply streaming materialized-view catalog entries.

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredOwner, SystemCatalog, catalog_err};
use crate::event::streaming_mv::StreamingMvDef;
use crate::types::DatabaseId;

pub fn put(definition: &StreamingMvDef, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_streaming_mv(definition).map_err(|e| {
        catalog_err(
            &format!(
                "put_streaming_materialized_view '{}' (database {}, tenant {})",
                definition.name,
                definition.database_id.as_u64(),
                definition.tenant_id
            ),
            e,
        )
    })?;
    super::owner::put(
        &StoredOwner {
            database_id: definition.database_id.as_u64(),
            object_type: object_type::STREAMING_MATERIALIZED_VIEW.to_string(),
            object_name: definition.name.clone(),
            tenant_id: definition.tenant_id,
            owner_username: definition.owner.clone(),
        },
        catalog,
    )
}

pub fn delete(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog.delete_streaming_mv(DatabaseId::new(database_id), tenant_id, name)?;
    super::owner::delete_parent_owner(
        object_type::STREAMING_MATERIALIZED_VIEW,
        database_id,
        tenant_id,
        name,
        catalog,
    )
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

    fn definition(database_id: DatabaseId) -> StreamingMvDef {
        StreamingMvDef {
            database_id,
            tenant_id: 7,
            name: "orders_summary".into(),
            source_stream: "orders_stream".into(),
            group_by_columns: Vec::new(),
            aggregates: Vec::new(),
            filter_expr: None,
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn delete_is_scoped_to_database_and_removes_matching_owner() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        for database_id in [DatabaseId::new(1), DatabaseId::new(2)] {
            let definition = definition(database_id);
            catalog.put_streaming_mv(&definition).unwrap();
            catalog
                .put_owner(&StoredOwner {
                    database_id: database_id.as_u64(),
                    object_type: object_type::STREAMING_MATERIALIZED_VIEW.into(),
                    object_name: definition.name,
                    tenant_id: 7,
                    owner_username: "admin".into(),
                })
                .unwrap();
        }

        apply_to(
            &CatalogEntry::DeleteStreamingMaterializedView {
                database_id: 1,
                tenant_id: 7,
                name: "orders_summary".into(),
            },
            catalog,
        )
        .expect("apply delete_streaming_materialized_view");

        let remaining = catalog.load_all_streaming_mvs().unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].database_id, DatabaseId::new(2));
        let owners = catalog.load_all_owners().unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].database_id, 2);
    }
}
