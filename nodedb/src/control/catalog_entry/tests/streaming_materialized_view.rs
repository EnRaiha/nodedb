// SPDX-License-Identifier: BUSL-1.1

use crate::control::catalog_entry::apply::apply_to;
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::tests::open_catalog;
use crate::control::security::catalog::StoredOwner;
use crate::control::security::catalog::auth_types::object_type;
use crate::event::streaming_mv::StreamingMvDef;
use crate::types::DatabaseId;

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
    );

    let remaining = catalog.load_all_streaming_mvs().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].database_id, DatabaseId::new(2));
    let owners = catalog.load_all_owners().unwrap();
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].database_id, 2);
}
