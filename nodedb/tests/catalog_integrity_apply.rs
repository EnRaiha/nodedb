// SPDX-License-Identifier: BUSL-1.1

//! Applier contract: for every parent-replicated `Put<T>` variant, the
//! synchronous `apply_to` path MUST write a matching `StoredOwner` row
//! to redb. If it does not, the next restart's integrity check aborts
//! boot with an `OrphanRow` divergence.

mod catalog_integrity_helpers;

use catalog_integrity_helpers::*;
use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::catalog_entry::apply::apply_to;
use nodedb::control::cluster::recovery_check::integrity::verify_redb_integrity;
use nodedb::control::security::catalog::auth_types::StoredOwner;
use nodedb::control::security::catalog::{DatabaseDescriptor, ParentCloneRef};
use nodedb_types::DatabaseId;

#[test]
fn apply_put_collection_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutCollection(Box::new(make_collection("orders")));
    apply_to(&entry, &catalog).expect("apply");
    assert!(
        owner_row_present(&catalog, "collection", "orders"),
        "PutCollection apply must write a StoredOwner row to redb; \
         missing row causes verify_redb_integrity to abort startup \
         with an OrphanRow(collection) divergence"
    );
}

#[test]
fn apply_put_function_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutFunction(Box::new(make_function("normalize_email")));
    apply_to(&entry, &catalog).expect("apply");
    assert!(
        owner_row_present(&catalog, "function", "normalize_email"),
        "PutFunction apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_procedure_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutProcedure(Box::new(make_procedure("purge_old")));
    apply_to(&entry, &catalog).expect("apply");
    assert!(
        owner_row_present(&catalog, "procedure", "purge_old"),
        "PutProcedure apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_trigger_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    // Write the parent collection first so Check 4 (trigger →
    // collection) doesn't also fire — this test is about the
    // owner-row gap only.
    catalog
        .put_collection(
            nodedb_types::DatabaseId::DEFAULT,
            &make_collection("orders"),
        )
        .unwrap();
    catalog
        .put_owner(&StoredOwner {
            database_id: 0,
            object_type: "collection".into(),
            object_name: "orders".into(),
            tenant_id: TENANT,
            owner_username: ADMIN.into(),
        })
        .unwrap();

    let entry = CatalogEntry::PutTrigger(Box::new(make_trigger("send_email", "orders")));
    apply_to(&entry, &catalog).expect("apply");
    assert!(
        owner_row_present(&catalog, "trigger", "send_email"),
        "PutTrigger apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_materialized_view_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutMaterializedView(Box::new(make_mv("orders_summary")));
    apply_to(&entry, &catalog).expect("apply");
    assert!(
        owner_row_present(&catalog, "materialized_view", "orders_summary"),
        "PutMaterializedView apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_sequence_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutSequence(Box::new(make_sequence("orders_seq")));
    apply_to(&entry, &catalog).expect("apply");
    assert!(
        owner_row_present(&catalog, "sequence", "orders_seq"),
        "PutSequence apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_schedule_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutSchedule(Box::new(make_schedule("nightly")));
    apply_to(&entry, &catalog).expect("apply");
    assert!(
        owner_row_present(&catalog, "schedule", "nightly"),
        "PutSchedule apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_change_stream_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutChangeStream(Box::new(make_stream("orders_cdc")));
    apply_to(&entry, &catalog).expect("apply");
    assert!(
        owner_row_present(&catalog, "change_stream", "orders_cdc"),
        "PutChangeStream apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_clone_database_writes_owner_row_for_shadow_collection() {
    let (_dir, catalog) = make_catalog();

    // Source collection, applied through the correct path so it carries
    // its own owner row — the assertion below is about the SHADOW
    // collection `CloneDatabase` stamps into the target database, not
    // about the source.
    apply_to(
        &CatalogEntry::PutCollection(Box::new(make_collection("notes"))),
        &catalog,
    )
    .expect("apply put_collection");

    let mut target_descriptor = DatabaseDescriptor::default_db();
    target_descriptor.id = DatabaseId::new(1025);
    target_descriptor.name = "cl_dst".into();
    target_descriptor.parent_clone = Some(ParentCloneRef {
        source_db_id: DatabaseId::DEFAULT,
        as_of_lsn: 0,
        as_of_ms: 0,
        kv_surrogate_ceiling: None,
    });

    let entry = CatalogEntry::CloneDatabase {
        target_descriptor: Box::new(target_descriptor),
        source_db_id: DatabaseId::DEFAULT.as_u64(),
    };
    apply_to(&entry, &catalog).expect("apply clone_database");

    assert!(
        owner_row_present(&catalog, "collection", "notes"),
        "CloneDatabase apply must write a StoredOwner row for every shadow \
         collection it stamps into the target database; missing row causes \
         verify_redb_integrity to abort startup with an OrphanRow(collection) \
         divergence, which is the failure this test guards against"
    );

    let violations = verify_redb_integrity(&catalog);
    assert!(
        violations.is_empty(),
        "expected zero integrity violations after clone_database, got: {violations:?}"
    );
}

#[test]
fn apply_move_tenant_cutover_writes_target_owner_and_removes_source_owner() {
    let (_dir, catalog) = make_catalog();

    let source_db = DatabaseId::DEFAULT;
    let target_db = DatabaseId::new(9);

    let source_coll = make_collection("orders");
    apply_to(
        &CatalogEntry::PutCollection(Box::new(source_coll.clone())),
        &catalog,
    )
    .expect("apply put_collection");
    assert!(owner_row_present(&catalog, "collection", "orders"));

    let entry = CatalogEntry::MoveTenantCutover {
        tenant_id: TENANT,
        source_db_id: source_db.as_u64(),
        target_db_id: target_db.as_u64(),
        collections: vec![source_coll],
    };
    apply_to(&entry, &catalog).expect("apply move_tenant_cutover");

    let owners = catalog.load_all_owners().unwrap();
    assert!(
        owners.iter().any(|o| o.object_type == "collection"
            && o.database_id == target_db.as_u64()
            && o.object_name == "orders"
            && o.owner_username == ADMIN),
        "MoveTenantCutover apply must write a StoredOwner row for the \
         collection's new (target-database) location"
    );
    assert!(
        !owners.iter().any(|o| o.object_type == "collection"
            && o.database_id == source_db.as_u64()
            && o.object_name == "orders"),
        "MoveTenantCutover apply must remove the stale StoredOwner row at \
         the collection's old (source-database) location"
    );
}
