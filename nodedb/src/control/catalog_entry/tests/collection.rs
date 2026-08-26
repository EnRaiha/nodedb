// SPDX-License-Identifier: BUSL-1.1

//! Collection-family tests: roundtrip + apply semantics.

use crate::control::catalog_entry::apply::apply_to;
use crate::control::catalog_entry::codec::{decode, encode};
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::tests::open_catalog;
use crate::control::security::catalog::StoredCollection;
use nodedb_types::DatabaseId;

#[test]
fn roundtrip_put_collection() {
    let stored = StoredCollection::new(7, "orders", "alice");
    let entry = CatalogEntry::PutCollection(Box::new(stored));
    let bytes = encode(&entry).expect("encode");
    let decoded = decode(&bytes).expect("decode");
    match decoded {
        CatalogEntry::PutCollection(s) => {
            assert_eq!(s.tenant_id, 7);
            assert_eq!(s.name, "orders");
            assert_eq!(s.owner, "alice");
        }
        other => panic!("expected PutCollection, got {other:?}"),
    }
}

#[test]
fn roundtrip_deactivate_collection() {
    let entry = CatalogEntry::DeactivateCollection {
        database_id: 0,
        tenant_id: 3,
        name: "legacy".into(),
    };
    let bytes = encode(&entry).unwrap();
    match decode(&bytes).unwrap() {
        CatalogEntry::DeactivateCollection {
            database_id,
            tenant_id,
            name,
        } => {
            assert_eq!(database_id, 0);
            assert_eq!(tenant_id, 3);
            assert_eq!(name, "legacy");
        }
        other => panic!("expected DeactivateCollection, got {other:?}"),
    }
}

#[test]
fn apply_put_collection_writes_redb() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog();

    let stored = StoredCollection::new(1, "widgets", "carol");
    apply_to(&CatalogEntry::PutCollection(Box::new(stored)), catalog)
        .expect("apply put_collection");

    let loaded = catalog
        .get_collection(DatabaseId::DEFAULT, 1, "widgets")
        .unwrap()
        .expect("present");
    assert_eq!(loaded.name, "widgets");
    assert_eq!(loaded.owner, "carol");
    assert!(loaded.is_active);
}

#[test]
fn apply_deactivate_collection_preserves_record() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog();

    // Set up through `apply_to` so the owner row is written alongside the
    // primary row, avoiding an orphan-row integrity trip on deactivate.
    let stored = StoredCollection::new(1, "archived", "carol");
    apply_to(&CatalogEntry::PutCollection(Box::new(stored)), catalog)
        .expect("apply put_collection");

    apply_to(
        &CatalogEntry::DeactivateCollection {
            database_id: 0,
            tenant_id: 1,
            name: "archived".into(),
        },
        catalog,
    )
    .expect("apply deactivate_collection");

    let loaded = catalog
        .get_collection(DatabaseId::DEFAULT, 1, "archived")
        .unwrap()
        .expect("record preserved");
    assert!(!loaded.is_active);
}

#[test]
fn purge_collection_is_scoped_to_database() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog();
    let default = StoredCollection::new(1, "shared", "default_owner");
    let mut other = StoredCollection::new(1, "shared", "other_owner");
    other.database_id = DatabaseId::new(9);
    apply_to(&CatalogEntry::PutCollection(Box::new(default)), catalog)
        .expect("apply put_collection");
    apply_to(&CatalogEntry::PutCollection(Box::new(other)), catalog).expect("apply put_collection");

    // `PurgeCollection` only deactivates the row; `finalize_purge` deletes it.
    // Drive both to assert the delete is scoped to the target database.
    apply_to(
        &CatalogEntry::PurgeCollection {
            database_id: 9,
            tenant_id: 1,
            name: "shared".into(),
        },
        catalog,
    )
    .expect("apply purge_collection");
    crate::control::catalog_entry::apply::collection::finalize_purge(9, 1, "shared", catalog)
        .expect("finalize purge for database 9");

    assert!(
        catalog
            .get_collection(DatabaseId::new(9), 1, "shared")
            .unwrap()
            .is_none()
    );
    assert!(
        catalog
            .get_collection(DatabaseId::DEFAULT, 1, "shared")
            .unwrap()
            .is_some()
    );
    let owners = catalog.load_all_owners().unwrap();
    assert!(!owners.iter().any(|owner| owner.database_id == 9));
    assert!(owners.iter().any(|owner| owner.database_id == 0));
}

#[test]
fn apply_deactivate_missing_is_noop() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog();
    apply_to(
        &CatalogEntry::DeactivateCollection {
            database_id: 0,
            tenant_id: 1,
            name: "ghost".into(),
        },
        catalog,
    )
    .expect("apply deactivate_collection on missing row is a no-op, not an error");
    assert!(
        catalog
            .get_collection(DatabaseId::DEFAULT, 1, "ghost")
            .unwrap()
            .is_none()
    );
}

#[test]
fn prepare_purge_reports_the_row_it_deactivated() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog();
    let stored = StoredCollection::new(1, "orders", "tester");
    apply_to(&CatalogEntry::PutCollection(Box::new(stored)), catalog).expect("apply");

    let found =
        crate::control::catalog_entry::apply::collection::prepare_purge(0, 1, "orders", catalog)
            .expect("prepare purge");
    assert!(found);
    assert!(
        !catalog
            .get_collection(DatabaseId::DEFAULT, 1, "orders")
            .unwrap()
            .expect("row preserved")
            .is_active
    );
}

/// The replicated applier runs on nodes that may never have held the row, so
/// `prepare_purge` reports the miss instead of raising.
#[test]
fn prepare_purge_reports_a_missing_row_without_raising() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog();
    let found =
        crate::control::catalog_entry::apply::collection::prepare_purge(0, 1, "ghost", catalog)
            .expect("missing row is not a read failure");
    assert!(!found);
}

/// A collection stored under the default database is invisible to a purge
/// looking under another one. The checked variant must not report success.
#[test]
fn prepare_purge_checked_rejects_a_database_id_that_holds_no_row() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog();
    let stored = StoredCollection::new(1, "orders", "tester");
    apply_to(&CatalogEntry::PutCollection(Box::new(stored)), catalog).expect("apply");

    let err = crate::control::catalog_entry::apply::collection::prepare_purge_checked(
        1024, 1, "orders", catalog,
    )
    .expect_err("a purge that deactivates nothing must not report success");
    assert!(matches!(
        err,
        crate::Error::CollectionPurgeRowMissing {
            database_id: 1024,
            tenant_id: 1,
            ..
        }
    ));
    // The row under the database that actually holds it is untouched.
    assert!(
        catalog
            .get_collection(DatabaseId::DEFAULT, 1, "orders")
            .unwrap()
            .expect("row preserved")
            .is_active
    );
}

#[test]
fn prepare_purge_checked_accepts_the_database_that_holds_the_row() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog();
    let mut stored = StoredCollection::new(1, "orders", "tester");
    stored.database_id = DatabaseId::new(1024);
    apply_to(&CatalogEntry::PutCollection(Box::new(stored)), catalog).expect("apply");

    crate::control::catalog_entry::apply::collection::prepare_purge_checked(
        1024, 1, "orders", catalog,
    )
    .expect("prepare purge under the owning database");
    assert!(
        !catalog
            .get_collection(DatabaseId::new(1024), 1, "orders")
            .unwrap()
            .expect("row preserved")
            .is_active
    );
}
