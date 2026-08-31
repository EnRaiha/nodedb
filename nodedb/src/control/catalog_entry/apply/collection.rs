// SPDX-License-Identifier: BUSL-1.1

//! Apply Collection catalog entries to `SystemCatalog` redb.

use nodedb_types::DatabaseId;
use tracing::{debug, warn};

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};

pub fn put(stored: &StoredCollection, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_collection(stored.database_id, stored) {
        warn!(
            collection = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_collection failed"
        );
    }
    super::owner::put_parent_owner_in_database(
        object_type::COLLECTION,
        stored.database_id.as_u64(),
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
    // An index is only observable while its collection is. `UNDROP COLLECTION`
    // reaches here with `is_active = true`, which restores the indexes the
    // soft-delete hid.
    sync_index_visibility(stored, catalog);
}

/// Align the collection's index records with its own `is_active` state, so a
/// soft-dropped collection hides its indexes and an undropped one brings them
/// back. Indexes are never deleted here — that happens only at purge.
pub(super) fn sync_index_visibility(stored: &StoredCollection, catalog: &SystemCatalog) {
    set_index_visibility(
        stored.database_id.as_u64(),
        stored.tenant_id,
        &stored.name,
        stored.is_active,
        catalog,
    );
}

fn set_index_visibility(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    is_active: bool,
    catalog: &SystemCatalog,
) {
    if let Err(e) =
        catalog.set_index_records_active_for_collection(database_id, tenant_id, name, is_active)
    {
        warn!(
            collection = %name,
            tenant = tenant_id,
            is_active,
            error = %e,
            "catalog_entry: index visibility sync failed"
        );
    }
}

/// Create-only variant of [`put`]: writes the collection (and its owner row)
/// exactly as `put` does, but only when no collection with the same
/// `(database_id, tenant_id, name)` already exists — a no-op otherwise, so
/// replay/snapshot re-application stays idempotent.
pub fn put_if_absent(stored: &StoredCollection, catalog: &SystemCatalog) {
    match catalog.put_collection_if_absent(stored.database_id, stored) {
        Ok(true) => super::owner::put_parent_owner_in_database(
            object_type::COLLECTION,
            stored.database_id.as_u64(),
            stored.tenant_id,
            &stored.name,
            &stored.owner,
            catalog,
        ),
        Ok(false) => debug!(
            collection = %stored.name,
            tenant = stored.tenant_id,
            "catalog_entry: put_collection_if_absent skipped existing collection"
        ),
        Err(e) => warn!(
            collection = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: atomic put_collection_if_absent failed"
        ),
    }
}

/// Persist the fail-closed catalog half of a purge before touching storage.
/// The inactive row survives crashes and blocks a same-name CREATE/UNDROP
/// from crossing an incomplete reclaim.
///
/// Returns whether a row was found and deactivated. `false` is legitimate
/// only for the replicated applier (may never have held the row) — a caller
/// that already read the row must use [`prepare_purge_checked`] instead.
pub fn prepare_purge(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<bool> {
    let database_id = DatabaseId::new(database_id);
    let Some(mut stored) = catalog.get_collection(database_id, tenant_id, name)? else {
        return Ok(false);
    };
    stored.is_active = false;
    catalog.put_collection(database_id, &stored)?;
    // Hide the indexes for the window between the fail-closed row write and
    // `finalize_purge`, which removes their records outright.
    set_index_visibility(database_id.as_u64(), tenant_id, name, false, catalog);
    Ok(true)
}

/// Fail-closed [`prepare_purge`] for callers that resolved the collection
/// before asking for the purge. A miss is never benign: the reclaim would run
/// while the row is active and a same-name CREATE could register over keys
/// the old incarnation still owns. Raises rather than reporting success.
pub fn prepare_purge_checked(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    if prepare_purge(database_id, tenant_id, name, catalog)? {
        return Ok(());
    }
    crate::diag::collection_purge_row_missing(database_id, tenant_id, name);
    Err(crate::Error::CollectionPurgeRowMissing {
        database_id,
        tenant_id,
        name: name.to_string(),
    })
}

/// Remove catalog metadata only after every persistent engine surface has been
/// reclaimed. The primary inactive row is deleted last, so any intermediate
/// failure continues to block same-name lifecycle operations across restart.
pub fn finalize_purge(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let database_id = DatabaseId::new(database_id);
    super::owner::delete_parent_owner_in_database_checked(
        object_type::COLLECTION,
        database_id.as_u64(),
        tenant_id,
        name,
        catalog,
    )?;
    catalog.delete_all_surrogates_for_collection(
        database_id,
        nodedb_types::TenantId::new(tenant_id),
        name,
    )?;
    // An index cannot outlive the collection it indexes. Its identity rows,
    // its ownership rows, and any engine-side build parameters go with the
    // collection; the Data Plane storage itself is reclaimed by the
    // `UnregisterCollection` half of the purge.
    purge_index_records(database_id.as_u64(), tenant_id, name, catalog)?;
    let removed = catalog.delete_collection(database_id, tenant_id, name)?;
    debug!(
        collection = %name,
        tenant = tenant_id,
        removed,
        "catalog_entry: purge_collection finalized"
    );
    Ok(())
}

/// Remove every index record of `name`, along with each index's ownership row
/// and (for vector indexes) its durable build parameters.
fn purge_index_records(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let records = catalog.list_index_records_for_collection(database_id, tenant_id, name)?;
    for record in &records {
        if record.kind == crate::control::security::catalog::IndexKind::Vector {
            catalog.delete_vector_index_params(
                database_id,
                tenant_id,
                name,
                record.primary_field(),
            )?;
        }
        catalog.delete_owner(
            record.kind.owner_object_type(),
            database_id,
            tenant_id,
            &record.name,
        )?;
        catalog.delete_index_record(database_id, tenant_id, &record.name)?;
    }
    debug!(
        collection = %name,
        tenant = tenant_id,
        indexes = records.len(),
        "catalog_entry: purge_collection removed index records"
    );
    Ok(())
}

/// Ordering metadata a soft delete records on the collection row, frozen at
/// propose time by `descriptor_stamp::stamp` and carried inside the entry so
/// every replica writes identical values.
pub struct DeactivateStamp {
    pub descriptor_version: u64,
    pub modification_hlc: nodedb_types::Hlc,
}

pub fn deactivate(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    stamp: DeactivateStamp,
    catalog: &SystemCatalog,
) {
    let database_id = DatabaseId::new(database_id);
    match catalog.get_collection(database_id, tenant_id, name) {
        Ok(Some(mut stored)) => {
            stored.is_active = false;
            // The drop is itself a descriptor mutation: without its own
            // version and HLC the row keeps the CREATE's metadata, and a
            // replayed CREATE cannot be ordered against it. Version `0` is
            // the pre-stamping sentinel — an unstamped entry carries no
            // ordering information, so the row keeps what it already had
            // rather than being reset behind the CREATE.
            if stamp.descriptor_version != 0 {
                stored.descriptor_version = stamp.descriptor_version;
                stored.modification_hlc = stamp.modification_hlc;
                // Retention (`resolve_retention`) reads `deactivated_at_ns`,
                // not `modification_hlc`, as the drop time — stamp both from
                // the same value so the two never diverge.
                stored.deactivated_at_ns = stamp.modification_hlc.wall_ns;
            }
            if let Err(e) = catalog.put_collection(database_id, &stored) {
                warn!(
                    collection = %name,
                    tenant = tenant_id,
                    error = %e,
                    "catalog_entry: deactivate_collection put failed"
                );
            }
            // Hide the collection's indexes for as long as the collection
            // itself is hidden. They are retained, not dropped: `UNDROP
            // COLLECTION` must restore the collection with its indexes.
            set_index_visibility(database_id.as_u64(), tenant_id, name, false, catalog);
        }
        Ok(None) => {
            debug!(
                collection = %name,
                tenant = tenant_id,
                "catalog_entry: deactivate on missing collection (fresh follower)"
            );
        }
        Err(e) => warn!(
            collection = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: deactivate_collection get failed"
        ),
    }
    // Intentionally preserve the `StoredOwner` row on soft-delete: the
    // primary record's `owner` field stays populated, and stripping the
    // owner row would break `UNDROP COLLECTION`'s ownership restore.
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::{DatabaseId, HlcClock};

    use super::*;
    use crate::control::catalog_entry::apply::apply_to;
    use crate::control::catalog_entry::codec::{decode, encode};
    use crate::control::catalog_entry::descriptor_stamp::stamp;
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
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        };
        let bytes = encode(&entry).unwrap();
        match decode(&bytes).unwrap() {
            CatalogEntry::DeactivateCollection {
                database_id,
                tenant_id,
                name,
                ..
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
                descriptor_version: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
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

    /// DROP COLLECTION is a soft delete, so it must advance the same ordering
    /// metadata a CREATE or ALTER would — a replayed CREATE cannot be ordered
    /// against the current row otherwise, and retention (which reads
    /// `modification_hlc` as the drop time) would measure from the original
    /// CREATE instead. Drives the entry through the exact production path:
    /// `descriptor_stamp::stamp` then `apply_to`.
    #[test]
    fn apply_deactivate_collection_advances_descriptor_version_and_hlc() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let clock = HlcClock::new();

        let stored = StoredCollection::new(1, "audit_log", "carol");
        let create = stamp(
            CatalogEntry::PutCollection(Box::new(stored)),
            &clock,
            catalog,
        );
        let CatalogEntry::PutCollection(created) = &create else {
            panic!("expected PutCollection");
        };
        let create_version = created.descriptor_version;
        let create_hlc = created.modification_hlc;
        apply_to(&create, catalog).expect("apply put_collection");

        let deactivate = stamp(
            CatalogEntry::DeactivateCollection {
                database_id: 0,
                tenant_id: 1,
                name: "audit_log".into(),
                descriptor_version: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
            },
            &clock,
            catalog,
        );
        apply_to(&deactivate, catalog).expect("apply deactivate_collection");

        let loaded = catalog
            .get_collection(DatabaseId::DEFAULT, 1, "audit_log")
            .unwrap()
            .expect("record preserved");
        assert!(!loaded.is_active);
        assert_eq!(
            loaded.descriptor_version,
            create_version + 1,
            "DROP must consume its own descriptor version, not leave the CREATE's version in place"
        );
        assert!(
            loaded.modification_hlc > create_hlc,
            "DROP must stamp a fresh modification_hlc, not leave the CREATE's HLC in place"
        );
    }

    /// `deactivate()` must stamp `deactivated_at_ns` from the same
    /// `modification_hlc.wall_ns` it stamps on the row — this is the field
    /// `resolve_retention` reads instead of `modification_hlc`, so a mismatch
    /// here reproduces the pre-fix bug one field over.
    #[test]
    fn apply_deactivate_collection_stamps_deactivated_at_ns_from_modification_hlc() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let clock = HlcClock::new();

        let stored = StoredCollection::new(1, "deactivated_stamp", "carol");
        let create = stamp(
            CatalogEntry::PutCollection(Box::new(stored)),
            &clock,
            catalog,
        );
        apply_to(&create, catalog).expect("apply put_collection");

        let deactivate = stamp(
            CatalogEntry::DeactivateCollection {
                database_id: 0,
                tenant_id: 1,
                name: "deactivated_stamp".into(),
                descriptor_version: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
            },
            &clock,
            catalog,
        );
        apply_to(&deactivate, catalog).expect("apply deactivate_collection");

        let loaded = catalog
            .get_collection(DatabaseId::DEFAULT, 1, "deactivated_stamp")
            .unwrap()
            .expect("record preserved");
        assert_ne!(
            loaded.deactivated_at_ns, 0,
            "deactivate() must stamp a non-zero deactivated_at_ns"
        );
        assert_eq!(
            loaded.deactivated_at_ns, loaded.modification_hlc.wall_ns,
            "deactivated_at_ns must equal the stamped modification_hlc.wall_ns"
        );
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
        apply_to(&CatalogEntry::PutCollection(Box::new(other)), catalog)
            .expect("apply put_collection");

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
        finalize_purge(9, 1, "shared", catalog).expect("finalize purge for database 9");

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
                descriptor_version: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
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

        let found = prepare_purge(0, 1, "orders", catalog).expect("prepare purge");
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
            prepare_purge(0, 1, "ghost", catalog).expect("missing row is not a read failure");
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

        let err = prepare_purge_checked(1024, 1, "orders", catalog)
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

        prepare_purge_checked(1024, 1, "orders", catalog)
            .expect("prepare purge under the owning database");
        assert!(
            !catalog
                .get_collection(DatabaseId::new(1024), 1, "orders")
                .unwrap()
                .expect("row preserved")
                .is_active
        );
    }
}
