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
            catalog.delete_vector_index_params(tenant_id, name, record.primary_field())?;
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
