// SPDX-License-Identifier: BUSL-1.1

//! Apply ownership catalog entries to `SystemCatalog` redb.
//!
//! Two write paths share this file:
//!
//! 1. **Standalone path** — [`put`] / [`delete`] handle
//!    `CatalogEntry::PutOwner` / `DeleteOwner` for objects that have
//!    no parent `Stored*` variant (indexes, spatial indexes).
//!
//! 2. **Parent-replicated path** — [`put_parent_owner`] /
//!    [`delete_parent_owner`] are the single write helpers used by
//!    every sibling applier for objects whose `Stored<T>` record
//!    carries an embedded `owner` field (collection, function,
//!    procedure, trigger, materialized_view, sequence, schedule,
//!    change_stream). Each applier writes the primary row and then
//!    calls one of these helpers so the `OWNERS` redb table — the
//!    persistent backing for the in-memory `PermissionStore.owners`
//!    HashMap — stays in lockstep with the primary row. Omitting
//!    the call leaves redb orphaned on the next restart
//!    (`verify_redb_integrity` aborts boot with `OrphanRow`).

use crate::control::security::catalog::{StoredOwner, SystemCatalog, catalog_err};

pub fn put(stored: &StoredOwner, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_owner(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_owner {} '{}' (tenant {})",
                stored.object_type, stored.object_name, stored.tenant_id
            ),
            e,
        )
    })
}

pub fn delete(
    object_type: &str,
    database_id: u64,
    tenant_id: u64,
    object_name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_owner(object_type, database_id, tenant_id, object_name)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "delete_owner {object_type} '{object_name}' \
                     (database {database_id}, tenant {tenant_id})"
                ),
                e,
            )
        })
}

/// Write the `StoredOwner` row for a parent-replicated DDL object.
///
/// Every `apply/<type>.rs::put` for the 8 parent-replicated types
/// must call this after writing the primary row. The primary row's
/// `owner` field is canonical; this call keeps the `OWNERS` redb
/// table in sync so `PermissionStore::load_from` rebuilds the
/// in-memory authorization map correctly on restart.
///
/// `database_id` must be the database the object lives in. The
/// owner row is keyed by it, so a wrong value grants ownership in
/// the wrong database.
pub(super) fn put_parent_owner(
    object_type: &'static str,
    database_id: u64,
    tenant_id: u64,
    object_name: &str,
    owner_username: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let stored = StoredOwner {
        database_id,
        object_type: object_type.to_string(),
        object_name: object_name.to_string(),
        tenant_id,
        owner_username: owner_username.to_string(),
    };
    catalog.put_owner(&stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_parent_owner {object_type} '{object_name}' \
                 (database {database_id}, tenant {tenant_id})"
            ),
            e,
        )
    })
}

/// Remove the `StoredOwner` row for a parent-replicated DDL object.
///
/// Symmetric counterpart of [`put_parent_owner`]. Every drop /
/// deactivate applier for the 8 parent-replicated types must call
/// this so the `OWNERS` redb table does not accumulate orphaned
/// rows after the primary record is gone. `database_id` must match
/// the value the matching [`put_parent_owner`] wrote.
pub(super) fn delete_parent_owner(
    object_type: &'static str,
    database_id: u64,
    tenant_id: u64,
    object_name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_owner(object_type, database_id, tenant_id, object_name)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "delete_parent_owner {object_type} '{object_name}' \
                     (database {database_id}, tenant {tenant_id})"
                ),
                e,
            )
        })
}
