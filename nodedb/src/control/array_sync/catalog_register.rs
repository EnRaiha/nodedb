// SPDX-License-Identifier: BUSL-1.1

//! Shared `array_catalog` registration helper.
//!
//! Split out of `raft_apply.rs` (which was pushing the 500-line file-size
//! limit) so both array-schema-import codepaths — the Raft-apply path
//! ([`crate::control::array_sync::raft_apply::apply_array_schema`], run on
//! every replica after Raft commit) and the single-node direct-import path
//! (`OriginArrayInbound::handle_schema`'s no-cluster branch, which never
//! goes through Raft) — converge on one registration routine instead of
//! duplicating (and risking drift between) the catalog-entry construction.

use std::sync::Arc;

use crate::control::array_catalog::entry::ArrayCatalogEntry;
use crate::control::state::SharedState;

/// Register (or no-op if already present) an [`ArrayCatalogEntry`] for
/// `array` by reading back its just-imported schema from
/// `state.array_sync_schemas`, and persist it to the system catalog.
///
/// Without this call, a synced array's schema lands in `array_sync_schemas`
/// but the array never becomes openable by the Data Plane
/// (`ensure_array_open` looks it up in `array_catalog`) and never becomes
/// visible to system-catalog introspection (`SHOW COLLECTIONS` merges in
/// `array_catalog::all_entries()`).
///
/// The persist is not optional. `ArrayCatalog::register` mutates an in-memory
/// registry that is rebuilt at boot from the system catalog alone
/// (`array_catalog::persist::load_all`), so an entry that is only registered
/// vanishes on restart — taking with it the Data Plane's ability to open the
/// array, and therefore WAL replay's ability to restore its cells. Both the
/// Raft-apply and the single-node direct-import path report success to a caller
/// that treats it as durable, so both must make it durable here.
///
/// Returns `Ok(())` when an entry already exists or was freshly registered.
/// Every successful return guarantees that the entry is persisted, so a retry
/// repairs a row that was lost after an earlier in-memory registration.
/// Returns `Err` on a genuine registration failure (schema not readable back,
/// encode failure, or catalog write error).
pub(crate) fn register_array_catalog_entry(
    state: &Arc<SharedState>,
    tenant_id: crate::types::TenantId,
    database_id: crate::types::DatabaseId,
    array: &str,
) -> crate::Result<()> {
    let schema = state
        .array_sync_schemas
        .to_array_schema_in_database(database_id, tenant_id.as_u64(), array)
        .ok_or_else(|| {
        crate::Error::Internal {
            detail: format!(
                "register_array_catalog_entry: to_array_schema returned None for '{array}' after import"
            ),
        }
    })?;
    let schema_msgpack = zerompk::to_msgpack_vec(&schema).map_err(|e| crate::Error::Internal {
        detail: format!("register_array_catalog_entry: schema_msgpack encode failed: {e}"),
    })?;

    let array_id = nodedb_array::types::ArrayId::in_database(tenant_id, database_id, array);
    let entry = ArrayCatalogEntry {
        array_id: array_id.clone(),
        name: array.to_string(),
        schema_msgpack,
        schema_hash: 0,
        created_at_ms: 0,
        prefix_bits: 8,
        audit_retain_ms: None,
        minimum_audit_retain_ms: None,
    };
    // Persist first, including the duplicate/retry case. A previous attempt
    // may have registered this entry in memory but failed before its durable
    // write; treating that state as a no-op would make the array disappear on
    // restart. Keep the lock through the write so concurrent callers cannot
    // observe a durable/in-memory split.
    let mut cat = state
        .array_catalog
        .write()
        .unwrap_or_else(|p| p.into_inner());
    persist_then_register_if_missing(&mut cat, entry, |entry| {
        crate::control::array_catalog::persist::persist(state.credentials.catalog(), entry).map_err(
            |e| crate::Error::Internal {
                detail: format!("register_array_catalog_entry: catalog persist failed: {e}"),
            },
        )
    })
}

fn persist_then_register_if_missing<F>(
    catalog: &mut crate::control::array_catalog::ArrayCatalog,
    entry: ArrayCatalogEntry,
    persist: F,
) -> crate::Result<()>
where
    F: FnOnce(&ArrayCatalogEntry) -> crate::Result<()>,
{
    persist(&entry)?;
    if catalog.lookup_by_id(&entry.array_id).is_none() {
        catalog
            .register(entry)
            .map_err(|e| crate::Error::Internal {
                detail: format!("register_array_catalog_entry: catalog register failed: {e}"),
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::types::ArrayId;
    use nodedb_types::{DatabaseId, TenantId};

    fn entry() -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::in_database(TenantId::new(9), DatabaseId::new(4), "synced"),
            name: "synced".into(),
            schema_msgpack: vec![0x80],
            schema_hash: 0,
            created_at_ms: 0,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        }
    }

    #[test]
    fn retry_repairs_missing_durable_entry_already_in_memory() {
        let entry = entry();
        let mut catalog = crate::control::array_catalog::ArrayCatalog::new();
        catalog.register(entry.clone()).unwrap();
        let mut persisted = false;
        assert!(!persisted, "fixture begins with the durable row missing");

        persist_then_register_if_missing(&mut catalog, entry.clone(), |_| {
            persisted = true;
            Ok(())
        })
        .unwrap();

        assert!(persisted);
        assert_eq!(catalog.lookup_by_id(&entry.array_id), Some(entry));
    }
}
