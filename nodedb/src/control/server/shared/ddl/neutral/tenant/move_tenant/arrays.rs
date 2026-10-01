// SPDX-License-Identifier: BUSL-1.1

//! The arrays of a `MOVE TENANT`.
//!
//! A cell routes to its vShard by Hilbert prefix alone, so moving an array to
//! another database moves no cell to another vShard. Only the catalog key,
//! the surrogate bindings, and each core's store directory carry the
//! database. The cutover rekeys all three with a `DeleteArray` at the source
//! key, which carries the move, followed by a `PutArray` at the target key.

use nodedb_cluster::DescriptorId;
use nodedb_types::{DatabaseId, Hlc, NodeDbError};

use crate::control::array_catalog::{ArrayCatalogEntry, ArrayMove};
use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;
use crate::control::server::shared::clone_write::array_descriptor;
use crate::types::TenantId;

/// Every array of `database_id`.
pub fn arrays_in(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
) -> crate::Result<Vec<ArrayCatalogEntry>> {
    Ok(catalog
        .load_all_arrays()?
        .into_iter()
        .filter(|a| a.array_id.database_id == database_id)
        .collect())
}

/// The descriptor the drain phase drains for each source array.
pub fn source_descriptors(
    catalog: &SystemCatalog,
    source_db_id: DatabaseId,
) -> crate::Result<Vec<DescriptorId>> {
    Ok(arrays_in(catalog, source_db_id)?
        .iter()
        .map(|a| array_descriptor(&a.array_id))
        .collect())
}

/// Refuse the move when the target already holds an array a source array
/// will land on. The rekey renames each store into the target key, so that
/// key must be free.
pub fn preflight(
    catalog: &SystemCatalog,
    source_db_id: DatabaseId,
    target_db_id: DatabaseId,
    tenant_name: &str,
    target_db_name: &str,
) -> Result<(), NodeDbError> {
    let failed = |detail: String| NodeDbError::move_tenant_preflight_failed(tenant_name, detail);
    let arrays = arrays_in(catalog, source_db_id)
        .map_err(|e| failed(format!("failed to enumerate source arrays: {e}")))?;
    for array in arrays {
        let tenant = array.array_id.tenant_id;
        let taken = catalog
            .get_array_in_database(tenant, target_db_id, &array.name)
            .map_err(|e| {
                failed(format!(
                    "catalog lookup for array '{}' in target failed: {e}",
                    array.name
                ))
            })?;
        if taken.is_some() {
            return Err(failed(format!(
                "array '{}' exists in both the source database and target database \
                 '{target_db_name}'; drop one of them before the move",
                array.name
            )));
        }
    }
    Ok(())
}

/// The rekey entries for every source array, read from committed state.
/// Each source delete comes before its target put: the put opens the store
/// the delete renamed.
pub fn rekey_entries(
    catalog: &SystemCatalog,
    mover: TenantId,
    source_db_id: DatabaseId,
    target_db_id: DatabaseId,
) -> crate::Result<Vec<CatalogEntry>> {
    let mut entries = Vec::new();
    for array in arrays_in(catalog, source_db_id)? {
        let source = array.array_id.clone();
        entries.push(CatalogEntry::DeleteArray {
            database_id: source_db_id.as_u64(),
            tenant_id: source.tenant_id.as_u64(),
            name: array.name.clone(),
            // Frozen by the proposer's stamp.
            target_hlc: Hlc::ZERO,
            moved_to: Some(ArrayMove {
                target_db_id: target_db_id.as_u64(),
                mover_tenant_id: mover.as_u64(),
            }),
        });
        entries.push(CatalogEntry::PutArray(Box::new(ArrayCatalogEntry {
            array_id: nodedb_array::types::ArrayId::in_database(
                source.tenant_id,
                target_db_id,
                &source.name,
            ),
            // Frozen by the proposer's stamp.
            modification_hlc: Hlc::ZERO,
            ..array
        })));
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::types::ArrayId;

    fn catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().expect("tmpdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("open");
        (dir, catalog)
    }

    fn array(db: DatabaseId, name: &str) -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::in_database(TenantId::new(5), db, name),
            name: name.to_string(),
            schema_msgpack: vec![0x90],
            schema_hash: 7,
            created_at_ms: 0,
            prefix_bits: 8,
            audit_retain_ms: Some(60_000),
            minimum_audit_retain_ms: None,
            modification_hlc: Hlc::new(9, 0),
            incarnation: nodedb_types::Hlc::ZERO,
        }
    }

    /// Each source array becomes a moving delete at the source key followed
    /// by a put of the same definition at the target key.
    #[test]
    fn rekey_deletes_the_source_then_puts_the_target() {
        let (_dir, catalog) = catalog();
        let (src, tgt) = (DatabaseId::new(10), DatabaseId::new(11));
        catalog.put_array(&array(src, "grid")).expect("seed");
        catalog.put_array(&array(tgt, "other")).expect("seed");

        let entries = rekey_entries(&catalog, TenantId::new(77), src, tgt).expect("plan");

        let [
            CatalogEntry::DeleteArray {
                database_id,
                tenant_id,
                name,
                moved_to: Some(moved),
                ..
            },
            CatalogEntry::PutArray(put),
        ] = entries.as_slice()
        else {
            unreachable!("expected one delete and one put, got {entries:?}");
        };
        assert_eq!((*database_id, *tenant_id, name.as_str()), (10, 5, "grid"));
        assert_eq!(
            *moved,
            ArrayMove {
                target_db_id: 11,
                mover_tenant_id: 77,
            }
        );
        assert_eq!(
            put.array_id,
            ArrayId::in_database(TenantId::new(5), tgt, "grid")
        );
        assert_eq!(put.audit_retain_ms, Some(60_000));
        assert_eq!(put.modification_hlc, Hlc::ZERO);
    }

    #[test]
    fn preflight_refuses_an_occupied_target_key() {
        let (_dir, catalog) = catalog();
        let (src, tgt) = (DatabaseId::new(10), DatabaseId::new(11));
        catalog.put_array(&array(src, "grid")).expect("seed");
        preflight(&catalog, src, tgt, "t", "tgt").expect("a free target key passes");

        catalog.put_array(&array(tgt, "grid")).expect("seed");
        assert!(preflight(&catalog, src, tgt, "t", "tgt").is_err());
    }
}
