// SPDX-License-Identifier: BUSL-1.1

//! Array-catalog redb ops for the `_system.arrays` table.
//!
//! Mirrors the `triggers.rs` shape: typed put/get/delete plus a bulk
//! loader for startup. Keyed by `name` (already globally scoped in the
//! `ArrayCatalogEntry` via `ArrayId`'s tenant field — a second-level
//! tenant prefix would only duplicate that information).

use redb::{ReadableDatabase, ReadableTable};

use crate::control::array_catalog::ArrayCatalogEntry;

use super::types::{ARRAYS, SURROGATE_PK_REV_V3, SURROGATE_PK_V3, SystemCatalog, catalog_err};

/// Version-two catalog key.  It includes both tenant and database because a
/// database id is only meaningful inside a tenant.
fn array_storage_key(
    tenant_id: nodedb_types::TenantId,
    database_id: nodedb_types::DatabaseId,
    name: &str,
) -> String {
    format!(
        "\u{0}v2:{}:{}:{name}",
        tenant_id.as_u64(),
        database_id.as_u64()
    )
}

fn array_storage_key_for_entry(entry: &ArrayCatalogEntry) -> String {
    array_storage_key(
        entry.array_id.tenant_id,
        entry.array_id.database_id,
        &entry.name,
    )
}

impl SystemCatalog {
    /// Insert or overwrite an array catalog entry.
    pub fn put_array(&self, entry: &ArrayCatalogEntry) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(entry).map_err(|e| catalog_err("serialize array", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ARRAYS)
                .map_err(|e| catalog_err("open arrays", e))?;
            let key = array_storage_key_for_entry(entry);
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert array", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Fetch an array by its explicit tenant/database identity.
    pub fn get_array_in_database(
        &self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> crate::Result<Option<ArrayCatalogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(ARRAYS)
            .map_err(|e| catalog_err("open arrays", e))?;
        let key = array_storage_key(tenant_id, database_id, name);
        table
            .get(key.as_str())
            .map_err(|e| catalog_err("get array", e))?
            .map(|value| zerompk::from_msgpack(value.value()))
            .transpose()
            .map_err(|e| catalog_err("deser array", e))
    }

    /// Delete by explicit tenant/database identity.
    pub fn delete_array_in_database(
        &self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(ARRAYS)
                .map_err(|e| catalog_err("open arrays", e))?;
            let key = array_storage_key(tenant_id, database_id, name);
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove array", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Atomically delete an array catalog row and all of its surrogate
    /// bindings. Keeping these mutations in one redb write transaction means
    /// a failed DROP cannot leave an array without its identity mappings (or
    /// vice versa).
    pub fn delete_array_and_surrogates_in_database(
        &self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("array/surrogate delete txn", e))?;
        let db_id = database_id.as_u64();
        let tid = tenant_id.as_u64();
        let existed;
        {
            let mut arrays = write_txn
                .open_table(ARRAYS)
                .map_err(|e| catalog_err("open arrays", e))?;
            let key = array_storage_key(tenant_id, database_id, name);
            existed = arrays
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove array", e))?
                .is_some();

            let mut forward = write_txn
                .open_table(SURROGATE_PK_V3)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            let bindings: Vec<(Vec<u8>, u32)> = forward
                .range((db_id, tid, name, [].as_slice())..)
                .map_err(|e| catalog_err("range surrogate_pk", e))?
                .take_while(|row| match row {
                    Ok((key, _)) => {
                        let (row_db, row_tenant, collection, _) = key.value();
                        row_db == db_id && row_tenant == tid && collection == name
                    }
                    Err(_) => true,
                })
                .map(|row| {
                    let (key, value) = row.map_err(|e| catalog_err("iter surrogate_pk", e))?;
                    Ok((key.value().3.to_vec(), value.value()))
                })
                .collect::<crate::Result<_>>()?;
            let mut reverse = write_txn
                .open_table(SURROGATE_PK_REV_V3)
                .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
            for (pk, surrogate) in bindings {
                forward
                    .remove((db_id, tid, name, pk.as_slice()))
                    .map_err(|e| catalog_err("remove surrogate_pk", e))?;
                reverse
                    .remove((db_id, tid, name, surrogate))
                    .map_err(|e| catalog_err("remove surrogate_pk_rev", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit array/surrogate delete", e))?;
        Ok(existed)
    }

    /// Legacy DEFAULT-database read API.
    pub fn get_array(&self, name: &str) -> crate::Result<Option<ArrayCatalogEntry>> {
        self.get_array_in_database(
            nodedb_types::TenantId::new(0),
            nodedb_types::DatabaseId::DEFAULT,
            name,
        )
    }

    /// Legacy DEFAULT-database delete API.
    pub fn delete_array(&self, name: &str) -> crate::Result<bool> {
        self.delete_array_in_database(
            nodedb_types::TenantId::new(0),
            nodedb_types::DatabaseId::DEFAULT,
            name,
        )
    }

    /// Load every entry. Used by `ArrayCatalog::load_from_catalog` at
    /// startup.
    pub fn load_all_arrays(&self) -> crate::Result<Vec<ArrayCatalogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        load_all_arrays_in(&read_txn)
    }
}

/// Body of [`SystemCatalog::load_all_arrays`], over an already-open read
/// transaction so the read-only catalog handle can reuse it verbatim.
pub(super) fn load_all_arrays_in(
    read_txn: &redb::ReadTransaction,
) -> crate::Result<Vec<ArrayCatalogEntry>> {
    let table = read_txn
        .open_table(ARRAYS)
        .map_err(|e| catalog_err("open arrays", e))?;
    let mut rows = Vec::new();
    let iter = table.iter().map_err(|e| catalog_err("iter arrays", e))?;
    for row in iter {
        let (_, value) = row.map_err(|e| catalog_err("iter row", e))?;
        let entry: ArrayCatalogEntry =
            zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser array", e))?;
        rows.push(entry);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::types::ArrayId;
    use nodedb_types::{DatabaseId, TenantId};

    fn catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn entry(
        tenant_id: u64,
        database_id: DatabaseId,
        name: &str,
        schema_hash: u64,
    ) -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::in_database(TenantId::new(tenant_id), database_id, name),
            name: name.into(),
            schema_msgpack: vec![0x80],
            schema_hash,
            created_at_ms: 1,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        }
    }

    #[test]
    fn same_name_is_isolated_across_tenants_and_databases() {
        let catalog = catalog();
        let tenant_one_db_one = entry(1, DatabaseId::new(1), "same", 1);
        let tenant_two_db_one = entry(2, DatabaseId::new(1), "same", 2);
        let tenant_one_db_two = entry(1, DatabaseId::new(2), "same", 3);
        catalog.put_array(&tenant_one_db_one).unwrap();
        catalog.put_array(&tenant_two_db_one).unwrap();
        catalog.put_array(&tenant_one_db_two).unwrap();

        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(1), DatabaseId::new(1), "same")
                .unwrap(),
            Some(tenant_one_db_one)
        );
        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(2), DatabaseId::new(1), "same")
                .unwrap(),
            Some(tenant_two_db_one)
        );
        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(1), DatabaseId::new(2), "same")
                .unwrap(),
            Some(tenant_one_db_two)
        );
    }

    #[test]
    fn a_replacing_put_is_visible_to_both_read_paths() {
        let catalog = catalog();
        catalog
            .put_array(&entry(1, DatabaseId::DEFAULT, "same", 1))
            .unwrap();
        let replacement = entry(1, DatabaseId::DEFAULT, "same", 2);
        catalog.put_array(&replacement).unwrap();

        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(1), DatabaseId::DEFAULT, "same")
                .unwrap(),
            Some(replacement.clone())
        );
        assert_eq!(catalog.load_all_arrays().unwrap(), vec![replacement]);
    }

    #[test]
    fn delete_is_scoped_to_one_tenant_and_database() {
        let catalog = catalog();
        let kept = entry(2, DatabaseId::DEFAULT, "same", 1);
        catalog.put_array(&kept).unwrap();

        assert!(
            !catalog
                .delete_array_in_database(TenantId::new(1), DatabaseId::DEFAULT, "same")
                .unwrap()
        );
        assert!(
            !catalog
                .delete_array_in_database(TenantId::new(2), DatabaseId::new(7), "same")
                .unwrap()
        );
        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(2), DatabaseId::DEFAULT, "same")
                .unwrap(),
            Some(kept)
        );
        assert!(
            catalog
                .delete_array_in_database(TenantId::new(2), DatabaseId::DEFAULT, "same")
                .unwrap()
        );
    }
}
