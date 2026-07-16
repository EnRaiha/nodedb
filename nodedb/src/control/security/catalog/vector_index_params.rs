// SPDX-License-Identifier: BUSL-1.1

//! Catalog operations for durable vector-index parameters.
//!
//! Stores `CREATE VECTOR INDEX` build parameters in
//! `_system.vector_index_params`. Key format: `"{tenant_id}:{collection}:{field_name}"`.

use nodedb_types::StoredVectorIndexParams;

use super::types::{SystemCatalog, VECTOR_INDEX_PARAMS, catalog_err};

impl SystemCatalog {
    /// Store vector index parameters for a collection/field.
    pub fn put_vector_index_params(&self, entry: &StoredVectorIndexParams) -> crate::Result<()> {
        let key = vector_index_params_key(entry.tenant_id, &entry.collection, &entry.field_name);
        let bytes = zerompk::to_msgpack_vec(entry)
            .map_err(|e| catalog_err("serialize vector index params", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(VECTOR_INDEX_PARAMS)
                .map_err(|e| catalog_err("open vector_index_params", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert vector index params", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Load vector index parameters for a specific collection/field.
    pub fn get_vector_index_params(
        &self,
        tenant_id: u64,
        collection: &str,
        field_name: &str,
    ) -> crate::Result<Option<StoredVectorIndexParams>> {
        let key = vector_index_params_key(tenant_id, collection, field_name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(VECTOR_INDEX_PARAMS)
            .map_err(|e| catalog_err("open vector_index_params", e))?;

        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let entry: StoredVectorIndexParams = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser vector index params", e))?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get vector index params", e)),
        }
    }

    /// List all vector index parameter entries across all tenants.
    pub fn list_all_vector_index_params(&self) -> crate::Result<Vec<StoredVectorIndexParams>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(VECTOR_INDEX_PARAMS)
            .map_err(|e| catalog_err("open vector_index_params", e))?;

        let mut entries = Vec::new();
        for item in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range vector index params", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read vector index params", e))?;
            let entry: StoredVectorIndexParams = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser vector index params", e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

fn vector_index_params_key(tenant_id: u64, collection: &str, field_name: &str) -> String {
    format!("{tenant_id}:{collection}:{field_name}")
}
