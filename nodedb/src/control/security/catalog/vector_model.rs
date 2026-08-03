// SPDX-License-Identifier: BUSL-1.1

//! Catalog operations for vector model metadata.
//!
//! Stores per-column embedding model information in `_system.vector_model_metadata`.
//! Key format: `"{tenant_id}:{collection}:{column}"`.

use nodedb_types::VectorModelEntry;
use redb::ReadableDatabase;

use super::types::{SystemCatalog, VECTOR_MODEL_METADATA, catalog_err};

impl SystemCatalog {
    /// Store vector model metadata for a collection/column.
    pub fn put_vector_model(&self, entry: &VectorModelEntry) -> crate::Result<()> {
        let key = vector_model_key(entry.tenant_id, &entry.collection, &entry.column);
        let bytes =
            zerompk::to_msgpack_vec(entry).map_err(|e| catalog_err("serialize vector model", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(VECTOR_MODEL_METADATA)
                .map_err(|e| catalog_err("open vector_model_metadata", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert vector model", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Load vector model metadata for a specific collection/column.
    pub fn get_vector_model(
        &self,
        tenant_id: u64,
        collection: &str,
        column: &str,
    ) -> crate::Result<Option<VectorModelEntry>> {
        let key = vector_model_key(tenant_id, collection, column);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(VECTOR_MODEL_METADATA)
            .map_err(|e| catalog_err("open vector_model_metadata", e))?;

        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let entry: VectorModelEntry = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser vector model", e))?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get vector model", e)),
        }
    }

    /// List all vector model metadata entries for a tenant.
    pub fn list_vector_models(&self, tenant_id: u64) -> crate::Result<Vec<VectorModelEntry>> {
        let prefix = format!("{tenant_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(VECTOR_MODEL_METADATA)
            .map_err(|e| catalog_err("open vector_model_metadata", e))?;

        let mut entries = Vec::new();
        for item in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range vector models", e))?
        {
            let (key, value) = item.map_err(|e| catalog_err("read vector model", e))?;
            if key.value().starts_with(&prefix) {
                let entry: VectorModelEntry = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser vector model", e))?;
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// List all vector model metadata entries across all tenants.
    pub fn list_all_vector_models(&self) -> crate::Result<Vec<VectorModelEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(VECTOR_MODEL_METADATA)
            .map_err(|e| catalog_err("open vector_model_metadata", e))?;

        let mut entries = Vec::new();
        for item in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range vector models", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read vector model", e))?;
            let entry: VectorModelEntry = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser vector model", e))?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Delete vector model metadata for a specific collection/column.
    pub fn delete_vector_model(
        &self,
        tenant_id: u64,
        collection: &str,
        column: &str,
    ) -> crate::Result<bool> {
        let key = vector_model_key(tenant_id, collection, column);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(VECTOR_MODEL_METADATA)
                .map_err(|e| catalog_err("open vector_model_metadata", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove vector model", e))?
                .is_some()
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(removed)
    }
}

fn vector_model_key(tenant_id: u64, collection: &str, column: &str) -> String {
    format!("{tenant_id}:{collection}:{column}")
}
