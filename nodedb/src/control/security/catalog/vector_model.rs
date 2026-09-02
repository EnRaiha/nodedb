// SPDX-License-Identifier: BUSL-1.1

//! Catalog operations for vector model metadata.
//!
//! Stores per-column embedding model information in `_system.vector_model_metadata`.
//! Key format: `"{database_id}:{tenant_id}:{collection}:{column}"`.
//!
//! The database segment scopes the row: two databases in one tenant can hold a
//! same-named collection, and a shared key lets one inherit the other's
//! dimension enforcement.

use nodedb_types::VectorModelEntry;
use redb::{ReadableDatabase, ReadableTable};

use super::types::{SystemCatalog, VECTOR_MODEL_METADATA, catalog_err};

impl SystemCatalog {
    /// Store vector model metadata for a collection/column.
    ///
    /// The key comes from the entry, so the row can never land under a
    /// database the entry does not name.
    pub fn put_vector_model(&self, entry: &VectorModelEntry) -> crate::Result<()> {
        let key = vector_model_key(
            entry.database_id,
            entry.tenant_id,
            &entry.collection,
            &entry.column,
        );
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
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        column: &str,
    ) -> crate::Result<Option<VectorModelEntry>> {
        let key = vector_model_key(database_id, tenant_id, collection, column);
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

    /// List every vector model row of one tenant in one database.
    ///
    /// The scan is bounded to the tenant's key range, so a node holding many
    /// tenants reads only the rows it returns.
    pub fn list_vector_models(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<VectorModelEntry>> {
        let lower = format!("{database_id}:{tenant_id}:");
        let upper = tenant_upper_bound(database_id, tenant_id);
        self.range_vector_models(&lower, &upper)
    }

    /// List every vector model row of one database, across every tenant.
    ///
    /// The scan is bounded to the database's key range.
    pub fn list_vector_models_in_database(
        &self,
        database_id: u64,
    ) -> crate::Result<Vec<VectorModelEntry>> {
        let lower = format!("{database_id}:");
        let upper = database_upper_bound(database_id);
        self.range_vector_models(&lower, &upper)
    }

    /// List every vector model row of one collection in one database.
    pub fn list_vector_models_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<VectorModelEntry>> {
        let lower = format!("{database_id}:{tenant_id}:{collection}:");
        let upper = collection_upper_bound(database_id, tenant_id, collection);
        self.range_vector_models(&lower, &upper)
    }

    /// Delete vector model metadata for a specific collection/column.
    pub fn delete_vector_model(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        column: &str,
    ) -> crate::Result<bool> {
        let key = vector_model_key(database_id, tenant_id, collection, column);
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

    /// Delete every vector model row of one collection, returning the count.
    ///
    /// A model row is set per column and needs no index, so a collection can
    /// carry rows for columns no index covers. Purging by index field alone
    /// leaves those behind for the next same-named collection to inherit.
    pub fn delete_vector_models_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<usize> {
        let lower = format!("{database_id}:{tenant_id}:{collection}:");
        let upper = collection_upper_bound(database_id, tenant_id, collection);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(VECTOR_MODEL_METADATA)
                .map_err(|e| catalog_err("open vector_model_metadata", e))?;
            let keys: Vec<String> = table
                .range(lower.as_str()..upper.as_str())
                .map_err(|e| catalog_err("range vector models", e))?
                .map(|item| {
                    item.map(|(key, _)| key.value().to_string())
                        .map_err(|e| catalog_err("read vector model", e))
                })
                .collect::<crate::Result<Vec<String>>>()?;
            for key in &keys {
                table
                    .remove(key.as_str())
                    .map_err(|e| catalog_err("remove vector model", e))?;
            }
            keys.len()
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(removed)
    }

    /// Decode every row in one key range.
    fn range_vector_models(
        &self,
        lower: &str,
        upper: &str,
    ) -> crate::Result<Vec<VectorModelEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(VECTOR_MODEL_METADATA)
            .map_err(|e| catalog_err("open vector_model_metadata", e))?;

        let mut entries = Vec::new();
        for item in table
            .range(lower..upper)
            .map_err(|e| catalog_err("range vector models", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read vector model", e))?;
            let entry: VectorModelEntry = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser vector model", e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

fn vector_model_key(database_id: u64, tenant_id: u64, collection: &str, column: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}:{column}")
}

/// Exclusive upper bound for one database's key prefix.
///
/// The prefix ends with `:`. The next byte after `:` is `;`, so this key sorts
/// immediately past every tenant of the database.
fn database_upper_bound(database_id: u64) -> String {
    format!("{database_id};")
}

/// Exclusive upper bound for one tenant's key prefix.
///
/// The prefix ends with `:`. The next byte after `:` is `;`, so this key sorts
/// immediately past every collection of the tenant.
fn tenant_upper_bound(database_id: u64, tenant_id: u64) -> String {
    format!("{database_id}:{tenant_id};")
}

/// Exclusive upper bound for one collection's key prefix.
fn collection_upper_bound(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection};")
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::VectorModelMetadata;

    fn make_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn entry(database_id: u64, collection: &str, column: &str) -> VectorModelEntry {
        VectorModelEntry {
            database_id,
            tenant_id: 1,
            collection: collection.into(),
            column: column.into(),
            metadata: VectorModelMetadata {
                model: "all-MiniLM-L6-v2".into(),
                dimensions: 384,
                created_at: "2026-01-01".into(),
                strict_dimensions: true,
            },
        }
    }

    #[test]
    fn put_and_get_roundtrip() {
        let (_dir, cat) = make_catalog();
        cat.put_vector_model(&entry(2, "chunks", "embedding"))
            .unwrap();

        let stored = cat.get_vector_model(2, 1, "chunks", "embedding").unwrap();
        assert_eq!(stored.unwrap().metadata.dimensions, 384);
    }

    #[test]
    fn a_row_of_one_database_is_invisible_to_another() {
        let (_dir, cat) = make_catalog();
        cat.put_vector_model(&entry(2, "chunks", "embedding"))
            .unwrap();

        assert!(
            cat.get_vector_model(3, 1, "chunks", "embedding")
                .unwrap()
                .is_none(),
            "the key is scoped by database"
        );
    }

    #[test]
    fn listing_a_tenant_excludes_another_database() {
        let (_dir, cat) = make_catalog();
        cat.put_vector_model(&entry(2, "chunks", "embedding"))
            .unwrap();
        cat.put_vector_model(&entry(3, "chunks", "embedding"))
            .unwrap();

        assert_eq!(cat.list_vector_models(2, 1).unwrap().len(), 1);
        assert_eq!(cat.list_vector_models(3, 1).unwrap().len(), 1);
    }

    #[test]
    fn listing_a_collection_excludes_a_name_prefix_sibling() {
        let (_dir, cat) = make_catalog();
        cat.put_vector_model(&entry(2, "chunks", "embedding"))
            .unwrap();
        cat.put_vector_model(&entry(2, "chunks_archive", "embedding"))
            .unwrap();

        let listed = cat
            .list_vector_models_for_collection(2, 1, "chunks")
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].collection, "chunks");
    }

    #[test]
    fn deleting_a_collection_removes_every_column_row() {
        let (_dir, cat) = make_catalog();
        cat.put_vector_model(&entry(2, "chunks", "embedding"))
            .unwrap();
        cat.put_vector_model(&entry(2, "chunks", "image_embedding"))
            .unwrap();
        cat.put_vector_model(&entry(2, "chunks_archive", "embedding"))
            .unwrap();

        assert_eq!(
            cat.delete_vector_models_for_collection(2, 1, "chunks")
                .unwrap(),
            2
        );
        assert!(
            cat.list_vector_models_for_collection(2, 1, "chunks")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            cat.list_vector_models_for_collection(2, 1, "chunks_archive")
                .unwrap()
                .len(),
            1,
            "the prefix sibling keeps its row"
        );
    }

    #[test]
    fn deleting_one_column_leaves_its_siblings() {
        let (_dir, cat) = make_catalog();
        cat.put_vector_model(&entry(2, "chunks", "embedding"))
            .unwrap();
        cat.put_vector_model(&entry(2, "chunks", "image_embedding"))
            .unwrap();

        assert!(
            cat.delete_vector_model(2, 1, "chunks", "embedding")
                .unwrap()
        );
        assert!(
            !cat.delete_vector_model(2, 1, "chunks", "embedding")
                .unwrap()
        );
        assert_eq!(
            cat.list_vector_models_for_collection(2, 1, "chunks")
                .unwrap()
                .len(),
            1
        );
    }
}
