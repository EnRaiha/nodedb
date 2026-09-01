// SPDX-License-Identifier: BUSL-1.1

//! Apply vector model metadata and vector-index parameter entries to
//! `SystemCatalog` redb.
//!
//! Writes only. The leader resolves the duplicate index, the missing
//! collection, and every build-parameter rule before proposing, so apply runs
//! the unvalidated catalog path: a rejection here would diverge a follower
//! from a statement the leader already accepted.
//!
//! Both tables describe the same object — a collection's embedding column and
//! the index built over it — so they share one apply module.

use nodedb_types::{StoredVectorIndexParams, VectorModelEntry};

use crate::control::security::catalog::{SystemCatalog, catalog_err};

/// Apply a `PutVectorModel` entry. A re-delivery rewrites the same row.
pub fn put_model(entry: &VectorModelEntry, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_vector_model(entry).map_err(|e| {
        catalog_err(
            &format!("put_vector_model '{}.{}'", entry.collection, entry.column),
            e,
        )
    })
}

/// Apply a `PutVectorIndexParams` entry. A re-delivery rewrites the same row.
pub fn put_params(entry: &StoredVectorIndexParams, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_vector_index_params(entry).map_err(|e| {
        catalog_err(
            &format!(
                "put_vector_index_params '{}.{}'",
                entry.collection, entry.field_name
            ),
            e,
        )
    })
}

/// Apply a `DeleteVectorIndexParams` entry.
///
/// A missing row is not an error: the entry is idempotent under replay.
pub fn delete_params(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    field_name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_vector_index_params(database_id, tenant_id, collection, field_name)
        .map_err(|e| {
            catalog_err(
                &format!("delete_vector_index_params '{collection}.{field_name}'"),
                e,
            )
        })
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::catalog_entry::{apply, decode, encode};
    use nodedb_types::VectorModelMetadata;

    const DATABASE: u64 = 3;
    const TENANT: u64 = 7;
    const COLLECTION: &str = "documents";
    const FIELD: &str = "embedding";

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn model() -> VectorModelEntry {
        VectorModelEntry {
            tenant_id: TENANT,
            collection: COLLECTION.to_string(),
            column: FIELD.to_string(),
            metadata: VectorModelMetadata {
                model: "all-MiniLM-L6-v2".to_string(),
                dimensions: 384,
                created_at: "2026-01-01".to_string(),
                strict_dimensions: true,
            },
        }
    }

    fn params() -> StoredVectorIndexParams {
        StoredVectorIndexParams {
            database_id: DATABASE,
            tenant_id: TENANT,
            collection: COLLECTION.to_string(),
            field_name: FIELD.to_string(),
            dim: 384,
            metric: "cosine".to_string(),
            m: 32,
            ef_construction: 400,
            index_type: "hnsw".to_string(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
        }
    }

    #[test]
    fn put_vector_model_roundtrips_through_codec() {
        let entry = CatalogEntry::PutVectorModel(Box::new(model()));
        match decode(&encode(&entry).unwrap()).unwrap() {
            CatalogEntry::PutVectorModel(record) => {
                assert_eq!(record.tenant_id, TENANT);
                assert_eq!(record.collection, COLLECTION);
                assert_eq!(record.column, FIELD);
                assert_eq!(record.metadata.model, "all-MiniLM-L6-v2");
                assert_eq!(record.metadata.dimensions, 384);
                assert_eq!(record.metadata.created_at, "2026-01-01");
                assert!(record.metadata.strict_dimensions);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn put_vector_index_params_roundtrips_through_codec() {
        let entry = CatalogEntry::PutVectorIndexParams(Box::new(params()));
        match decode(&encode(&entry).unwrap()).unwrap() {
            CatalogEntry::PutVectorIndexParams(record) => {
                assert_eq!(record.database_id, DATABASE);
                assert_eq!(record.tenant_id, TENANT);
                assert_eq!(record.collection, COLLECTION);
                assert_eq!(record.field_name, FIELD);
                assert_eq!(record.dim, 384);
                assert_eq!(record.metric, "cosine");
                assert_eq!(record.m, 32);
                assert_eq!(record.ef_construction, 400);
                assert_eq!(record.index_type, "hnsw");
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_vector_index_params_roundtrips_through_codec() {
        let entry = CatalogEntry::DeleteVectorIndexParams {
            database_id: DATABASE,
            tenant_id: TENANT,
            collection: COLLECTION.to_string(),
            field_name: FIELD.to_string(),
        };
        match decode(&encode(&entry).unwrap()).unwrap() {
            CatalogEntry::DeleteVectorIndexParams {
                database_id,
                tenant_id,
                collection,
                field_name,
            } => {
                assert_eq!(database_id, DATABASE);
                assert_eq!(tenant_id, TENANT);
                assert_eq!(collection, COLLECTION);
                assert_eq!(field_name, FIELD);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn apply_writes_the_vector_model_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(&CatalogEntry::PutVectorModel(Box::new(model())), &catalog).unwrap();

        let stored = catalog
            .get_vector_model(TENANT, COLLECTION, FIELD)
            .unwrap()
            .expect("apply writes the vector model row");
        assert_eq!(stored.metadata.model, "all-MiniLM-L6-v2");
        assert_eq!(stored.metadata.dimensions, 384);
    }

    #[test]
    fn apply_writes_and_removes_the_vector_index_params_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(
            &CatalogEntry::PutVectorIndexParams(Box::new(params())),
            &catalog,
        )
        .unwrap();
        assert!(
            catalog
                .get_vector_index_params(DATABASE, TENANT, COLLECTION, FIELD)
                .unwrap()
                .is_some()
        );

        apply::apply_to(
            &CatalogEntry::DeleteVectorIndexParams {
                database_id: DATABASE,
                tenant_id: TENANT,
                collection: COLLECTION.to_string(),
                field_name: FIELD.to_string(),
            },
            &catalog,
        )
        .unwrap();
        assert!(
            catalog
                .get_vector_index_params(DATABASE, TENANT, COLLECTION, FIELD)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn deleting_absent_vector_index_params_is_a_noop() {
        let (_dir, catalog) = open_catalog();
        delete_params(DATABASE, TENANT, COLLECTION, "never-created", &catalog)
            .expect("delete absent");
    }

    #[test]
    fn params_of_one_database_survive_a_delete_in_another() {
        let (_dir, catalog) = open_catalog();
        put_params(&params(), &catalog).unwrap();
        let mut other = params();
        other.database_id = DATABASE + 1;
        put_params(&other, &catalog).unwrap();

        delete_params(DATABASE, TENANT, COLLECTION, FIELD, &catalog).expect("delete");

        assert!(
            catalog
                .get_vector_index_params(DATABASE + 1, TENANT, COLLECTION, FIELD)
                .unwrap()
                .is_some(),
            "the key is scoped by database, so the sibling row stays"
        );
    }
}
