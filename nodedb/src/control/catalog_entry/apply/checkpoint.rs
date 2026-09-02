// SPDX-License-Identifier: BUSL-1.1

//! Apply version-history checkpoint entries to `SystemCatalog` redb.
//!
//! Writes only. The leader reports the duplicate and the missing checkpoint
//! before proposing, so apply runs the unvalidated catalog path: a rejection
//! here would diverge a follower from a statement the leader already accepted.

use crate::control::security::catalog::types::CheckpointRecord;
use crate::control::security::catalog::{SystemCatalog, catalog_err};

/// Apply a `PutCheckpoint` entry. A re-delivery rewrites the same row.
pub fn put(record: &CheckpointRecord, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .put_checkpoint(record)
        .map_err(|e| catalog_err(&format!("put_checkpoint '{}'", record.checkpoint_name), e))
}

/// Apply a `DeleteCheckpoint` entry.
///
/// A missing row is not an error: the entry is idempotent under replay.
pub fn delete(
    tenant_id: u64,
    collection: &str,
    doc_id: &str,
    checkpoint_name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_checkpoint(tenant_id, collection, doc_id, checkpoint_name)
        .map_err(|e| catalog_err(&format!("delete_checkpoint '{checkpoint_name}'"), e))
        .map(|_| ())
}

/// Apply the range delete `CompactHistory` carries.
///
/// The boundary is exclusive, so a checkpoint stamped exactly at
/// `before_timestamp` survives on every node.
pub fn delete_before(
    tenant_id: u64,
    collection: &str,
    doc_id: &str,
    before_timestamp: u64,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_checkpoints_before(tenant_id, collection, doc_id, before_timestamp)
        .map_err(|e| {
            catalog_err(
                &format!("delete_checkpoints_before '{collection}/{doc_id}'"),
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

    const TENANT: u64 = 7;
    const DATABASE: u64 = 3;
    const COLLECTION: &str = "documents";
    const DOC: &str = "doc-1";
    const NAME: &str = "launch-ready";

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn sample(name: &str, created_at: u64) -> CheckpointRecord {
        CheckpointRecord {
            tenant_id: TENANT,
            collection: COLLECTION.to_string(),
            doc_id: DOC.to_string(),
            checkpoint_name: name.to_string(),
            version_vector_json: "{\"n1\":4}".to_string(),
            created_by: "admin".to_string(),
            created_at,
        }
    }

    fn names(catalog: &SystemCatalog) -> Vec<String> {
        catalog
            .list_checkpoints(TENANT, COLLECTION, DOC, 0)
            .unwrap()
            .into_iter()
            .map(|r| r.checkpoint_name)
            .collect()
    }

    #[test]
    fn put_checkpoint_roundtrips_through_codec() {
        let entry = CatalogEntry::PutCheckpoint(Box::new(sample(NAME, 1_000)));
        match decode(&encode(&entry).unwrap()).unwrap() {
            CatalogEntry::PutCheckpoint(record) => {
                assert_eq!(record.tenant_id, TENANT);
                assert_eq!(record.collection, COLLECTION);
                assert_eq!(record.doc_id, DOC);
                assert_eq!(record.checkpoint_name, NAME);
                assert_eq!(record.version_vector_json, "{\"n1\":4}");
                assert_eq!(record.created_by, "admin");
                assert_eq!(record.created_at, 1_000);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_checkpoint_roundtrips_through_codec() {
        let entry = CatalogEntry::DeleteCheckpoint {
            tenant_id: TENANT,
            collection: COLLECTION.to_string(),
            doc_id: DOC.to_string(),
            checkpoint_name: NAME.to_string(),
        };
        match decode(&encode(&entry).unwrap()).unwrap() {
            CatalogEntry::DeleteCheckpoint {
                tenant_id,
                collection,
                doc_id,
                checkpoint_name,
            } => {
                assert_eq!(tenant_id, TENANT);
                assert_eq!(collection, COLLECTION);
                assert_eq!(doc_id, DOC);
                assert_eq!(checkpoint_name, NAME);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    /// The entry carries the compaction target as well as the boundary. A
    /// target lost in the codec leaves every follower unable to compact.
    #[test]
    fn compact_history_roundtrips_through_codec() {
        let entry = CatalogEntry::CompactHistory {
            tenant_id: TENANT,
            database_id: DATABASE,
            collection: COLLECTION.to_string(),
            doc_id: DOC.to_string(),
            before_timestamp: 500,
            target_version_json: "{\"n1\":4}".to_string(),
        };
        match decode(&encode(&entry).unwrap()).unwrap() {
            CatalogEntry::CompactHistory {
                tenant_id,
                database_id,
                collection,
                doc_id,
                before_timestamp,
                target_version_json,
            } => {
                assert_eq!(tenant_id, TENANT);
                assert_eq!(database_id, DATABASE);
                assert_eq!(collection, COLLECTION);
                assert_eq!(doc_id, DOC);
                assert_eq!(before_timestamp, 500);
                assert_eq!(target_version_json, "{\"n1\":4}");
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn apply_writes_and_removes_the_checkpoint_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(
            &CatalogEntry::PutCheckpoint(Box::new(sample(NAME, 1_000))),
            &catalog,
        )
        .unwrap();
        assert_eq!(names(&catalog), vec![NAME.to_string()]);

        apply::apply_to(
            &CatalogEntry::DeleteCheckpoint {
                tenant_id: TENANT,
                collection: COLLECTION.to_string(),
                doc_id: DOC.to_string(),
                checkpoint_name: NAME.to_string(),
            },
            &catalog,
        )
        .unwrap();
        assert!(names(&catalog).is_empty());
    }

    #[test]
    fn deleting_an_absent_checkpoint_is_a_noop() {
        let (_dir, catalog) = open_catalog();
        delete(TENANT, COLLECTION, DOC, "never-created", &catalog).expect("delete absent");
    }

    #[test]
    fn range_delete_boundary_is_exclusive() {
        let (_dir, catalog) = open_catalog();
        put(&sample("older", 99), &catalog).unwrap();
        put(&sample("boundary", 100), &catalog).unwrap();
        put(&sample("newer", 101), &catalog).unwrap();

        apply::apply_to(
            &CatalogEntry::CompactHistory {
                tenant_id: TENANT,
                database_id: DATABASE,
                collection: COLLECTION.to_string(),
                doc_id: DOC.to_string(),
                before_timestamp: 100,
                target_version_json: "{\"n1\":4}".to_string(),
            },
            &catalog,
        )
        .unwrap();

        let mut remaining = names(&catalog);
        remaining.sort();
        assert_eq!(
            remaining,
            vec!["boundary".to_string(), "newer".to_string()],
            "created_at == before_timestamp survives the range delete"
        );
    }

    #[test]
    fn range_delete_leaves_other_documents_alone() {
        let (_dir, catalog) = open_catalog();
        put(&sample("mine", 10), &catalog).unwrap();
        let mut other = sample("theirs", 10);
        other.doc_id = "doc-2".to_string();
        put(&other, &catalog).unwrap();

        delete_before(TENANT, COLLECTION, DOC, 1_000, &catalog).expect("range delete");

        assert!(names(&catalog).is_empty());
        assert_eq!(
            catalog
                .list_checkpoints(TENANT, COLLECTION, "doc-2", 0)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn count_before_matches_the_range_delete() {
        let (_dir, catalog) = open_catalog();
        put(&sample("a", 1), &catalog).unwrap();
        put(&sample("b", 2), &catalog).unwrap();
        put(&sample("c", 9), &catalog).unwrap();

        let counted = catalog
            .count_checkpoints_before(TENANT, COLLECTION, DOC, 9)
            .expect("count");
        let deleted = catalog
            .delete_checkpoints_before(TENANT, COLLECTION, DOC, 9)
            .expect("delete");
        assert_eq!(counted, 2);
        assert_eq!(deleted, counted);
    }
}
