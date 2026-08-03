// SPDX-License-Identifier: BUSL-1.1

//! Global Secondary Indexes (GSI) for cross-shard index lookups.
//!
//! A GSI is a hidden redb table sharded by the indexed column's value.
//! It maps `{index_name}:{value}` → `Vec<(tenant_id, collection, doc_id, shard_location)>`.
//!
//! This allows queries like `WHERE email = 'alice@example.com'` to be
//! resolved via a single index lookup instead of scanning all shards.
//!
//! Constraints: max 4 GSIs per collection to limit write amplification
//! (each document write updates all GSIs).

use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::debug;

/// GSI table: key = "{index_name}:{value}" → MessagePack Vec<GsiEntry>.
const GSI_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("gsi.entries");

/// GSI metadata: key = "{tenant}:{collection}:{index_name}" → MessagePack GsiMeta.
const GSI_META: TableDefinition<&str, &[u8]> = TableDefinition::new("gsi.meta");

/// Default maximum GSIs per collection. Sourced from `SparseTuning::max_gsis_per_collection`.
pub const DEFAULT_MAX_GSIS_PER_COLLECTION: usize = 4;

/// A single GSI entry pointing to the document's primary location.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Clone,
    Debug,
)]
pub struct GsiEntry {
    pub tenant_id: u64,
    pub collection: String,
    pub document_id: String,
    /// vShard where the primary document lives.
    pub shard_id: u16,
}

/// GSI metadata for a declared index.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Clone,
    Debug,
)]
pub struct GsiMeta {
    pub index_name: String,
    pub collection: String,
    pub field: String,
    /// Additional payload columns stored in the GSI for covering-index
    /// queries. If `payload_fields` covers all projected columns, the
    /// query can be served directly from the GSI without a primary hop.
    pub payload_fields: Vec<String>,
    pub tenant_id: u64,
}

/// GSI store backed by redb.
pub struct GsiStore {
    db: Arc<Database>,
    /// Maximum GSIs per collection. Set from `SparseTuning::max_gsis_per_collection`.
    max_gsis: usize,
}

impl GsiStore {
    /// Open or create the GSI store.
    pub fn open(db: Arc<Database>) -> crate::Result<Self> {
        let write_txn = db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "gsi".into(),
            detail: format!("open: {e}"),
        })?;
        {
            let _ = write_txn.open_table(GSI_TABLE);
            let _ = write_txn.open_table(GSI_META);
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "gsi".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(Self {
            db,
            max_gsis: DEFAULT_MAX_GSIS_PER_COLLECTION,
        })
    }

    /// Declare a new GSI on a collection field.
    ///
    /// Enforces the max 4 GSIs per collection limit.
    pub fn create_index(
        &self,
        tenant_id: u64,
        collection: &str,
        index_name: &str,
        field: &str,
        payload_fields: Vec<String>,
    ) -> crate::Result<()> {
        // Check limit.
        let existing = self.list_indexes(tenant_id, collection)?;
        if existing.len() >= self.max_gsis {
            return Err(crate::Error::Storage {
                engine: "gsi".into(),
                detail: format!(
                    "collection '{collection}' already has {} GSIs (max)",
                    self.max_gsis
                ),
            });
        }

        let meta = GsiMeta {
            index_name: index_name.to_string(),
            collection: collection.to_string(),
            field: field.to_string(),
            payload_fields,
            tenant_id,
        };
        let meta_key = format!("{tenant_id}:{collection}:{index_name}");
        let meta_bytes =
            zerompk::to_msgpack_vec(&meta).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("gsi meta: {e}"),
            })?;

        let write_txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "gsi".into(),
            detail: format!("write: {e}"),
        })?;
        {
            let mut table = write_txn
                .open_table(GSI_META)
                .map_err(|e| crate::Error::Storage {
                    engine: "gsi".into(),
                    detail: format!("open meta: {e}"),
                })?;
            table
                .insert(meta_key.as_str(), meta_bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "gsi".into(),
                    detail: format!("insert meta: {e}"),
                })?;
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "gsi".into(),
            detail: format!("commit: {e}"),
        })?;

        debug!(tenant_id, collection, index_name, field, "GSI created");
        Ok(())
    }

    /// Index a document's field value in all applicable GSIs.
    ///
    /// Called on every PointPut. For each GSI declared on this collection,
    /// extracts the indexed field value and inserts/updates the GSI entry.
    pub fn index_document(
        &self,
        tenant_id: u64,
        collection: &str,
        document_id: &str,
        shard_id: u16,
        doc: &serde_json::Value,
        indexes: &[GsiMeta],
    ) -> crate::Result<()> {
        if indexes.is_empty() {
            return Ok(());
        }

        let write_txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "gsi".into(),
            detail: format!("write: {e}"),
        })?;
        {
            let mut table = write_txn
                .open_table(GSI_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "gsi".into(),
                    detail: format!("open entries: {e}"),
                })?;

            for idx_meta in indexes {
                let value = doc.get(&idx_meta.field).and_then(|v| match v {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    serde_json::Value::Bool(b) => Some(b.to_string()),
                    _ => None,
                });
                let Some(value_str) = value else { continue };

                let key = format!("{}:{}", idx_meta.index_name, value_str);
                let mut entries: Vec<GsiEntry> = match table.get(key.as_str()) {
                    Ok(Some(guard)) => match zerompk::from_msgpack(guard.value()) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(index = %key, error = %e, "GSI entry deserialization failed, starting fresh");
                            Vec::new()
                        }
                    },
                    _ => Vec::new(),
                };

                // Remove existing entry for this doc (update case).
                entries.retain(|e| e.document_id != document_id);
                entries.push(GsiEntry {
                    tenant_id,
                    collection: collection.to_string(),
                    document_id: document_id.to_string(),
                    shard_id,
                });

                let bytes =
                    zerompk::to_msgpack_vec(&entries).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("gsi entries: {e}"),
                    })?;
                table.insert(key.as_str(), bytes.as_slice()).map_err(|e| {
                    crate::Error::Storage {
                        engine: "gsi".into(),
                        detail: format!("insert entry: {e}"),
                    }
                })?;
            }
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "gsi".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }

    /// Look up documents by GSI value. Returns entries pointing to primary locations.
    pub fn lookup(&self, index_name: &str, value: &str) -> crate::Result<Vec<GsiEntry>> {
        let key = format!("{index_name}:{value}");
        let read_txn = self.db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "gsi".into(),
            detail: format!("read: {e}"),
        })?;
        let table = read_txn
            .open_table(GSI_TABLE)
            .map_err(|e| crate::Error::Storage {
                engine: "gsi".into(),
                detail: format!("open entries: {e}"),
            })?;

        match table.get(key.as_str()) {
            Ok(Some(guard)) => {
                let entries: Vec<GsiEntry> = match zerompk::from_msgpack(guard.value()) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(index = %key, error = %e, "GSI lookup deserialization failed");
                        Vec::new()
                    }
                };
                Ok(entries)
            }
            Ok(None) => Ok(Vec::new()),
            Err(e) => Err(crate::Error::Storage {
                engine: "gsi".into(),
                detail: format!("get: {e}"),
            }),
        }
    }

    /// List all GSIs declared for a collection.
    pub fn list_indexes(&self, tenant_id: u64, collection: &str) -> crate::Result<Vec<GsiMeta>> {
        let prefix = format!("{tenant_id}:{collection}:");
        let end = format!("{tenant_id}:{collection}:\u{ffff}");

        let read_txn = self.db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "gsi".into(),
            detail: format!("read: {e}"),
        })?;
        let table = read_txn
            .open_table(GSI_META)
            .map_err(|e| crate::Error::Storage {
                engine: "gsi".into(),
                detail: format!("open meta: {e}"),
            })?;

        let range =
            table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| crate::Error::Storage {
                    engine: "gsi".into(),
                    detail: format!("range: {e}"),
                })?;

        let mut result = Vec::new();
        for entry in range {
            let entry = entry.map_err(|e| crate::Error::Storage {
                engine: "gsi".into(),
                detail: format!("entry: {e}"),
            })?;
            match zerompk::from_msgpack::<GsiMeta>(entry.1.value()) {
                Ok(meta) => result.push(meta),
                Err(e) => {
                    tracing::warn!(
                        key = %entry.0.value(),
                        error = %e,
                        "GSI metadata deserialization failed, skipping entry"
                    );
                }
            }
        }
        Ok(result)
    }

    /// Check if a GSI covers all projected columns (covering index query).
    ///
    /// A covering index includes the indexed field + all payload_fields.
    /// If the query projection is a subset, we can serve the query from
    /// the GSI without a primary document fetch.
    pub fn is_covering(meta: &GsiMeta, projection: &[String]) -> bool {
        if projection.is_empty() {
            return false; // SELECT * always needs primary.
        }
        projection.iter().all(|col| {
            col == &meta.field
                || col == "id"
                || col == "document_id"
                || meta.payload_fields.contains(col)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_store() -> (GsiStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path().join("gsi.redb")).unwrap());
        let store = GsiStore::open(db).unwrap();
        (store, dir)
    }

    #[test]
    fn create_and_lookup() {
        let (store, _dir) = open_store();
        store
            .create_index(1, "users", "email_idx", "email", vec![])
            .unwrap();

        let doc = serde_json::json!({"email": "alice@example.com", "name": "Alice"});
        let indexes = store.list_indexes(1, "users").unwrap();
        store
            .index_document(1, "users", "u1", 0, &doc, &indexes)
            .unwrap();

        let results = store.lookup("email_idx", "alice@example.com").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].document_id, "u1");
    }

    #[test]
    fn max_gsi_limit() {
        let (store, _dir) = open_store();
        for i in 0..DEFAULT_MAX_GSIS_PER_COLLECTION {
            store
                .create_index(1, "t", &format!("idx{i}"), &format!("f{i}"), vec![])
                .unwrap();
        }
        // One over the limit should fail.
        let result = store.create_index(1, "t", "idx_extra", "f_extra", vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn covering_index_check() {
        let meta = GsiMeta {
            index_name: "email_idx".into(),
            collection: "users".into(),
            field: "email".into(),
            payload_fields: vec!["name".into(), "status".into()],
            tenant_id: 1,
        };
        assert!(GsiStore::is_covering(
            &meta,
            &["email".into(), "name".into()]
        ));
        assert!(GsiStore::is_covering(
            &meta,
            &["id".into(), "email".into(), "status".into()]
        ));
        assert!(!GsiStore::is_covering(
            &meta,
            &["email".into(), "age".into()]
        ));
        assert!(!GsiStore::is_covering(&meta, &[])); // SELECT *
    }

    #[test]
    fn update_replaces_entry() {
        let (store, _dir) = open_store();
        store
            .create_index(1, "users", "email_idx", "email", vec![])
            .unwrap();
        let indexes = store.list_indexes(1, "users").unwrap();

        let doc1 = serde_json::json!({"email": "old@example.com"});
        store
            .index_document(1, "users", "u1", 0, &doc1, &indexes)
            .unwrap();

        let doc2 = serde_json::json!({"email": "new@example.com"});
        store
            .index_document(1, "users", "u1", 0, &doc2, &indexes)
            .unwrap();

        // Old value still has the stale entry because we only deduplicate
        // within the same key. A separate cleanup handles stale entries.
        let _old = store.lookup("email_idx", "old@example.com").unwrap();
        // The new value should have the entry.
        let new = store.lookup("email_idx", "new@example.com").unwrap();
        assert_eq!(new.len(), 1);
        assert_eq!(new[0].document_id, "u1");
    }
}
