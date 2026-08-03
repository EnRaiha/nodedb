// SPDX-License-Identifier: BUSL-1.1

use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use tracing::{debug, info};

/// Table definition for the primary document store.
/// Key: "{database_id}:{tenant_id}:{collection}:{document_id}" → Value: document bytes.
pub(crate) const DOCUMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");

/// Table definition for secondary indexes.
/// Key: "{database_id}:{tenant_id}:{collection}:{field}:{value}:{document_id}" → Value: empty (existence index).
pub(super) const INDEXES: TableDefinition<&str, &[u8]> = TableDefinition::new("indexes");

/// Map a redb error into our crate error with context.
pub(super) fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// The `"{database_id}:{tenant_id}:{collection}:"` prefix shared by every
/// document / secondary-index key in the current (non-versioned) tables.
/// Centralised so encode, prefix-scan, range-bound, and rename sites can't
/// drift apart. The trailing `:` makes it a clean lower bound for a
/// prefix scan; the matching upper bound appends `\u{ffff}`.
pub(crate) fn coll_prefix(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}:")
}

/// Lower bound for a `(database_id, tenant_id)` whole-tenant prefix scan.
pub(super) fn tenant_prefix(database_id: u64, tenant_id: u64) -> String {
    format!("{database_id}:{tenant_id}:")
}

std::thread_local! {
    static KEY_BUF: std::cell::RefCell<String> = std::cell::RefCell::new(String::with_capacity(256));
}

/// Build a database/tenant-scoped composite key `"{db}:{tenant}:{a}:{b}"`
/// using a thread-local buffer.
fn with_tenant_key<R>(
    database_id: u64,
    tenant_id: u64,
    a: &str,
    b: &str,
    f: impl FnOnce(&str) -> R,
) -> R {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        use std::fmt::Write;
        let _ = write!(buf, "{database_id}");
        buf.push(':');
        let _ = write!(buf, "{tenant_id}");
        buf.push(':');
        buf.push_str(a);
        buf.push(':');
        buf.push_str(b);
        f(&buf)
    })
}

/// Build a database/tenant-scoped index key `"{db}:{tenant}:{a}:{b}:{c}:{d}"`.
pub(super) fn with_tenant_key4<R>(
    database_id: u64,
    tenant_id: u64,
    a: &str,
    b: &str,
    c: &str,
    d: &str,
    f: impl FnOnce(&str) -> R,
) -> R {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        use std::fmt::Write;
        let _ = write!(buf, "{database_id}");
        buf.push(':');
        let _ = write!(buf, "{tenant_id}");
        buf.push(':');
        buf.push_str(a);
        buf.push(':');
        buf.push_str(b);
        buf.push(':');
        buf.push_str(c);
        buf.push(':');
        buf.push_str(d);
        f(&buf)
    })
}

/// redb-backed B-Tree storage engine for sparse/metadata queries.
pub struct SparseEngine {
    pub(super) db: Arc<Database>,
}

impl SparseEngine {
    /// Open or create the sparse engine database at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| redb_err("open", e))?;

        let write_txn = db.begin_write().map_err(|e| redb_err("write txn", e))?;
        {
            let _ = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open documents table", e))?;
            let _ = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes table", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;

        info!(path = %path.display(), "sparse engine opened");

        let engine = Self { db: Arc::new(db) };
        engine.ensure_documents_versioned_table()?;
        engine.ensure_indexes_versioned_table()?;
        Ok(engine)
    }

    /// Insert or update a document (tenant-scoped).
    ///
    /// Returns the prior bytes when this write replaced an existing document,
    /// or `None` when it was a fresh insert. Callers thread the prior value
    /// into Event Plane emission so the `WriteOp` tag (Insert vs Update)
    /// reflects the actual mutation — there is no separate probe.
    pub fn put(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| redb_err("write txn", e))?;
            let prior = {
                let mut table = write_txn
                    .open_table(DOCUMENTS)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .insert(key, value)
                    .map_err(|e| redb_err("insert", e))?
                    .map(|g| g.value().to_vec())
            };
            write_txn.commit().map_err(|e| redb_err("commit", e))?;

            debug!(collection, document_id, len = value.len(), "document put");
            Ok(prior)
        })
    }

    /// Insert or update a document within an externally-owned write transaction.
    /// Same prior-bytes semantics as [`SparseEngine::put`].
    pub fn put_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let mut table = txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;
            let prior = table
                .insert(key, value)
                .map_err(|e| redb_err("insert", e))?
                .map(|g| g.value().to_vec());
            Ok(prior)
        })
    }

    /// Check whether a document exists within an externally-owned write
    /// transaction — the probe used by INSERT-with-unique-PK semantics.
    ///
    /// Uses the caller's write txn so the check is linearizable with the
    /// subsequent `put_in_txn`: no other writer can slip a row in between
    /// the "does it exist" read and the insert commit. Returns `Ok(true)`
    /// if a document with this (tenant, collection, document_id) is
    /// already present.
    pub fn exists_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &str,
    ) -> crate::Result<bool> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let table = txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;
            match table.get(key) {
                Ok(Some(_)) => Ok(true),
                Ok(None) => Ok(false),
                Err(e) => Err(redb_err("exists_in_txn", e)),
            }
        })
    }

    /// Batch insert or update multiple documents in a single redb transaction.
    pub fn batch_put(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        documents: &[(&str, &[u8])],
    ) -> crate::Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("batch write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;

            for (document_id, value) in documents {
                with_tenant_key(
                    database_id,
                    tenant_id,
                    collection,
                    document_id,
                    |key| -> crate::Result<()> {
                        table
                            .insert(key, *value)
                            .map_err(|e| redb_err("batch insert", e))?;
                        Ok(())
                    },
                )?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("batch commit", e))?;

        debug!(collection, count = documents.len(), "batch document put");
        Ok(())
    }

    /// Point lookup: retrieve a document by collection + document_id (tenant-scoped).
    pub fn get(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
            let table = read_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;

            match table.get(key) {
                Ok(Some(value)) => Ok(Some(value.value().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(redb_err("get", e)),
            }
        })
    }

    /// Approximate byte count for all documents in a single
    /// `(tenant_id, collection)` pair. Sums the raw value sizes via a
    /// redb range scan — O(N) in row count for a single read
    /// transaction. Best-effort: redb key overhead + secondary-index
    /// bytes are not counted. Used by the
    /// `_system.dropped_collections.size_bytes_estimate` column.
    pub fn approx_bytes_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> u64 {
        let prefix = coll_prefix(database_id, tenant_id, collection);
        let end = format!("{prefix}\u{ffff}");
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let table = match read_txn.open_table(DOCUMENTS) {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let mut total: u64 = 0;
        let range = match table.range::<&str>(prefix.as_str()..end.as_str()) {
            Ok(r) => r,
            Err(_) => return 0,
        };
        for entry in range {
            let Ok((_k, v)) = entry else { continue };
            total = total.saturating_add(v.value().len() as u64);
        }
        total
    }

    /// Delete a document (tenant-scoped).
    ///
    /// Returns the prior bytes when a row was actually removed, or `None`
    /// when nothing matched. The Event Plane needs the prior bytes as the
    /// `old_value` for CDC/trigger delete events; returning them here
    /// avoids a second read pass in the handler.
    pub fn delete(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| redb_err("write txn", e))?;
            let prior = {
                let mut table = write_txn
                    .open_table(DOCUMENTS)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .remove(key)
                    .map_err(|e| redb_err("remove", e))?
                    .map(|g| g.value().to_vec())
            };
            write_txn.commit().map_err(|e| redb_err("commit", e))?;

            debug!(
                collection,
                document_id,
                removed = prior.is_some(),
                "document delete"
            );
            Ok(prior)
        })
    }

    /// Begin a write transaction on the underlying database.
    pub fn begin_write(&self) -> crate::Result<WriteTransaction> {
        self.db
            .begin_write()
            .map_err(|e| redb_err("begin write txn", e))
    }

    /// Get the underlying database handle.
    pub fn db(&self) -> &Arc<Database> {
        &self.db
    }

    /// Re-key all documents and secondary indexes for
    /// `(old_database_id, tenant_id, old_collection)` to
    /// `(new_database_id, tenant_id, new_collection)` in a single write
    /// transaction. Supports both in-place renames and cross-database moves.
    ///
    /// Reads every row with the old prefix from both the DOCUMENTS and INDEXES
    /// tables, writes them under the new prefix, and deletes the old rows —
    /// all inside one transaction so the rename is atomic.
    ///
    /// Returns the count of document rows that were moved.
    pub fn rename_collection(
        &self,
        old_database_id: u64,
        new_database_id: u64,
        tenant_id: u64,
        old_collection: &str,
        new_collection: &str,
    ) -> crate::Result<usize> {
        let old_prefix = coll_prefix(old_database_id, tenant_id, old_collection);
        let old_end = format!("{old_prefix}\u{ffff}");
        let new_prefix = coll_prefix(new_database_id, tenant_id, new_collection);
        let new_prefix_len = new_prefix.len();

        // Collect document rows.
        let doc_rows: Vec<(String, Vec<u8>)> = {
            let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
            let table = read_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open docs", e))?;
            let mut out = Vec::new();
            for entry in table
                .range::<&str>(old_prefix.as_str()..old_end.as_str())
                .map_err(|e| redb_err("range docs", e))?
            {
                let (k, v) = entry.map_err(|e| redb_err("scan doc row", e))?;
                if let Some(suffix) = k.value().strip_prefix(&old_prefix) {
                    out.push((format!("{new_prefix}{suffix}"), v.value().to_vec()));
                }
            }
            out
        };

        // Collect index rows.
        let idx_rows: Vec<String> = {
            let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
            let table = read_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes", e))?;
            let mut out = Vec::new();
            for entry in table
                .range::<&str>(old_prefix.as_str()..old_end.as_str())
                .map_err(|e| redb_err("range indexes", e))?
            {
                let (k, _) = entry.map_err(|e| redb_err("scan idx row", e))?;
                if let Some(suffix) = k.value().strip_prefix(&old_prefix) {
                    out.push(suffix.to_string());
                }
            }
            out
        };

        if doc_rows.is_empty() && idx_rows.is_empty() {
            return Ok(0);
        }
        let doc_count = doc_rows.len();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut docs = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open docs write", e))?;
            for (new_key, value) in &doc_rows {
                docs.insert(new_key.as_str(), value.as_slice())
                    .map_err(|e| redb_err("insert doc renamed", e))?;
                let old_key = format!("{old_prefix}{}", &new_key[new_prefix_len..]);
                docs.remove(old_key.as_str())
                    .map_err(|e| redb_err("remove old doc", e))?;
            }

            let mut idxs = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes write", e))?;
            for suffix in &idx_rows {
                let new_key = format!("{new_prefix}{suffix}");
                let old_key = format!("{old_prefix}{suffix}");
                idxs.insert(new_key.as_str(), &[] as &[u8])
                    .map_err(|e| redb_err("insert idx renamed", e))?;
                idxs.remove(old_key.as_str())
                    .map_err(|e| redb_err("remove old idx", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit rename", e))?;

        debug!(
            tenant_id,
            old_collection, new_collection, doc_count, "sparse: rename_collection complete"
        );
        Ok(doc_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        (engine, dir)
    }

    #[test]
    fn put_and_get() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", "u1", b"alice").unwrap();
        engine.put(0, 1, "users", "u2", b"bob").unwrap();
        assert_eq!(
            engine.get(0, 1, "users", "u1").unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(0, 1, "users", "u2").unwrap(),
            Some(b"bob".to_vec())
        );
        assert_eq!(engine.get(0, 1, "users", "u3").unwrap(), None);
    }

    #[test]
    fn databases_are_isolated() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", "u1", b"alice").unwrap();
        engine.put(7, 1, "users", "u1", b"alice-db7").unwrap();
        assert_eq!(
            engine.get(0, 1, "users", "u1").unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(7, 1, "users", "u1").unwrap(),
            Some(b"alice-db7".to_vec())
        );
    }

    #[test]
    fn put_overwrites() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", "u1", b"alice").unwrap();
        engine.put(0, 1, "users", "u1", b"ALICE").unwrap();
        assert_eq!(
            engine.get(0, 1, "users", "u1").unwrap(),
            Some(b"ALICE".to_vec())
        );
    }

    #[test]
    fn delete_removes() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", "u1", b"alice").unwrap();
        assert_eq!(
            engine.delete(0, 1, "users", "u1").unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(engine.get(0, 1, "users", "u1").unwrap(), None);
        assert_eq!(engine.delete(0, 1, "users", "u1").unwrap(), None);
    }

    #[test]
    fn range_scan_with_index() {
        let (engine, _dir) = open_temp();
        engine.index_put(0, 1, "users", "age", "025", "u1").unwrap();
        engine.index_put(0, 1, "users", "age", "030", "u2").unwrap();
        engine.index_put(0, 1, "users", "age", "035", "u3").unwrap();
        engine.index_put(0, 1, "users", "age", "040", "u4").unwrap();
        let results = engine
            .range_scan(crate::engine::sparse::btree_index::RangeScanParams {
                database_id: 0,
                tenant_id: 1,
                collection: "users",
                field: "age",
                lower: Some(b"025"),
                upper: Some(b"036"),
                limit: 10,
            })
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn collections_are_isolated() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", "u1", b"alice").unwrap();
        engine.put(0, 1, "orders", "u1", b"order-1").unwrap();
        assert_eq!(
            engine.get(0, 1, "users", "u1").unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(0, 1, "orders", "u1").unwrap(),
            Some(b"order-1".to_vec())
        );
    }

    #[test]
    fn delete_index_entries_for_field() {
        let (engine, _dir) = open_temp();
        engine
            .index_put(0, 1, "users", "email", "alice@example.com", "u1")
            .unwrap();
        engine
            .index_put(0, 1, "users", "email", "bob@example.com", "u2")
            .unwrap();
        engine.index_put(0, 1, "users", "age", "30", "u1").unwrap();
        engine.index_put(0, 1, "users", "age", "25", "u2").unwrap();
        let removed = engine
            .delete_index_entries_for_field(0, 1, "users", "email")
            .unwrap();
        assert_eq!(removed, 2);
        let age_entries = engine.scan_index_groups(0, 1, "users", "age").unwrap();
        assert_eq!(age_entries.len(), 2);
        let email_entries = engine.scan_index_groups(0, 1, "users", "email").unwrap();
        assert!(email_entries.is_empty());
    }
}
