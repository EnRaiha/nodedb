// SPDX-License-Identifier: BUSL-1.1

//! Secondary index operations for the sparse engine.
//!
//! Index key format: `"{database_id}:{tenant_id}:{collection}:{field}:{value}:{document_id}"`.
//! Extracted from `btree.rs` — document CRUD stays there, index ops live here.

use redb::ReadableTable;
use tracing::{debug, info};

use super::btree::{DOCUMENTS, INDEXES, SparseEngine, coll_prefix, redb_err};

impl SparseEngine {
    /// Delete all secondary index entries for a document.
    ///
    /// Scans the INDEXES table for entries ending with `:{document_id}` and
    /// removes them. Called during document deletion cascade.
    pub fn delete_indexes_for_document(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &str,
    ) -> crate::Result<()> {
        let prefix = coll_prefix(database_id, tenant_id, collection);
        let end = format!("{prefix}\u{ffff}");
        let suffix = format!(":{document_id}");

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes", e))?;

            let keys_to_remove: Vec<String> = table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| redb_err("index range", e))?
                .filter_map(|r| {
                    r.ok().and_then(|(k, _)| {
                        let key = k.value().to_string();
                        if key.ends_with(&suffix) {
                            Some(key)
                        } else {
                            None
                        }
                    })
                })
                .collect();

            for key in &keys_to_remove {
                table
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove index", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit index cascade", e))?;

        Ok(())
    }

    /// Delete all secondary index entries for a specific field in a collection.
    pub fn delete_index_entries_for_field(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
    ) -> crate::Result<usize> {
        let prefix = format!(
            "{}{field}:",
            coll_prefix(database_id, tenant_id, collection)
        );
        let end = format!("{prefix}\u{ffff}");

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;

        let removed;
        {
            let mut table = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes", e))?;

            let keys_to_remove: Vec<String> = table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| redb_err("index range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();

            removed = keys_to_remove.len();
            for key in &keys_to_remove {
                table
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove index entry", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit index delete", e))?;

        if removed > 0 {
            debug!(
                collection,
                field, removed, "index entries deleted for field"
            );
        }

        Ok(removed)
    }

    /// Delete ALL documents and indexes for a single `(tenant_id, collection)`.
    ///
    /// Collection-scoped analogue of [`delete_all_for_tenant`]. Used by
    /// `execute_unregister_collection` when a collection is hard-dropped.
    pub fn delete_all_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<(usize, usize)> {
        let prefix = coll_prefix(database_id, tenant_id, collection);
        let end = format!("{prefix}\u{ffff}");

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;

        let docs_removed;
        {
            let mut table = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open docs", e))?;
            let keys: Vec<String> = table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| redb_err("doc range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            docs_removed = keys.len();
            for key in &keys {
                table
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove doc", e))?;
            }
        }

        let idx_removed;
        {
            let mut table = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes", e))?;
            let keys: Vec<String> = table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| redb_err("index range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            idx_removed = keys.len();
            for key in &keys {
                table
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove index", e))?;
            }
        }

        write_txn
            .commit()
            .map_err(|e| redb_err("commit collection purge", e))?;

        if docs_removed > 0 || idx_removed > 0 {
            info!(
                tenant_id,
                collection, docs_removed, idx_removed, "collection data purged from sparse engine"
            );
        }

        Ok((docs_removed, idx_removed))
    }

    /// Delete ALL documents and indexes for a tenant across all collections
    /// within a single database. A tenant lives in exactly one database, so
    /// the purge is scoped by `(database_id, tenant_id)`.
    pub fn delete_all_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<(usize, usize)> {
        let prefix = super::btree::tenant_prefix(database_id, tenant_id);
        let end = format!("{prefix}\u{ffff}");

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;

        let docs_removed;
        {
            let mut table = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open docs", e))?;
            let keys: Vec<String> = table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| redb_err("doc range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            docs_removed = keys.len();
            for key in &keys {
                table
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove doc", e))?;
            }
        }

        let idx_removed;
        {
            let mut table = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes", e))?;
            let keys: Vec<String> = table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| redb_err("index range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            idx_removed = keys.len();
            for key in &keys {
                table
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove index", e))?;
            }
        }

        write_txn
            .commit()
            .map_err(|e| redb_err("commit tenant purge", e))?;

        if docs_removed > 0 || idx_removed > 0 {
            info!(
                tenant_id,
                docs_removed, idx_removed, "tenant data purged from sparse engine"
            );
        }

        Ok((docs_removed, idx_removed))
    }

    /// Range scan on secondary index entries.
    #[allow(clippy::too_many_arguments)]
    pub fn range_scan(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let prefix = format!(
            "{}{field}:",
            coll_prefix(database_id, tenant_id, collection)
        );

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEXES)
            .map_err(|e| redb_err("open table", e))?;

        let start = match lower {
            Some(l) => format!("{prefix}{}", String::from_utf8_lossy(l)),
            None => prefix.clone(),
        };
        let end = match upper {
            Some(u) => format!("{prefix}{}", String::from_utf8_lossy(u)),
            None => {
                let mut end = prefix.clone();
                end.push('\u{ffff}');
                end
            }
        };

        let mut results = Vec::with_capacity(limit.min(256));
        let range = table
            .range(start.as_str()..end.as_str())
            .map_err(|e| redb_err("range", e))?;

        for entry in range {
            if results.len() >= limit {
                break;
            }
            let entry = entry.map_err(|e| redb_err("range entry", e))?;
            let key = entry.0.value().to_string();
            let value = entry.1.value().to_vec();
            results.push((key, value));
        }

        debug!(collection, field, count = results.len(), "range scan");
        Ok(results)
    }

    /// Insert a secondary index entry (tenant-scoped).
    ///
    /// Opens its own write transaction. Callers already inside a write
    /// transaction — the PointPut / BatchInsert apply path — MUST use
    /// [`SparseEngine::index_put_in_txn`] instead; redb allows only one
    /// active writer so a nested `begin_write` here deadlocks.
    pub fn index_put(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        value: &str,
        document_id: &str,
    ) -> crate::Result<()> {
        super::btree::with_tenant_key4(
            database_id,
            tenant_id,
            collection,
            field,
            value,
            document_id,
            |key| {
                let write_txn = self
                    .db
                    .begin_write()
                    .map_err(|e| redb_err("write txn", e))?;
                {
                    let mut table = write_txn
                        .open_table(INDEXES)
                        .map_err(|e| redb_err("open table", e))?;
                    table
                        .insert(key, [].as_slice())
                        .map_err(|e| redb_err("index insert", e))?;
                }
                write_txn.commit().map_err(|e| redb_err("commit", e))?;
                Ok(())
            },
        )
    }

    /// Insert a secondary index entry into an already-open write txn.
    ///
    /// Use from the PointPut / BatchInsert apply path so document + index
    /// writes commit atomically in the same redb transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn index_put_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        value: &str,
        document_id: &str,
    ) -> crate::Result<()> {
        super::btree::with_tenant_key4(
            database_id,
            tenant_id,
            collection,
            field,
            value,
            document_id,
            |key| {
                let mut table = txn
                    .open_table(INDEXES)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .insert(key, [].as_slice())
                    .map_err(|e| redb_err("index insert", e))?;
                Ok(())
            },
        )
    }

    /// Index-only scan: return `(doc_id, field_value)` pairs without touching DOCUMENTS.
    pub fn scan_index_values(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, String)>> {
        let prefix = format!(
            "{}{field}:",
            coll_prefix(database_id, tenant_id, collection)
        );
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEXES)
            .map_err(|e| redb_err("open table", e))?;

        let range = table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| redb_err("index range", e))?;

        let mut results = Vec::with_capacity(limit.min(256));
        for entry in range {
            if results.len() >= limit {
                break;
            }
            let entry = entry.map_err(|e| redb_err("index entry", e))?;
            let key = entry.0.value().to_string();
            if let Some(rest) = key.strip_prefix(&prefix)
                && let Some(colon_pos) = rest.rfind(':')
            {
                let value = &rest[..colon_pos];
                let doc_id = &rest[colon_pos + 1..];
                results.push((doc_id.to_string(), value.to_string()));
            }
        }

        debug!(collection, field, count = results.len(), "index-only scan");
        Ok(results)
    }

    /// Insert a document by raw pre-formed key (snapshot restore).
    pub fn put_raw(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("raw write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;
            table
                .insert(key, value)
                .map_err(|e| redb_err("raw insert", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Point lookup by raw pre-formed key (snapshot restore verification).
    pub fn get_raw(&self, key: &str) -> crate::Result<Option<Vec<u8>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("raw read txn", e))?;
        let table = read_txn
            .open_table(DOCUMENTS)
            .map_err(|e| redb_err("open table", e))?;
        match table.get(key) {
            Ok(Some(v)) => Ok(Some(v.value().to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(redb_err("raw get", e)),
        }
    }
}
