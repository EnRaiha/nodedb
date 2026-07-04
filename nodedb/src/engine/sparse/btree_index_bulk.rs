// SPDX-License-Identifier: BUSL-1.1

//! Bulk purge operations for the sparse engine's secondary index + documents.
//!
//! Collection- and tenant-scoped hard drops. Split out of `btree_index.rs` to
//! keep that file focused on per-entry index CRUD.

use redb::ReadableTable;
use tracing::info;

use super::btree::{DOCUMENTS, INDEXES, SparseEngine, coll_prefix, redb_err};

impl SparseEngine {
    /// Delete ALL documents and indexes for a single `(tenant_id, collection)`.
    ///
    /// Collection-scoped analogue of [`Self::delete_all_for_tenant`]. Used by
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
}
