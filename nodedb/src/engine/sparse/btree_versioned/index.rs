// SPDX-License-Identifier: BUSL-1.1

//! Versioned secondary-index operations.
//!
//! Index key: `"{database_id}:{tenant}:{coll}:{field}:{value}:{doc_id}\x00{sys_from:020}"`.
//! Value: single byte (`0x00` live, `0xFF` tombstone).

use redb::{ReadableDatabase, TableDefinition};

use super::key::format_sys_from;
use super::value::{TAG_LIVE, TAG_TOMBSTONE, VersionedIndexEntry};
use crate::engine::sparse::btree::{SparseEngine, redb_err};

/// Keys carry the leading `{database_id}:` component.
pub(crate) const INDEXES_VERSIONED: TableDefinition<&str, &[u8]> =
    TableDefinition::new("indexes_versioned");

impl SparseEngine {
    /// Bootstrap: ensure the versioned index table exists.
    pub(in crate::engine::sparse) fn ensure_indexes_versioned_table(&self) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let _ = txn
                .open_table(INDEXES_VERSIONED)
                .map_err(|e| redb_err("open indexes_versioned", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// `versioned_index_put` inside a caller-owned write transaction.
    pub fn versioned_index_put_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        e: VersionedIndexEntry<'_>,
    ) -> crate::Result<()> {
        if e.doc_id.as_bytes().contains(&0) {
            return Err(crate::Error::BadRequest {
                detail: "document id may not contain NUL byte".into(),
            });
        }
        let key = e.redb_key();
        let mut t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        t.insert(key.as_str(), [TAG_LIVE].as_slice())
            .map_err(|e| redb_err("insert", e))?;
        Ok(())
    }

    /// `versioned_index_tombstone` inside a caller-owned write transaction.
    pub fn versioned_index_tombstone_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        e: VersionedIndexEntry<'_>,
    ) -> crate::Result<()> {
        let key = e.redb_key();
        let mut t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        t.insert(key.as_str(), [TAG_TOMBSTONE].as_slice())
            .map_err(|e| redb_err("insert tombstone", e))?;
        Ok(())
    }

    /// Physically remove one versioned index entry's redb entry inside a
    /// caller-owned write transaction. Unlike
    /// [`Self::versioned_index_tombstone_in_txn`] (which appends a
    /// tombstone marker), this deletes the entry at `sys_from_ms` outright.
    /// Used by the transaction-rollback path to undo a
    /// `versioned_index_put_in_txn` that must not survive an aborted
    /// transaction. Removing a non-existent key is a no-op.
    pub fn versioned_index_remove_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        e: VersionedIndexEntry<'_>,
    ) -> crate::Result<()> {
        let key = e.redb_key();
        let mut t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        t.remove(key.as_str()).map_err(|e| redb_err("remove", e))?;
        Ok(())
    }

    /// Append a versioned secondary index entry in its own transaction.
    pub fn versioned_index_put(&self, e: VersionedIndexEntry<'_>) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.versioned_index_put_in_txn(&txn, e)?;
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Append a tombstone entry for an index value that's been removed,
    /// in its own transaction.
    pub fn versioned_index_tombstone(&self, e: VersionedIndexEntry<'_>) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.versioned_index_tombstone_in_txn(&txn, e)?;
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Look up doc_ids for a `(field, value)` pair at a system-time cutoff.
    /// Returns only doc_ids whose newest entry ≤ cutoff is live.
    pub fn versioned_index_lookup_as_of(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        field: &str,
        value: &str,
        sys_cutoff_ms: Option<i64>,
    ) -> crate::Result<Vec<String>> {
        let lo = format!("{database_id}:{tenant}:{coll}:{field}:{value}:");
        // `:` = 0x3A, next byte `;` = 0x3B gives a clean exclusive bound.
        let hi = format!("{database_id}:{tenant}:{coll}:{field}:{value};");
        let cutoff_key = sys_cutoff_ms.map(format_sys_from);

        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?;

        // Group by doc_id; keep newest-in-window tag per doc.
        let mut out = Vec::new();
        let mut current_id: Option<String> = None;
        let mut best: Option<(i64, u8)> = None;

        for r in range {
            let (k, v) = r.map_err(|e| redb_err("entry", e))?;
            let key_str = k.value();
            let Some(rest) = key_str.strip_prefix(lo.as_str()) else {
                continue;
            };
            let Some((doc_id, suffix)) = rest.rsplit_once('\x00') else {
                continue;
            };
            if let Some(ref c) = cutoff_key
                && suffix > c.as_str()
            {
                continue;
            }
            let Ok(sf) = suffix.parse::<i64>() else {
                continue;
            };
            let tag = v.value().first().copied().unwrap_or(TAG_TOMBSTONE);

            if current_id.as_deref() != Some(doc_id) {
                if let Some(prev) = current_id.as_ref()
                    && let Some((_, t)) = best
                    && t == TAG_LIVE
                {
                    out.push(prev.clone());
                }
                current_id = Some(doc_id.to_string());
                best = None;
            }
            best = Some(match best.take() {
                Some((prev_sf, prev_t)) if prev_sf >= sf => (prev_sf, prev_t),
                _ => (sf, tag),
            });
        }
        if let Some(prev) = current_id
            && let Some((_, t)) = best
            && t == TAG_LIVE
        {
            out.push(prev);
        }
        Ok(out)
    }
}
