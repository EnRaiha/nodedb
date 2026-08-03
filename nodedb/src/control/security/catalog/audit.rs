// SPDX-License-Identifier: BUSL-1.1

//! Audit log operations for the system catalog.

use redb::{ReadableDatabase, ReadableTableMetadata};

use super::types::{AUDIT_LOG, StoredAuditEntry, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Delete audit entries older than `cutoff_us` (microseconds since epoch).
    pub fn prune_audit_before(&self, cutoff_us: u64) -> crate::Result<u64> {
        // Phase 1: Read to find keys to delete.
        let to_delete = {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("read txn", e))?;
            let table = read_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit", e))?;
            let mut keys = Vec::new();
            for entry in table
                .range::<&[u8]>(..)
                .map_err(|e| catalog_err("range audit", e))?
            {
                let (key, value) = entry.map_err(|e| catalog_err("read audit", e))?;
                let stored: StoredAuditEntry = match zerompk::from_msgpack(value.value()) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                if stored.timestamp_us < cutoff_us {
                    keys.push(key.value().to_vec());
                } else {
                    break;
                }
            }
            keys
        };

        if to_delete.is_empty() {
            return Ok(0);
        }

        // Phase 2: Write to delete the keys.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit", e))?;
            for key in &to_delete {
                table
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("prune audit", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(to_delete.len() as u64)
    }

    /// Append a batch of audit entries. Used by the periodic flush.
    pub fn append_audit_entries(&self, entries: &[StoredAuditEntry]) -> crate::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit_log", e))?;
            for entry in entries {
                let key = entry.seq.to_be_bytes();
                let value = zerompk::to_msgpack_vec(entry)
                    .map_err(|e| catalog_err("serialize audit", e))?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|e| catalog_err("insert audit", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Load the highest sequence number from the audit log.
    /// Used on startup to resume the sequence counter.
    pub fn load_audit_max_seq(&self) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;

        // Scan all keys to find the maximum sequence number.
        // Audit log keys are u64 big-endian, so the last entry in
        // iteration order is the highest.
        let mut max_seq = 0u64;
        let range = table
            .range::<&[u8]>(..)
            .map_err(|e| catalog_err("range audit", e))?;
        for entry in range {
            let (key, _) = entry.map_err(|e| catalog_err("read audit key", e))?;
            let key_bytes: &[u8] = key.value();
            if key_bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(key_bytes);
                let seq = u64::from_be_bytes(arr);
                if seq > max_seq {
                    max_seq = seq;
                }
            }
        }
        Ok(max_seq)
    }

    /// Load audit entries, most recent first, up to `limit`.
    pub fn load_recent_audit_entries(&self, limit: usize) -> crate::Result<Vec<StoredAuditEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;

        // Read all entries, then take the last `limit` (since redb iterates in key order = seq order).
        let mut all = Vec::new();
        for entry in table
            .range::<&[u8]>(..)
            .map_err(|e| catalog_err("range audit", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read audit", e))?;
            let stored: StoredAuditEntry =
                zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser audit", e))?;
            all.push(stored);
        }

        // Return most recent entries (last N).
        if all.len() > limit {
            Ok(all.split_off(all.len() - limit))
        } else {
            Ok(all)
        }
    }

    /// Delete the oldest audit entries until the total count is at or below
    /// `max_entries`. Returns the number of deleted entries plus the hash
    /// of the last deleted entry (for the `AuditCheckpoint` prev_hash) and
    /// the seq of the oldest surviving entry. Returns `(0, "", 0)` when no
    /// pruning was needed.
    pub fn prune_audit_to_count(&self, max_entries: u64) -> crate::Result<(u64, String, u64)> {
        let total = self.audit_entry_count()?;
        if total <= max_entries {
            return Ok((0, String::new(), 0));
        }
        let to_remove = total - max_entries;

        // Phase 1: collect keys and the hash of the last entry to be deleted.
        let (to_delete, last_deleted_hash, oldest_kept_seq) = {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("read txn", e))?;
            let table = read_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit", e))?;
            let mut keys: Vec<Vec<u8>> = Vec::new();
            let mut last_hash = String::new();
            let mut first_kept_seq = 0u64;
            for (idx, entry) in table
                .range::<&[u8]>(..)
                .map_err(|e| catalog_err("range audit", e))?
                .enumerate()
            {
                let (key, value) = entry.map_err(|e| catalog_err("read audit", e))?;
                if (idx as u64) < to_remove {
                    keys.push(key.value().to_vec());
                } else {
                    // This is the first surviving entry.
                    // Its prev_hash = hash of the last deleted entry — that
                    // is exactly what the AuditCheckpoint needs as its own
                    // prev_hash so the surviving chain verifies correctly.
                    let stored: StoredAuditEntry = match zerompk::from_msgpack(value.value()) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    first_kept_seq = stored.seq;
                    last_hash = stored.prev_hash.clone();
                    break;
                }
            }
            (keys, last_hash, first_kept_seq)
        };

        if to_delete.is_empty() {
            return Ok((0, String::new(), 0));
        }

        // Phase 2: delete.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit", e))?;
            for key in &to_delete {
                table
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("prune audit count", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok((to_delete.len() as u64, last_deleted_hash, oldest_kept_seq))
    }

    /// Load audit entries filtered by seq and/or timestamp range, up to `limit`.
    ///
    /// `from_seq`/`to_seq` are inclusive bounds (pass 0/u64::MAX for no bound).
    /// `from_ts_us`/`to_ts_us` are inclusive timestamp bounds in microseconds.
    /// Since the table is seq-ordered (big-endian key), seq range pushdown is
    /// cheap: we seek to `from_seq` and stop at `to_seq`.
    /// Timestamp filtering applies on top of the seq scan.
    /// Returns entries in ascending seq order (oldest first).
    pub fn load_audit_entries_ranged(
        &self,
        from_seq: u64,
        to_seq: u64,
        from_ts_us: u64,
        to_ts_us: u64,
        limit: usize,
    ) -> crate::Result<Vec<StoredAuditEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;

        let start_key = from_seq.to_be_bytes();
        let end_key = to_seq.to_be_bytes();

        let mut results = Vec::new();
        for entry in table
            .range(start_key.as_slice()..=end_key.as_slice())
            .map_err(|e| catalog_err("range audit", e))?
        {
            if results.len() >= limit {
                break;
            }
            let (_, value) = entry.map_err(|e| catalog_err("read audit", e))?;
            let stored: StoredAuditEntry =
                zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser audit", e))?;
            if stored.timestamp_us >= from_ts_us && stored.timestamp_us <= to_ts_us {
                results.push(stored);
            }
        }
        Ok(results)
    }

    /// Count total audit entries (for diagnostics).
    pub fn audit_entry_count(&self) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;
        table.len().map_err(|e| catalog_err("count audit", e))
    }
}
