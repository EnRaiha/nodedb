// SPDX-License-Identifier: BUSL-1.1

//! Blacklist catalog operations (redb persistence).

use super::types::{BLACKLIST, StoredBlacklistEntry, SystemCatalog, catalog_err};
use redb::ReadableDatabase;

impl SystemCatalog {
    /// Insert or update a blacklist entry.
    pub fn put_blacklist_entry(&self, entry: &StoredBlacklistEntry) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(entry)
            .map_err(|e| catalog_err("serialize blacklist entry", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("blacklist write txn", e))?;
        {
            let mut table = write_txn
                .open_table(BLACKLIST)
                .map_err(|e| catalog_err("open blacklist", e))?;
            table
                .insert(entry.key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert blacklist entry", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("blacklist commit", e))?;
        Ok(())
    }

    /// Remove a blacklist entry by key.
    pub fn delete_blacklist_entry(&self, key: &str) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("blacklist write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(BLACKLIST)
                .map_err(|e| catalog_err("open blacklist", e))?;
            table
                .remove(key)
                .map_err(|e| catalog_err("remove blacklist entry", e))?
                .is_some()
        };
        write_txn
            .commit()
            .map_err(|e| catalog_err("blacklist commit", e))?;
        Ok(removed)
    }

    /// Load all blacklist entries.
    pub fn load_all_blacklist_entries(&self) -> crate::Result<Vec<StoredBlacklistEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("blacklist read txn", e))?;
        let table = read_txn
            .open_table(BLACKLIST)
            .map_err(|e| catalog_err("open blacklist", e))?;
        let mut entries = Vec::new();
        let range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range blacklist", e))?;
        for item in range {
            let (_, value) = item.map_err(|e| catalog_err("read blacklist entry", e))?;
            if let Ok(entry) = zerompk::from_msgpack::<StoredBlacklistEntry>(value.value()) {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}
