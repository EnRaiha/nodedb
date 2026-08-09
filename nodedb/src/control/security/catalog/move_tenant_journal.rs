// SPDX-License-Identifier: BUSL-1.1

//! `_system.move_tenant_journal` redb CRUD.
//!
//! These methods live inside the `catalog` module so they have direct access
//! to `SystemCatalog::db`. The `MOVE TENANT` workflow drives them through the
//! thin wrappers it keeps beside the rest of that workflow.

use super::move_tenant_journal_types::{MOVE_TENANT_JOURNAL, MoveTenantJournalEntry};
use super::system_catalog::SystemCatalog;
use super::types::catalog_err;
use redb::ReadableDatabase;

impl SystemCatalog {
    /// Load the journal entry for `tenant_id`, if one exists.
    pub fn move_tenant_journal_load(
        &self,
        tenant_id: u64,
    ) -> crate::Result<Option<MoveTenantJournalEntry>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = match txn.open_table(MOVE_TENANT_JOURNAL) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(catalog_err("open move_tenant_journal", e)),
        };
        let bytes = table
            .get(tenant_id)
            .map_err(|e| catalog_err("get move_tenant_journal", e))?;
        match bytes {
            None => Ok(None),
            Some(guard) => {
                let entry: MoveTenantJournalEntry = zerompk::from_msgpack(guard.value())
                    .map_err(|e| catalog_err("decode move_tenant_journal", e))?;
                Ok(Some(entry))
            }
        }
    }

    /// Write or overwrite the journal entry for `entry.tenant_id`.
    pub fn move_tenant_journal_save(&self, entry: &MoveTenantJournalEntry) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(entry)
            .map_err(|e| catalog_err("encode move_tenant_journal", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn move_tenant_journal", e))?;
        {
            let mut table = txn
                .open_table(MOVE_TENANT_JOURNAL)
                .map_err(|e| catalog_err("open move_tenant_journal", e))?;
            table
                .insert(entry.tenant_id, bytes.as_slice())
                .map_err(|e| catalog_err("insert move_tenant_journal", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit move_tenant_journal", e))?;
        Ok(())
    }

    /// Remove the journal entry for `tenant_id`.
    pub fn move_tenant_journal_delete(&self, tenant_id: u64) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn move_tenant_journal", e))?;
        {
            let mut table = match txn.open_table(MOVE_TENANT_JOURNAL) {
                Ok(t) => t,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(e) => return Err(catalog_err("open move_tenant_journal", e)),
            };
            table
                .remove(tenant_id)
                .map_err(|e| catalog_err("remove move_tenant_journal", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit move_tenant_journal", e))?;
        Ok(())
    }

    /// Scan all in-progress journal entries. Used by startup recovery.
    pub fn move_tenant_journal_scan_all(&self) -> crate::Result<Vec<MoveTenantJournalEntry>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = match txn.open_table(MOVE_TENANT_JOURNAL) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(catalog_err("open move_tenant_journal", e)),
        };
        let mut entries = Vec::new();
        for result in table
            .range::<u64>(..)
            .map_err(|e| catalog_err("range move_tenant_journal", e))?
        {
            let (_, value) = result.map_err(|e| catalog_err("iter move_tenant_journal", e))?;
            let entry: MoveTenantJournalEntry = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("decode move_tenant_journal", e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}
