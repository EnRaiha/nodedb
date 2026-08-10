// SPDX-License-Identifier: BUSL-1.1

//! Per-scope token quota catalog operations (redb persistence).

use super::types::{SCOPE_QUOTAS, StoredScopeQuota, SystemCatalog, catalog_err};
use redb::ReadableDatabase;

impl SystemCatalog {
    /// Insert or update a scope quota definition.
    pub fn put_scope_quota(&self, quota: &StoredScopeQuota) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(quota).map_err(|e| catalog_err("serialize scope quota", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("scope quota write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SCOPE_QUOTAS)
                .map_err(|e| catalog_err("open scope quotas", e))?;
            table
                .insert(quota.scope_name.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert scope quota", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("scope quota commit", e))?;
        Ok(())
    }

    /// Remove a scope quota definition, reporting whether one was present.
    pub fn delete_scope_quota(&self, scope_name: &str) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("scope quota write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(SCOPE_QUOTAS)
                .map_err(|e| catalog_err("open scope quotas", e))?;
            table
                .remove(scope_name)
                .map_err(|e| catalog_err("remove scope quota", e))?
                .is_some()
        };
        write_txn
            .commit()
            .map_err(|e| catalog_err("scope quota commit", e))?;
        Ok(removed)
    }

    /// Load every stored scope quota definition.
    ///
    /// A row that fails to decode is propagated rather than skipped: a quota
    /// silently dropped at boot is a cap that stops applying, which is the
    /// failure this table exists to prevent.
    pub fn load_all_scope_quotas(&self) -> crate::Result<Vec<StoredScopeQuota>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("scope quota read txn", e))?;
        let table = read_txn
            .open_table(SCOPE_QUOTAS)
            .map_err(|e| catalog_err("open scope quotas", e))?;
        let mut quotas = Vec::new();
        let range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range scope quotas", e))?;
        for item in range {
            let (_, value) = item.map_err(|e| catalog_err("read scope quota", e))?;
            let quota = zerompk::from_msgpack::<StoredScopeQuota>(value.value())
                .map_err(|e| catalog_err("decode scope quota", e))?;
            quotas.push(quota);
        }
        Ok(quotas)
    }
}
