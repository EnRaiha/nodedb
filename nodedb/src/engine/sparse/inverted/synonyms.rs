// SPDX-License-Identifier: BUSL-1.1

//! Synonym group persistence: thin wrappers over `FtsIndex` that translate
//! tenant ids and surface backend errors as `crate::Error::Storage`.

use nodedb_types::TenantId;

use super::core::InvertedIndex;
use super::errors::inverted_err;

impl InvertedIndex {
    /// Persist a synonym group to the FTS backend.
    pub fn put_synonym_group(
        &self,
        database_id: u64,
        tid: TenantId,
        rec: &nodedb_fts::SynonymGroupRecord,
    ) -> crate::Result<()> {
        self.inner
            .put_synonym_group(database_id, tid.as_u64(), rec)
            .map_err(|e| inverted_err("put_synonym_group", e))
    }

    /// Delete a synonym group from the FTS backend. Returns `true` if it existed.
    pub fn delete_synonym_group(
        &self,
        database_id: u64,
        tid: TenantId,
        name: &str,
    ) -> crate::Result<bool> {
        self.inner
            .delete_synonym_group(database_id, tid.as_u64(), name)
            .map_err(|e| inverted_err("delete_synonym_group", e))
    }

    /// List all synonym groups for a tenant.
    pub fn list_synonym_groups(
        &self,
        database_id: u64,
        tid: TenantId,
    ) -> crate::Result<Vec<nodedb_fts::SynonymGroupRecord>> {
        self.inner
            .list_synonym_groups(database_id, tid.as_u64())
            .map_err(|e| inverted_err("list_synonym_groups", e))
    }
}
