// SPDX-License-Identifier: BUSL-1.1

//! `InvertedIndex` struct, lifecycle, backend access, and structural
//! tenant/collection purge. All other concerns (indexing, search,
//! synonyms, compaction) live in sibling modules.

use std::sync::Arc;

use redb::Database;

use nodedb_types::TenantId;

use super::errors::into_result_err;
use crate::engine::sparse::fts_redb::RedbFtsBackend;
use crate::storage::quarantine::QuarantineRegistry;

/// Full-text inverted index backed by redb via `nodedb-fts`.
pub struct InvertedIndex {
    pub(super) inner: nodedb_fts::index::FtsIndex<RedbFtsBackend>,
}

impl InvertedIndex {
    /// Open or create an inverted index at the given redb database.
    pub fn open(db: Arc<Database>) -> crate::Result<Self> {
        let backend = RedbFtsBackend::open(db)?;
        Ok(Self {
            inner: nodedb_fts::index::FtsIndex::new(backend),
        })
    }

    /// Install the quarantine registry for corrupt FTS segment detection.
    ///
    /// Called once by the server bootstrap after the registry is created.
    pub fn set_quarantine_registry(&mut self, registry: Arc<QuarantineRegistry>) {
        self.inner.backend_mut().set_quarantine_registry(registry);
    }

    /// Shared access to the underlying redb FTS backend.
    ///
    /// Exposes the raw `FtsBackend` methods for maintenance operations such as
    /// bulk postings snapshot and restore used by concurrent index rebuild.
    pub fn backend(&self) -> &RedbFtsBackend {
        self.inner.backend()
    }

    /// Mutable access to the underlying redb FTS backend.
    pub fn backend_mut(&mut self) -> &mut RedbFtsBackend {
        self.inner.backend_mut()
    }

    /// Purge all inverted index entries for a `(database, tenant)`. Structural
    /// drop via tuple ranges on every FTS table.
    pub fn purge_tenant(&self, database_id: u64, tid: TenantId) -> crate::Result<usize> {
        self.inner
            .purge_tenant(database_id, tid.as_u64())
            .map_err(into_result_err)
    }

    /// Purge all inverted index entries for a single
    /// `(database, tenant, collection)`. Structural drop via tuple ranges on
    /// every FTS table.
    pub fn purge_collection(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<usize> {
        self.inner
            .purge_collection(database_id, tid.as_u64(), collection)
            .map_err(into_result_err)
    }
}
