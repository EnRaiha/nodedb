// SPDX-License-Identifier: BUSL-1.1

//! `InvertedIndex` struct, lifecycle, backend access, and structural
//! tenant/collection purge. All other concerns (indexing, search,
//! synonyms, compaction) live in sibling modules.

use std::sync::Arc;

use redb::Database;

use nodedb_mem::MemoryGovernor;
use nodedb_types::TenantId;

use super::errors::into_result_err;
use crate::engine::sparse::fts_redb::RedbFtsBackend;
use crate::storage::quarantine::QuarantineRegistry;

/// Full-text inverted index backed by redb via `nodedb-fts`.
pub struct InvertedIndex {
    pub(super) inner: nodedb_fts::index::FtsIndex<RedbFtsBackend>,
}

impl InvertedIndex {
    /// Open or create an inverted index at the given redb database, with
    /// FTS memory budgeted against `governor`.
    pub fn open(db: Arc<Database>, governor: Arc<MemoryGovernor>) -> crate::Result<Self> {
        let backend = RedbFtsBackend::open(db)?;
        Ok(Self {
            inner: nodedb_fts::index::FtsIndex::new(backend, governor),
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

#[cfg(test)]
mod tests {
    use nodedb_fts::FtsSearchParams;
    use nodedb_fts::posting::QueryMode;
    use nodedb_types::Surrogate;

    use super::*;

    const DB: u64 = 0;

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(Database::create(&path).unwrap());
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    #[test]
    fn purge_tenant_structurally_drops_data() {
        let (idx, _dir) = open_temp();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        idx.index_document(DB, t1, "docs", Surrogate::new(1), "alpha bravo")
            .unwrap();
        idx.index_document(DB, t2, "docs", Surrogate::new(1), "alpha bravo")
            .unwrap();

        idx.purge_tenant(DB, t1).unwrap();

        assert!(
            idx.search(
                DB,
                t1,
                "docs",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None
                }
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            !idx.search(
                DB,
                t2,
                "docs",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None
                }
            )
            .unwrap()
            .is_empty()
        );
    }
}
