// SPDX-License-Identifier: BUSL-1.1

//! `RedbFtsBackend` struct, lifecycle, and the `FtsBackend` trait impl.
//!
//! Every trait method delegates to a topic-specific module
//! (`postings`, `doc_lengths`, `meta`, `stats`, `segments`, `purge`) so
//! this file stays focused on wiring rather than business logic.

use std::sync::Arc;

use redb::Database;

use nodedb_fts::backend::FtsBackend;
use nodedb_fts::posting::Posting;
use nodedb_types::Surrogate;

use super::segments::CompactCommit;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::tables::{
    DOC_LENGTHS, DOC_TERMS, INDEX_META, POSTINGS, SEGMENTS, STATS,
};
use crate::storage::quarantine::QuarantineRegistry;

/// Redb-backed FTS backend.
///
/// All persistent tables are keyed by the structural tuple
/// `(database_id, tenant_id, collection, …)` — database and tenant isolation
/// are enforced by the table schema, never by lexical-prefix ordering.
pub struct RedbFtsBackend {
    pub(super) db: Arc<Database>,
    /// Shared quarantine registry for corrupt FTS segment bytes.
    /// `None` until wired by the server bootstrap.
    pub(super) quarantine_registry: Option<Arc<QuarantineRegistry>>,
}

impl RedbFtsBackend {
    /// Open or create redb tables for FTS.
    pub fn open(db: Arc<Database>) -> crate::Result<Self> {
        let write_txn = db.begin_write().map_err(|e| redb_err("init tables", e))?;
        {
            write_txn
                .open_table(POSTINGS)
                .map_err(|e| redb_err("create postings table", e))?;
            write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| redb_err("create doc_lengths table", e))?;
            write_txn
                .open_table(DOC_TERMS)
                .map_err(|e| redb_err("create doc_terms table", e))?;
            write_txn
                .open_table(INDEX_META)
                .map_err(|e| redb_err("create index_meta table", e))?;
            write_txn
                .open_table(STATS)
                .map_err(|e| redb_err("create stats table", e))?;
            write_txn
                .open_table(SEGMENTS)
                .map_err(|e| redb_err("create segments table", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit init", e))?;

        Ok(Self {
            db,
            quarantine_registry: None,
        })
    }

    /// Access the underlying database.
    pub fn db(&self) -> &Database {
        &self.db
    }

    /// Install the quarantine registry.
    pub fn set_quarantine_registry(&mut self, registry: Arc<QuarantineRegistry>) {
        self.quarantine_registry = Some(registry);
    }

    /// Atomically write a new merged FTS segment and remove the source segments.
    ///
    /// All mutations run in a single redb write transaction so the operation
    /// is crash-safe: a crash mid-compaction leaves the original segments
    /// intact and the maintenance cycle retries on the next pass.
    pub fn compact_commit(&self, params: CompactCommit<'_>) -> crate::Result<()> {
        super::segments::compact_commit(self, params)
    }

    /// Enumerate all `(database_id, tid, collection)` triples that have at least
    /// one FTS segment. Used by maintenance to discover compaction candidates
    /// without a separate registry.
    pub fn list_all_fts_collections(&self) -> crate::Result<Vec<(u64, u64, String)>> {
        super::segments::list_all_collections(self)
    }
}

impl FtsBackend for RedbFtsBackend {
    type Error = crate::Error;

    fn read_postings(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        term: &str,
    ) -> crate::Result<Vec<Posting>> {
        super::postings::read(self, database_id, tid, collection, term)
    }

    fn write_postings(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        term: &str,
        postings: &[Posting],
    ) -> crate::Result<()> {
        super::postings::write(self, database_id, tid, collection, term, postings)
    }

    fn remove_postings(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        term: &str,
    ) -> crate::Result<()> {
        super::postings::remove(self, database_id, tid, collection, term)
    }

    fn read_doc_length(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
    ) -> crate::Result<Option<u32>> {
        super::doc_lengths::read(self, database_id, tid, collection, doc_id)
    }

    fn write_doc_length(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
        length: u32,
    ) -> crate::Result<()> {
        super::doc_lengths::write(self, database_id, tid, collection, doc_id, length)
    }

    fn remove_doc_length(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
    ) -> crate::Result<()> {
        super::doc_lengths::remove(self, database_id, tid, collection, doc_id)
    }

    fn collection_terms(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> crate::Result<Vec<String>> {
        super::postings::collection_terms(self, database_id, tid, collection)
    }

    fn collection_stats(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> crate::Result<(u32, u64)> {
        super::stats::read(self, database_id, tid, collection)
    }

    fn increment_stats(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc_len: u32,
    ) -> crate::Result<()> {
        super::stats::increment(self, database_id, tid, collection, doc_len)
    }

    fn decrement_stats(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc_len: u32,
    ) -> crate::Result<()> {
        super::stats::decrement(self, database_id, tid, collection, doc_len)
    }

    fn read_meta(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        subkey: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        super::meta::read(self, database_id, tid, collection, subkey)
    }

    fn write_meta(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        subkey: &str,
        value: &[u8],
    ) -> crate::Result<()> {
        super::meta::write(self, database_id, tid, collection, subkey, value)
    }

    fn write_segment(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        segment_id: &str,
        data: &[u8],
    ) -> crate::Result<()> {
        super::segments::write(self, database_id, tid, collection, segment_id, data)
    }

    fn read_segment(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        segment_id: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        super::segments::read(self, database_id, tid, collection, segment_id)
    }

    fn list_segments(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> crate::Result<Vec<String>> {
        super::segments::list(self, database_id, tid, collection)
    }

    fn remove_segment(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        segment_id: &str,
    ) -> crate::Result<()> {
        super::segments::remove(self, database_id, tid, collection, segment_id)
    }

    fn purge_collection(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> crate::Result<usize> {
        super::purge::collection(self, database_id, tid, collection)
    }

    fn purge_tenant(&self, database_id: u64, tid: u64) -> crate::Result<usize> {
        super::purge::tenant(self, database_id, tid)
    }
}
