// SPDX-License-Identifier: BUSL-1.1

//! Corpus-statistics accessors exposing the same `df` / `total_docs` /
//! `avg_doc_len` the base BM25 search reads from the durable index, so the
//! in-transaction FTS overlay merge (read-your-own-writes) can score a
//! staged, not-yet-durable document against the IDENTICAL corpus stats the
//! base search used — a staged doc must not shift the corpus itself, only
//! be scored against it.

use nodedb_fts::backend::FtsBackend;
use nodedb_types::TenantId;

use super::core::InvertedIndex;

impl InvertedIndex {
    /// Total document count and average document length for a collection,
    /// as read by the base BM25 search (`FtsIndex::index_stats`).
    pub fn corpus_stats(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<(u32, f32)> {
        self.inner
            .index_stats(database_id, tid.as_u64(), collection)
    }

    /// Document frequency (number of documents containing `term`) for a
    /// single already-analyzed term, read from the same POSTINGS table the
    /// base search scores against.
    pub fn term_df(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        term: &str,
    ) -> crate::Result<u32> {
        let postings =
            self.inner
                .backend()
                .read_postings(database_id, tid.as_u64(), collection, term)?;
        Ok(postings.len() as u32)
    }
}
