// SPDX-License-Identifier: BUSL-1.1

//! Document removal from the inverted index.
//!
//! Removal is expressed once, in transaction terms, and the standalone entry
//! point wraps it in its own write transaction. The transactional variant is
//! what an in-flight document write calls — an update that leaves the document
//! with no indexable text, or the delete cascade removing the row that owned
//! the postings. Those paths already own the only redb writer, so they cannot
//! open a second one.

use redb::WriteTransaction;

use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;
use super::doc_terms;
use super::errors::inverted_err;
use super::indexing::{IndexDocScope, prior_doc_length};
use crate::engine::sparse::fts_redb::tables::DOC_LENGTHS;

impl InvertedIndex {
    /// Remove a document from the inverted index.
    pub fn remove_document(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
    ) -> crate::Result<()> {
        let db = self.inner.backend().db();
        let write_txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        self.remove_document_in_txn(
            &write_txn,
            IndexDocScope {
                database_id,
                tid,
                collection,
                surrogate,
            },
        )?;
        write_txn
            .commit()
            .map_err(|e| inverted_err("commit remove", e))?;
        Ok(())
    }

    /// Remove a document within an externally-owned write transaction.
    ///
    /// A document that is not in the index (no `DOC_LENGTHS` row) is left
    /// entirely alone — that is what makes a repeated delete a no-op rather
    /// than a second decrement of the corpus counters, and it is also what
    /// keeps the term-set fallback scan off the path of documents that were
    /// never indexed.
    pub fn remove_document_in_txn(
        &self,
        txn: &WriteTransaction,
        scope: IndexDocScope<'_>,
    ) -> crate::Result<()> {
        let Some(old_len) = prior_doc_length(txn, scope)? else {
            return Ok(());
        };

        // The stored term set names exactly the lists this document occupies;
        // the fallback scan covers documents indexed before term sets were
        // recorded.
        let terms = doc_terms::occupied_terms(txn, scope, true)?;
        doc_terms::strip_postings(txn, scope, &terms)?;
        doc_terms::clear(txn, scope)?;

        {
            let mut lengths = txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| inverted_err("open doc_lengths", e))?;
            lengths
                .remove((
                    scope.database_id,
                    scope.tid.as_u64(),
                    scope.collection,
                    scope.surrogate.as_u32(),
                ))
                .map_err(|e| inverted_err("remove doc length", e))?;
        }

        Self::update_stats_in_txn(
            txn,
            scope.database_id,
            scope.tid,
            scope.collection,
            -1,
            -(old_len as i64),
        )?;

        // Note: the docmap sub-key in INDEX_META (previously maintained by the
        // old DocIdMap abstraction) is no longer updated. Searches filter via
        // Surrogate prefilter bitmaps instead.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use redb::Database;

    use nodedb_fts::FtsSearchParams;
    use nodedb_fts::posting::QueryMode;

    use super::*;

    const DB: u64 = 0;
    const T: TenantId = TenantId::new(1);

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(Database::create(&path).unwrap());
        let idx = InvertedIndex::open(db).unwrap();
        (idx, dir)
    }

    #[test]
    fn remove_document() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), "hello world")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), "hello rust")
            .unwrap();

        idx.remove_document(DB, T, "docs", Surrogate::new(1))
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "hello",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, Surrogate::new(2));
    }

    /// Removing a document must decrement both doc count and total token sum
    /// by its prior length, consistent with how insert/re-index adjust STATS.
    #[test]
    fn remove_document_decrements_stats() {
        let (idx, _dir) = open_temp();

        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo charlie")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), "delta echo")
            .unwrap();
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 2);
        assert_eq!(avg_len, 2.5); // (3 + 2) / 2

        idx.remove_document(DB, T, "docs", Surrogate::new(1))
            .unwrap();
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 1, "remove must decrement doc count");
        assert_eq!(
            avg_len, 2.0,
            "remove must subtract the removed doc's length"
        );
    }
}
