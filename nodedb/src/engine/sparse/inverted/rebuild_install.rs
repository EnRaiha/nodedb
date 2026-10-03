// SPDX-License-Identifier: BUSL-1.1

//! The on-core half of a collection rebuild: the atomic cutover.
//!
//! One redb write transaction:
//!
//! 1. reads the live footprint of every document the journal noted,
//! 2. replaces the collection's `POSTINGS`, `DOC_LENGTHS`, `DOC_TERMS` and
//!    `STATS` rows with the rebuilt ones,
//! 3. writes each noted document's live footprint over the rebuilt rows,
//!    or removes the document when it is no longer indexed.
//!
//! Readers see the transaction's state before or after the commit, never a
//! mix. `INDEX_META` (analyzer, fuzzy flag, synonyms) and `SEGMENTS` are
//! left alone.

use redb::{ReadableTable as _, WriteTransaction};

use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;
use super::doc_image::FtsDocImage;
use super::errors::inverted_err;
use super::indexing::IndexDocScope;
use super::rebuild_journal::JournalState;
use super::rebuild_snapshot::FtsRebuilt;
use crate::engine::sparse::fts_redb::tables::{DOC_LENGTHS, DOC_TERMS, POSTINGS, STATS};

/// Upper bound for the `term` component of a posting range scan.
const MAX_TERM: &str = "\u{10ffff}";

/// Why a cutover left the live index as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FtsRebuildRefusal {
    /// The journal of this rebuild is gone: it was aborted, or a newer
    /// rebuild of the collection replaced it.
    #[error("the rebuild's journal is closed; a newer rebuild or an abort replaced it")]
    Superseded,
    /// The collection or its tenant was purged during the rebuild.
    #[error("the collection was purged during the rebuild")]
    Purged,
    /// More distinct documents were written during the rebuild than the
    /// journal holds.
    #[error(
        "more than {max_docs} documents were written during the rebuild; \
         run REINDEX again"
    )]
    JournalOverflow { max_docs: usize },
}

/// What a cutover did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FtsInstallOutcome {
    /// The rebuilt rows are live.
    Installed {
        /// Terms with a posting list.
        terms: usize,
        /// Documents in the rebuilt rows before replay.
        docs: usize,
        /// Documents written during the rebuild and replayed.
        replayed: usize,
    },
    /// The live index is unchanged.
    Refused(FtsRebuildRefusal),
}

impl InvertedIndex {
    /// Install `rebuilt` over the live rows of its collection, with every
    /// write the journal noted replayed on top, in one transaction.
    ///
    /// Closes the journal in every case. A refusal or an error leaves the
    /// live index as it is.
    pub fn install_rebuild(&self, rebuilt: FtsRebuilt) -> crate::Result<FtsInstallOutcome> {
        let Some(journal) = self.take_journal(rebuilt.token) else {
            return Ok(FtsInstallOutcome::Refused(FtsRebuildRefusal::Superseded));
        };
        match journal.state {
            JournalState::Recording => {}
            JournalState::Purged => {
                return Ok(FtsInstallOutcome::Refused(FtsRebuildRefusal::Purged));
            }
            JournalState::Overflowed => {
                return Ok(FtsInstallOutcome::Refused(
                    FtsRebuildRefusal::JournalOverflow {
                        max_docs: journal.max_docs,
                    },
                ));
            }
        }
        let mut touched: Vec<u32> = journal.touched.into_iter().collect();
        touched.sort_unstable();

        let tid = TenantId::new(rebuilt.tid);
        let scope_of = |surrogate: u32| IndexDocScope {
            database_id: rebuilt.database_id,
            tid,
            collection: &rebuilt.collection,
            surrogate: Surrogate::new(surrogate),
        };

        let db = self.inner.backend().db();
        let txn = db
            .begin_write()
            .map_err(|e| inverted_err("rebuild cutover txn", e))?;

        let mut live: Vec<(u32, Option<FtsDocImage>)> = Vec::with_capacity(touched.len());
        for &surrogate in &touched {
            live.push((
                surrogate,
                Self::read_document_image(&txn, scope_of(surrogate))?,
            ));
        }

        clear_collection_rows(&txn, rebuilt.database_id, rebuilt.tid, &rebuilt.collection)?;
        write_rebuilt_rows(&txn, &rebuilt)?;

        for (surrogate, image) in &live {
            match image {
                Some(image) => self.write_index_data(&txn, scope_of(*surrogate), image.tokens())?,
                None => self.remove_document_in_txn(&txn, scope_of(*surrogate))?,
            }
        }

        txn.commit()
            .map_err(|e| inverted_err("rebuild cutover commit", e))?;
        Ok(FtsInstallOutcome::Installed {
            terms: rebuilt.postings.len(),
            docs: rebuilt.doc_lengths.len(),
            replayed: live.len(),
        })
    }
}

/// Remove the collection's rows from every table the rebuild owns.
fn clear_collection_rows(
    txn: &WriteTransaction,
    database_id: u64,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    {
        let mut table = txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("rebuild open postings", e))?;
        let terms: Vec<String> = table
            .range((database_id, tid, collection, "")..=(database_id, tid, collection, MAX_TERM))
            .map_err(|e| inverted_err("rebuild postings range", e))?
            .map(|entry| entry.map(|(k, _)| k.value().3.to_string()))
            .collect::<Result<_, _>>()
            .map_err(|e| inverted_err("rebuild postings entry", e))?;
        for term in &terms {
            table
                .remove((database_id, tid, collection, term.as_str()))
                .map_err(|e| inverted_err("rebuild remove postings", e))?;
        }
    }
    for (def, name) in [(DOC_LENGTHS, "doc_lengths"), (DOC_TERMS, "doc_terms")] {
        let mut table = txn
            .open_table(def)
            .map_err(|e| inverted_err(&format!("rebuild open {name}"), e))?;
        let docs: Vec<u32> = table
            .range((database_id, tid, collection, 0u32)..=(database_id, tid, collection, u32::MAX))
            .map_err(|e| inverted_err(&format!("rebuild {name} range"), e))?
            .map(|entry| entry.map(|(k, _)| k.value().3))
            .collect::<Result<_, _>>()
            .map_err(|e| inverted_err(&format!("rebuild {name} entry"), e))?;
        for doc in docs {
            table
                .remove((database_id, tid, collection, doc))
                .map_err(|e| inverted_err(&format!("rebuild remove {name}"), e))?;
        }
    }
    let mut stats = txn
        .open_table(STATS)
        .map_err(|e| inverted_err("rebuild open stats", e))?;
    stats
        .remove((database_id, tid, collection))
        .map_err(|e| inverted_err("rebuild remove stats", e))?;
    Ok(())
}

/// Write the rebuilt rows of the collection.
fn write_rebuilt_rows(txn: &WriteTransaction, rebuilt: &FtsRebuilt) -> crate::Result<()> {
    let db = rebuilt.database_id;
    let t = rebuilt.tid;
    let coll = rebuilt.collection.as_str();
    {
        let mut table = txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("rebuild open postings", e))?;
        for (term, list) in &rebuilt.postings {
            let bytes = zerompk::to_msgpack_vec(list)
                .map_err(|e| inverted_err("rebuild serialize postings", e))?;
            table
                .insert((db, t, coll, term.as_str()), bytes.as_slice())
                .map_err(|e| inverted_err("rebuild insert postings", e))?;
        }
    }
    {
        let mut table = txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| inverted_err("rebuild open doc_lengths", e))?;
        for &(doc, len) in &rebuilt.doc_lengths {
            let bytes = zerompk::to_msgpack_vec(&len)
                .map_err(|e| inverted_err("rebuild serialize doc_length", e))?;
            table
                .insert((db, t, coll, doc), bytes.as_slice())
                .map_err(|e| inverted_err("rebuild insert doc_length", e))?;
        }
    }
    {
        let mut table = txn
            .open_table(DOC_TERMS)
            .map_err(|e| inverted_err("rebuild open doc_terms", e))?;
        for (doc, terms) in &rebuilt.doc_terms {
            let bytes = zerompk::to_msgpack_vec(terms)
                .map_err(|e| inverted_err("rebuild serialize doc_terms", e))?;
            table
                .insert((db, t, coll, *doc), bytes.as_slice())
                .map_err(|e| inverted_err("rebuild insert doc_terms", e))?;
        }
    }
    let mut stats = txn
        .open_table(STATS)
        .map_err(|e| inverted_err("rebuild open stats", e))?;
    let bytes = zerompk::to_msgpack_vec(&(rebuilt.doc_count, rebuilt.total_tokens))
        .map_err(|e| inverted_err("rebuild serialize stats", e))?;
    stats
        .insert((db, t, coll), bytes.as_slice())
        .map_err(|e| inverted_err("rebuild insert stats", e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use redb::{Database, ReadableDatabase as _};

    use nodedb_fts::FtsSearchParams;
    use nodedb_fts::posting::QueryMode;

    use super::*;
    use crate::engine::sparse::inverted::FTS_REBUILD_JOURNAL_MAX_DOCS;

    const DB: u64 = 0;
    const T: TenantId = TenantId::new(1);

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(crate::engine::durability_gate::GatedDatabase::new(
            Database::create(&path).unwrap(),
        ));
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    fn hits(idx: &InvertedIndex, query: &str) -> Vec<u32> {
        let mut ids: Vec<u32> = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query,
                    top_k: 100,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap()
            .into_iter()
            .map(|r| r.doc_id.as_u32())
            .collect();
        ids.sort_unstable();
        ids
    }

    fn rebuild_with(idx: &InvertedIndex, during: impl FnOnce(&InvertedIndex)) -> FtsInstallOutcome {
        let ticket = idx
            .begin_rebuild(DB, T, "docs", FTS_REBUILD_JOURNAL_MAX_DOCS)
            .unwrap();
        during(idx);
        let rebuilt = ticket.read().unwrap().compact();
        idx.install_rebuild(rebuilt).unwrap()
    }

    #[test]
    fn writes_during_the_rebuild_survive_the_cutover() {
        let (idx, _dir) = open_temp();
        for i in 1..=3u32 {
            idx.index_document(DB, T, "docs", Surrogate::new(i), "alpha")
                .unwrap();
        }
        let outcome = rebuild_with(&idx, |idx| {
            idx.index_document(DB, T, "docs", Surrogate::new(4), "alpha")
                .unwrap();
            idx.index_document(DB, T, "docs", Surrogate::new(1), "beta")
                .unwrap();
            idx.remove_document(DB, T, "docs", Surrogate::new(2))
                .unwrap();
        });
        assert!(matches!(
            outcome,
            FtsInstallOutcome::Installed { replayed: 3, .. }
        ));
        assert_eq!(hits(&idx, "alpha"), vec![3, 4]);
        assert_eq!(hits(&idx, "beta"), vec![1]);
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 3);
        assert_eq!(avg_len, 1.0);
    }

    #[test]
    fn a_term_dropped_during_the_rebuild_leaves_its_posting_list() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), "alpha")
            .unwrap();
        let outcome = rebuild_with(&idx, |idx| {
            // The snapshot holds document 1 under `alpha`; the update drops it.
            idx.index_document(DB, T, "docs", Surrogate::new(1), "bravo charlie")
                .unwrap();
        });
        assert!(matches!(
            outcome,
            FtsInstallOutcome::Installed { replayed: 1, .. }
        ));
        let alpha = idx
            .backend()
            .db()
            .begin_read()
            .unwrap()
            .open_table(POSTINGS)
            .unwrap()
            .get((DB, T.as_u64(), "docs", "alpha"))
            .unwrap()
            .map(|v| zerompk::from_msgpack::<Vec<nodedb_fts::posting::Posting>>(v.value()).unwrap())
            .unwrap_or_default();
        assert_eq!(
            alpha.iter().map(|p| p.doc_id.as_u32()).collect::<Vec<_>>(),
            vec![2],
            "the rebuilt `alpha` list must not keep the updated document"
        );
        assert_eq!(idx.term_df(DB, T, "docs", "alpha").unwrap(), 1);
        assert_eq!(hits(&idx, "alpha"), vec![2]);
        assert_eq!(hits(&idx, "charlie"), vec![1]);
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 2, "stats count each document once");
        assert_eq!(avg_len, 1.5, "(2 + 1) tokens over 2 documents");
    }

    #[test]
    fn a_purge_during_the_rebuild_refuses_the_cutover() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha")
            .unwrap();
        let outcome = rebuild_with(&idx, |idx| {
            idx.purge_collection(DB, T, "docs").unwrap();
        });
        assert_eq!(
            outcome,
            FtsInstallOutcome::Refused(FtsRebuildRefusal::Purged)
        );
        assert!(hits(&idx, "alpha").is_empty(), "the purge is not undone");
    }

    #[test]
    fn journal_overflow_refuses_the_cutover_and_keeps_live_writes() {
        let (idx, _dir) = open_temp();
        let ticket = idx.begin_rebuild(DB, T, "docs", 1).unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), "alpha")
            .unwrap();
        let outcome = idx
            .install_rebuild(ticket.read().unwrap().compact())
            .unwrap();
        assert_eq!(
            outcome,
            FtsInstallOutcome::Refused(FtsRebuildRefusal::JournalOverflow { max_docs: 1 })
        );
        assert_eq!(hits(&idx, "alpha"), vec![1, 2]);
    }

    #[test]
    fn a_second_rebuild_of_the_collection_is_refused_while_one_runs() {
        let (idx, _dir) = open_temp();
        let _ticket = idx
            .begin_rebuild(DB, T, "docs", FTS_REBUILD_JOURNAL_MAX_DOCS)
            .unwrap();
        assert!(
            idx.begin_rebuild(DB, T, "docs", FTS_REBUILD_JOURNAL_MAX_DOCS)
                .is_err()
        );
    }
}
