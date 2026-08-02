// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the inverted index covering indexing, search,
//! removal, fuzzy lookup, and structural tenant purge.

use std::sync::Arc;

use redb::Database;

use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::QueryMode;
use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;

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
fn index_and_search() {
    let (idx, _dir) = open_temp();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(1),
        "The quick brown fox jumps over the lazy dog",
    )
    .unwrap();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(2),
        "A fast brown dog runs across the field",
    )
    .unwrap();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(3),
        "Rust programming language for systems",
    )
    .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "brown fox",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].doc_id, Surrogate::new(1));
}

#[test]
fn search_with_stemming() {
    let (idx, _dir) = open_temp();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(1),
        "running distributed databases",
    )
    .unwrap();
    idx.index_document(DB, T, "docs", Surrogate::new(2), "the cat sat on a mat")
        .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "database distribution",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].doc_id, Surrogate::new(1));
}

#[test]
fn fuzzy_search() {
    let (idx, _dir) = open_temp();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(1),
        "distributed database systems",
    )
    .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "databse",
                top_k: 10,
                fuzzy_enabled: true,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(!results.is_empty());
    assert!(results[0].fuzzy);
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

#[test]
fn empty_query() {
    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "docs", Surrogate::new(1), "some text here")
        .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "the a is",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn collections_isolated() {
    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "col_a", Surrogate::new(1), "alpha bravo charlie")
        .unwrap();
    idx.index_document(DB, T, "col_b", Surrogate::new(1), "delta echo foxtrot")
        .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "col_a",
            FtsSearchParams {
                query: "alpha",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert_eq!(results.len(), 1);

    let results = idx
        .search(
            DB,
            T,
            "col_b",
            FtsSearchParams {
                query: "alpha",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(results.is_empty());
}

/// STATS (doc count, avg doc length) must not double-count when the SAME
/// surrogate is indexed more than once — this is exactly what happens on
/// WAL replay, which re-invokes `index_document` for already-durable
/// `FtsIndex` records (see `data/executor/wal_replay_fts.rs`). Before the
/// fix, `update_stats_in_txn` unconditionally did `count += 1; total +=
/// len` on every call, so a replayed doc was counted twice, skewing avgdl
/// and therefore every BM25 score in the collection.
#[test]
fn reindex_same_surrogate_identical_content_does_not_double_count_stats() {
    let (idx, _dir) = open_temp();

    // "alpha bravo charlie" tokenizes to 3 terms.
    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo charlie")
        .unwrap();
    let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
    assert_eq!(count, 1, "first index must count the doc once");
    assert_eq!(avg_len, 3.0, "avg doc len == the single doc's length");

    // Re-index the SAME surrogate with IDENTICAL content, simulating a WAL
    // replay of an already-durable FtsIndex record. Doc count and total
    // token sum must be unchanged (net zero), not doubled.
    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo charlie")
        .unwrap();
    let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
    assert_eq!(
        count, 1,
        "replaying an already-indexed doc must not bump doc count"
    );
    assert_eq!(
        avg_len, 3.0,
        "replaying an already-indexed doc must not change total token sum"
    );
}

/// A genuine re-index of a surrogate whose content actually changed (not a
/// replay of identical content) must still leave `count` untouched but
/// adjust `total` by the length delta, keeping avgdl correct for the new
/// content.
#[test]
fn reindex_same_surrogate_different_length_adjusts_total_by_delta() {
    let (idx, _dir) = open_temp();

    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo charlie")
        .unwrap();
    let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
    assert_eq!(count, 1);
    assert_eq!(avg_len, 3.0);

    // Same surrogate, longer content (5 tokens): count stays at 1 (still
    // one logical document), total moves to 5 (not 3 + 5 = 8).
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(1),
        "alpha bravo charlie delta echo",
    )
    .unwrap();
    let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
    assert_eq!(count, 1, "re-indexing must not create a second doc count");
    assert_eq!(
        avg_len, 5.0,
        "total must reflect the new length, not the sum of old + new"
    );
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

/// A term that an UPDATE removed from a document must leave no posting for
/// that document, and its `df` must fall back to the documents that really do
/// contain it — a stale posting would keep the document matching a word it no
/// longer has AND inflate the term's IDF for every other query.
#[test]
fn update_dropping_a_term_removes_its_posting_and_restores_df() {
    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo")
        .unwrap();
    idx.index_document(DB, T, "docs", Surrogate::new(2), "bravo charlie")
        .unwrap();
    assert_eq!(idx.term_df(DB, T, "docs", "bravo").unwrap(), 2);

    // Document 1 loses "bravo" and gains "delta".
    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha delta")
        .unwrap();

    assert_eq!(
        idx.term_df(DB, T, "docs", "bravo").unwrap(),
        1,
        "only the document that still contains the term may be counted"
    );
    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "bravo",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert_eq!(results.len(), 1, "the updated document must not match");
    assert_eq!(results[0].doc_id, Surrogate::new(2));

    // The corpus itself is unchanged: still two documents, both two tokens.
    let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
    assert_eq!(count, 2);
    assert_eq!(avg_len, 2.0);
}

/// A term the update ADDS must be indexed, so the retraction above cannot be
/// implemented by simply refusing to touch a re-indexed document.
#[test]
fn update_adding_a_term_indexes_it() {
    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha")
        .unwrap();
    assert_eq!(idx.term_df(DB, T, "docs", "delta").unwrap(), 0);

    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha delta")
        .unwrap();

    assert_eq!(idx.term_df(DB, T, "docs", "alpha").unwrap(), 1);
    assert_eq!(
        idx.term_df(DB, T, "docs", "delta").unwrap(),
        1,
        "a term introduced by the update must be searchable"
    );
}

/// Re-indexing with an UNCHANGED token set must not move any count — this is
/// the WAL-replay shape, and the retraction path must not disturb it.
#[test]
fn reindex_with_unchanged_tokens_is_a_no_op_for_counts() {
    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo charlie")
        .unwrap();
    idx.index_document(DB, T, "docs", Surrogate::new(2), "bravo")
        .unwrap();

    let before = (
        idx.corpus_stats(DB, T, "docs").unwrap(),
        idx.term_df(DB, T, "docs", "alpha").unwrap(),
        idx.term_df(DB, T, "docs", "bravo").unwrap(),
        idx.term_df(DB, T, "docs", "charlie").unwrap(),
    );

    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo charlie")
        .unwrap();

    let after = (
        idx.corpus_stats(DB, T, "docs").unwrap(),
        idx.term_df(DB, T, "docs", "alpha").unwrap(),
        idx.term_df(DB, T, "docs", "bravo").unwrap(),
        idx.term_df(DB, T, "docs", "charlie").unwrap(),
    );
    assert_eq!(before, after, "an identical re-index must change nothing");
}

/// A document indexed before term sets were recorded has no stored set. Its
/// first re-index must still retract dropped terms, via the fallback scan.
#[test]
fn update_of_a_document_without_a_stored_term_set_still_retracts() {
    use crate::engine::sparse::fts_redb::tables::DOC_TERMS;

    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo")
        .unwrap();

    // Drop the term-set row to reproduce a document written by a build that
    // did not maintain one.
    {
        let db = idx.backend().db();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(DOC_TERMS).unwrap();
            table.remove((DB, T.as_u64(), "docs", 1u32)).unwrap();
        }
        txn.commit().unwrap();
    }

    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha")
        .unwrap();

    assert_eq!(
        idx.term_df(DB, T, "docs", "bravo").unwrap(),
        0,
        "the fallback scan must retract the dropped term"
    );
    assert_eq!(idx.term_df(DB, T, "docs", "alpha").unwrap(), 1);
}

/// An update that leaves a document with no indexable text at all is the
/// extreme case of the same bug: every term dropped out, so the document must
/// leave the index entirely rather than keep matching its old words.
#[test]
fn update_to_empty_text_removes_the_document() {
    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo")
        .unwrap();
    idx.index_document(DB, T, "docs", Surrogate::new(2), "bravo")
        .unwrap();

    idx.index_document(DB, T, "docs", Surrogate::new(1), "")
        .unwrap();

    assert_eq!(idx.term_df(DB, T, "docs", "alpha").unwrap(), 0);
    assert_eq!(
        idx.term_df(DB, T, "docs", "bravo").unwrap(),
        1,
        "the other document keeps its posting"
    );
    let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
    assert_eq!(count, 1, "the emptied document is no longer in the corpus");
    assert_eq!(avg_len, 1.0);
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
