// SPDX-License-Identifier: BUSL-1.1

//! A collection's per-collection FTS analyzer (`CREATE SEARCH INDEX ...
//! ANALYZER '<name>'`) must be honored for a document staged inside an
//! open transaction, exactly as it is for an already-committed document
//! (read-your-own-writes for FTS, analyzer-consistent variant).
//!
//! Analyzer + word used to make the difference observable: the collection
//! binds the `'hindi'` analyzer (a Hindi stop-word list with no stemming
//! change on ASCII text — see `nodedb-fts`'s `NoStemAnalyzer`), and the
//! probe word is `"the"` — an English stop word. The DEFAULT analyzer (no
//! `ANALYZER` clause; English stemmer + English stop words) strips `"the"`
//! from both the indexed document and the query, so it can never match.
//! The `'hindi'` analyzer's stop-word list has no English entries, so
//! `"the"` survives tokenization and IS a real, matchable term once the
//! collection is bound to it.
//!
//! Pre-fix, the in-transaction staged-write overlay
//! (`fts_merge.rs`/`fts_score.rs`) and the forward-indexing write path
//! (`index_document_in_txn`) always tokenized with the DEFAULT analyzer,
//! ignoring the collection's bound analyzer. So a staged document
//! containing `"the"` had it stripped at staging time, AND the in-transaction
//! query's own term analysis also stripped `"the"` — the staged doc was
//! unmatchable by any query built around it. Fixed, both resolve through
//! `InvertedIndex::analyze_for_collection`, matching the collection's bound
//! analyzer.

mod common;

use common::pgwire_harness::TestServer;

/// Create a collection with FTS bound to the `'hindi'` analyzer.
async fn create_hindi_analyzer_collection(server: &TestServer, coll: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE SEARCH INDEX idx_{coll}_fts ON {coll} FIELDS body ANALYZER 'hindi'"
        ))
        .await
        .unwrap();
}

/// Create a sibling collection with NO analyzer bound (default: English
/// stemmer + English stop words) — used only to demonstrate that `"the"`
/// is unmatchable under the default analyzer, confirming the two analyzers
/// are observably different.
async fn create_default_analyzer_collection(server: &TestServer, coll: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE SEARCH INDEX idx_{coll}_fts ON {coll} FIELDS body"
        ))
        .await
        .unwrap();
}

async fn matched_ids(server: &TestServer, coll: &str, term: &str) -> Vec<String> {
    let rows = server
        .query_rows(&format!(
            "SELECT id FROM {coll} WHERE text_match(body, '{term}') ORDER BY id"
        ))
        .await
        .unwrap();
    rows.into_iter().map(|r| r[0].clone()).collect()
}

/// The default analyzer strips the English stop word `"the"` from both the
/// document and the query, so a committed document containing it is never
/// matchable by a `"the"` query. Confirms the two analyzers genuinely
/// differ in observable behavior on the same word.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_analyzer_never_matches_stop_word() {
    let server = TestServer::start().await;
    create_default_analyzer_collection(&server, "fts_an_default").await;
    server
        .exec("INSERT INTO fts_an_default (id, body) VALUES ('d1', 'the toy shop opens early')")
        .await
        .unwrap();

    let matches = matched_ids(&server, "fts_an_default", "the").await;
    assert!(
        matches.is_empty(),
        "default analyzer must strip the English stop word 'the' from both \
         document and query, so it can never match: {matches:?}"
    );
}

/// Sanity: a COMMITTED document in the `'hindi'`-bound collection is
/// matchable by `"the"` — the analyzer binding is applied at index-write
/// time, not just at query time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_doc_matches_under_bound_analyzer() {
    let server = TestServer::start().await;
    create_hindi_analyzer_collection(&server, "fts_an_committed").await;
    server
        .exec(
            "INSERT INTO fts_an_committed (id, body) VALUES ('base1', 'the toy shop opens early')",
        )
        .await
        .unwrap();

    let matches = matched_ids(&server, "fts_an_committed", "the").await;
    assert_eq!(
        matches,
        vec!["base1".to_string()],
        "a committed document must be tokenized with the collection's bound \
         'hindi' analyzer, which keeps the English stop word 'the' as a \
         real term: {matches:?}"
    );
}

/// The core RYOW-analyzer assertion: a document staged inside an open
/// transaction, in a collection bound to a non-default analyzer, must be
/// matchable by the SAME query the committed path would match against —
/// before COMMIT.
///
/// Pre-fix: the staged write path and the staged-overlay query-term
/// analysis both used the default analyzer, stripping "the" on both sides,
/// so `in_txn` would come back WITHOUT `new1` (this is the assertion that
/// fails pre-fix).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_insert_matches_under_bound_analyzer_in_txn() {
    let server = TestServer::start().await;
    create_hindi_analyzer_collection(&server, "fts_an_txn").await;
    server
        .exec("INSERT INTO fts_an_txn (id, body) VALUES ('base1', 'the toy shop opens early')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO fts_an_txn (id, body) VALUES ('new1', 'the elephant never forgets')")
        .await
        .unwrap();

    let in_txn = matched_ids(&server, "fts_an_txn", "the").await;
    assert_eq!(
        in_txn,
        vec!["base1".to_string(), "new1".to_string()],
        "in-tx search must match the staged insert under the collection's \
         bound 'hindi' analyzer (which keeps the stop word 'the' as a real \
         term), exactly as it matches the already-committed doc: {in_txn:?}"
    );

    server.client.simple_query("COMMIT").await.unwrap();
    let after_commit = matched_ids(&server, "fts_an_txn", "the").await;
    assert_eq!(
        after_commit,
        vec!["base1".to_string(), "new1".to_string()],
        "committed insert stays visible to search under the bound analyzer: {after_commit:?}"
    );
}

/// ROLLBACK must leave no durable trace of the staged insert — the analyzer
/// fix must not change transactional visibility semantics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_insert_rollback_removes_match() {
    let server = TestServer::start().await;
    create_hindi_analyzer_collection(&server, "fts_an_rb").await;
    server
        .exec("INSERT INTO fts_an_rb (id, body) VALUES ('base1', 'the toy shop opens early')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO fts_an_rb (id, body) VALUES ('new2', 'the giraffe is tall')")
        .await
        .unwrap();

    let in_txn = matched_ids(&server, "fts_an_rb", "the").await;
    assert_eq!(
        in_txn,
        vec!["base1".to_string(), "new2".to_string()],
        "in-tx search must include the staged insert before ROLLBACK: {in_txn:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after_rollback = matched_ids(&server, "fts_an_rb", "the").await;
    assert_eq!(
        after_rollback,
        vec!["base1".to_string()],
        "ROLLBACK must leave no durable index trace of the staged insert: {after_rollback:?}"
    );
}
