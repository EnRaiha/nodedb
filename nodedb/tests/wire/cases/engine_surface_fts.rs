// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Full-Text Search overlay: `text_match`
//! search, `bm25_score` projection, phrase search, fuzzy matching, and
//! AND/OR query combinations.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basic_text_match_search() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_basic WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_basic { id: 'd1', body: 'The quick brown fox' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_basic { id: 'd2', body: 'A lazy dog sleeps' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_basic { id: 'd3', body: 'Fox hunting is banned' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM fts_basic WHERE text_match(body, 'fox') ORDER BY id")
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"d1"), "d1 should match 'fox': {ids:?}");
    assert!(ids.contains(&"d3"), "d3 should match 'fox': {ids:?}");
    assert!(!ids.contains(&"d2"), "d2 should not match 'fox': {ids:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_projection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_bm25 WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_bm25 { id: 'b1', content: 'database systems architecture' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_bm25 { id: 'b2', content: 'cooking recipes and food' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_bm25 { id: 'b3', content: 'distributed database performance' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(content, 'database') AS score \
             FROM fts_bm25 \
             ORDER BY score DESC",
        )
        .await
        .unwrap();
    assert!(rows.len() >= 2, "expected at least 2 rows");
    let first_id = &rows[0][0];
    assert!(
        first_id == "b1" || first_id == "b3",
        "expected database doc first, got {first_id}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phrase_search() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_phrase WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_phrase { id: 'p1', text: 'quick brown fox jumps' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_phrase { id: 'p2', text: 'brown quick fox' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_phrase { id: 'p3', text: 'the quick brown fox' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM fts_phrase WHERE text_match(text, '\"quick brown\"') ORDER BY id",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"p1"), "p1 should match phrase: {ids:?}");
    assert!(ids.contains(&"p3"), "p3 should match phrase: {ids:?}");
    assert!(!ids.contains(&"p2"), "p2 should not match phrase: {ids:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzzy_search() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_fuzzy WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_fuzzy { id: 'f1', body: 'distributed database systems' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM fts_fuzzy WHERE text_match(body, 'databse', fuzzy => true)")
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"f1"), "fuzzy should match with typo: {ids:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn and_or_query_combinations() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_logic WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_logic { id: 'l1', body: 'rust programming language' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_logic { id: 'l2', body: 'python programming tutorial' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_logic { id: 'l3', body: 'rust systems software' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM fts_logic WHERE text_match(body, 'rust programming') ORDER BY id",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"l1"), "l1 should match: {ids:?}");
}
