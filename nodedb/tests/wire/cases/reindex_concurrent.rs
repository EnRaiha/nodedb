// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for REINDEX CONCURRENTLY — shadow-build + atomic cutover
//! for HNSW, FTS LSM, and graph CSR, with non-blocking concurrent reads.

use crate::harness::TestServer;

// ── Non-concurrent backwards-compat ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex_non_concurrent_collection() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION docs FIELDS (body TEXT)")
        .await
        .unwrap();
    server.exec("REINDEX docs").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex_table_compat() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION compat_tbl FIELDS (val TEXT)")
        .await
        .unwrap();
    // REINDEX TABLE <coll> is the legacy form and must still be accepted.
    server.exec("REINDEX TABLE compat_tbl").await.unwrap();
}

// ── REINDEX CONCURRENTLY — vector collection ─────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex_concurrently_vector() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION vecs FIELDS (id TEXT, emb VECTOR(4)) WITH (engine='vector', m=8, ef_construction=50)",
        )
        .await
        .unwrap();

    // Insert a few vectors so the rebuild has something to work with.
    server
        .exec("INSERT INTO vecs (id, emb) VALUES ('v1', ARRAY[0.1, 0.2, 0.3, 0.4])")
        .await
        .unwrap();
    server
        .exec("INSERT INTO vecs (id, emb) VALUES ('v2', ARRAY[0.9, 0.8, 0.7, 0.6])")
        .await
        .unwrap();

    // Rebuild the HNSW index concurrently; reads must continue unblocked.
    server.exec("REINDEX CONCURRENTLY vecs").await.unwrap();

    // Verify data still queryable after cutover.
    let rows = server
        .query_text("SELECT id FROM vecs LIMIT 10")
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "rows should still be readable after REINDEX CONCURRENTLY"
    );
}

// ── REINDEX CONCURRENTLY — FTS collection ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex_concurrently_fts() {
    let server = TestServer::start().await;

    // FTS is an overlay on a regular document collection.
    server
        .exec("CREATE COLLECTION articles FIELDS (title TEXT, body TEXT)")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO articles { id: 'a1', title: 'hello world', body: 'the quick brown fox' }",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO articles { id: 'a2', title: 'rust lang', body: 'systems programming language' }",
        )
        .await
        .unwrap();

    server.exec("REINDEX CONCURRENTLY articles").await.unwrap();

    // Collection should still be readable after cutover.
    let rows = server
        .query_text("SELECT title FROM articles LIMIT 10")
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "collection should still be readable after REINDEX CONCURRENTLY"
    );
}

// ── REINDEX CONCURRENTLY — graph collection ───────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex_concurrently_graph() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION org_nodes (id TEXT PRIMARY KEY, parent TEXT, name TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE GRAPH INDEX edges ON org_nodes (parent -> id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO org_nodes (id, parent, name) VALUES ('alice', 'root', 'Alice')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO org_nodes (id, parent, name) VALUES ('bob', 'alice', 'Bob')")
        .await
        .unwrap();

    server.exec("REINDEX CONCURRENTLY org_nodes").await.unwrap();

    // Verify nodes are still accessible after CSR cutover.
    let rows = server
        .query_text("SELECT id FROM org_nodes LIMIT 10")
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "graph nodes should remain readable after REINDEX CONCURRENTLY"
    );
}

// ── REINDEX [INDEX name] CONCURRENTLY ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex_concurrently_named_index() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION items FIELDS (tag TEXT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO items (tag) VALUES ('alpha')")
        .await
        .unwrap();

    // Named-index form; the index may not exist but the server must not panic.
    server
        .exec("REINDEX INDEX tag_idx CONCURRENTLY items")
        .await
        .unwrap();
}

// ── Non-existent collection returns an error ─────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex_concurrently_missing_collection_error() {
    let server = TestServer::start().await;

    let result = server.exec("REINDEX CONCURRENTLY ghost_collection").await;
    assert!(
        result.is_err(),
        "REINDEX CONCURRENTLY on missing collection must return an error"
    );
}
