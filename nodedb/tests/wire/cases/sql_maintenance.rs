// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for maintenance commands: ANALYZE, COMPACT, REINDEX, SHOW STORAGE.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_collection() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION metrics FIELDS (ts BIGINT, value FLOAT)")
        .await
        .unwrap();
    server.exec("ANALYZE metrics").await.unwrap();
    server.exec("ANALYZE metrics (ts)").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compact_collection() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION logs FIELDS (msg TEXT)")
        .await
        .unwrap();
    server.exec("COMPACT logs").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION users FIELDS (email TEXT)")
        .await
        .unwrap();
    server.exec("REINDEX TABLE users").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_storage_and_compaction() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION data FIELDS (val INT)")
        .await
        .unwrap();
    server.query_text("SHOW STORAGE FOR data").await.unwrap();
    server.query_text("SHOW COMPACTION STATUS").await.unwrap();
}
