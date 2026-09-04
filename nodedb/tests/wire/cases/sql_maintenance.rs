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

/// Row count `SHOW STORAGE` reports for `auto_stats`, or `0` when no ANALYZE
/// has stored statistics yet.
async fn auto_stats_row_count(server: &TestServer) -> u64 {
    let rows = server
        .query_named_rows("SHOW STORAGE FOR auto_stats")
        .await
        .unwrap();
    rows.first()
        .and_then(|row| row.get("row_count"))
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Writes past the auto-ANALYZE threshold refresh column statistics without
/// an explicit `ANALYZE`.
///
/// The server boots with `auto_analyze_min_mutations = 20`, and the counter
/// takes one increment per dispatched statement, so 20 single-row inserts
/// trip the floor. The refresh runs in the background, so the assertion polls
/// `SHOW STORAGE` instead of waiting a fixed duration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_analyze_refreshes_stats_after_threshold_writes() {
    const THRESHOLD_WRITES: u64 = 20;
    let server = TestServer::start_with_auto_analyze_threshold(THRESHOLD_WRITES).await;

    server
        .exec("CREATE COLLECTION auto_stats FIELDS (id BIGINT, value BIGINT)")
        .await
        .unwrap();

    assert_eq!(
        auto_stats_row_count(&server).await,
        0,
        "a collection with no ANALYZE reports no rows"
    );

    for i in 0..THRESHOLD_WRITES {
        server
            .exec(&format!("INSERT INTO auto_stats VALUES ({i}, {i})"))
            .await
            .unwrap();
    }

    let mut observed = 0;
    for _ in 0..200 {
        observed = auto_stats_row_count(&server).await;
        if observed > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(
        observed >= THRESHOLD_WRITES,
        "auto-ANALYZE records the scanned row count, got {observed}"
    );
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

/// Maintenance statements resolve a quoted mixed-case collection.
///
/// A quoted identifier keeps its case in the catalog. A handler that reads the
/// name out of the raw statement must strip the quotes and preserve that case,
/// or every lookup here fails with `does not exist`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_resolves_quoted_mixed_case_collection() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION \"MiXeD\" FIELDS (ts BIGINT, value FLOAT)")
        .await
        .unwrap();

    server.exec("COMPACT \"MiXeD\"").await.unwrap();
    server.exec("ANALYZE \"MiXeD\"").await.unwrap();
    server.exec("ANALYZE \"MiXeD\" (ts)").await.unwrap();
    server.exec("REINDEX \"MiXeD\"").await.unwrap();

    let rows = server
        .query_named_rows("SHOW STORAGE FOR \"MiXeD\"")
        .await
        .unwrap();
    assert_eq!(
        rows.first().and_then(|row| row.get("collection")),
        Some(&"MiXeD".to_string()),
        "SHOW STORAGE reports the stored name"
    );
}

/// A lowercased spelling of a quoted mixed-case name names no collection.
///
/// This pins the direction of the convention: `COMPACT mixed` must not reach
/// the collection stored as `MiXeD`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_rejects_lowercased_spelling_of_quoted_collection() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION \"CaseD\" FIELDS (ts BIGINT)")
        .await
        .unwrap();

    assert!(
        server.exec("COMPACT cased").await.is_err(),
        "a bare token lowercases and names a different collection"
    );
    assert!(
        server.exec("ANALYZE cased").await.is_err(),
        "a bare token lowercases and names a different collection"
    );
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
