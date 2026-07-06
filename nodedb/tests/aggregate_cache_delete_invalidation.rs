// SPDX-License-Identifier: BUSL-1.1

//! A live `COUNT(*)` aggregate is cached and keyed per `(tenant, collection,
//! group_by, agg_ops)`. The cache is invalidated on INSERT/PUT, and must be
//! invalidated on DELETE too — otherwise `SELECT COUNT(*)` returns a stale,
//! too-high count after rows are removed. Covers both the single-row point
//! delete path and the predicate-driven bulk delete path.

mod common;

use common::pgwire_harness::TestServer;

async fn create_table(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION t \
             COLUMNS (id TEXT PRIMARY KEY, v INTEGER) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
}

/// A single-PK `DELETE ... WHERE id = ...` (the point-delete path) must
/// invalidate the cached `COUNT(*)`, not leave the pre-delete count cached.
#[tokio::test]
async fn count_star_reflects_point_delete_not_stale_cache() {
    let srv = TestServer::start().await;
    create_table(&srv).await;
    srv.exec("INSERT INTO t (id, v) VALUES ('a', 1), ('b', 2), ('c', 3)")
        .await
        .unwrap();

    // Warm the aggregate cache with the pre-delete count.
    let rows = srv.query_rows("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(
        rows[0].first().map(String::as_str),
        Some("3"),
        "COUNT(*) before delete must be 3, got {rows:?}"
    );

    srv.exec("DELETE FROM t WHERE id = 'b'").await.unwrap();

    let rows = srv.query_rows("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(
        rows[0].first().map(String::as_str),
        Some("2"),
        "COUNT(*) after a point delete must drop to 2, not stay stale at 3, got {rows:?}"
    );
}

/// A predicate `DELETE ... WHERE v > ...` (the bulk-delete path) must also
/// invalidate the cached `COUNT(*)`.
#[tokio::test]
async fn count_star_reflects_bulk_delete_not_stale_cache() {
    let srv = TestServer::start().await;
    create_table(&srv).await;
    srv.exec("INSERT INTO t (id, v) VALUES ('a', 1), ('b', 2), ('c', 3)")
        .await
        .unwrap();

    // Warm the aggregate cache with the pre-delete count.
    let rows = srv.query_rows("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(
        rows[0].first().map(String::as_str),
        Some("3"),
        "COUNT(*) before delete must be 3, got {rows:?}"
    );

    srv.exec("DELETE FROM t WHERE v > 1").await.unwrap();

    let rows = srv.query_rows("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(
        rows[0].first().map(String::as_str),
        Some("1"),
        "COUNT(*) after a bulk delete must drop to 1, not stay stale at 3, got {rows:?}"
    );
}
