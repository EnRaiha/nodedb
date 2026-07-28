// SPDX-License-Identifier: BUSL-1.1

//! Regression: after a committed transactional DELETE, the PK
//! point-lookup path and the full-scan path must agree — both must report
//! the row gone. A prior bug left the point-lookup index stale while the
//! scan (or vice versa) still reflected the deleted row.
//!
//! The same "point-lookup and scan must agree" invariant is exercised here
//! along two further axes that a divergence bug exposes:
//!   * operand order — `id = 'x'` is answered by a primary-key point-lookup
//!     (`SqlPlan::PointGet`) while `'x' = id` stays a sequential scan; the two
//!     spellings are logically identical and must never select operators that
//!     disagree;
//!   * a restart — a scan-visible row that no point-lookup (nor `COUNT(*)`)
//!     can reach is a ghost tuple, the failure mode where a restart leaves the
//!     PK index diverged from the stored tuples.

mod common;

use common::pgwire_harness::TestServer;

/// Assert that both a PK point-lookup (`WHERE id = ...`) and a full scan
/// (`COUNT(*)`) agree that the given id is absent.
async fn assert_point_and_scan_agree_absent(server: &TestServer, table: &str, id: &str) {
    let point = server
        .query_text(&format!("SELECT id FROM {table} WHERE id = '{id}'"))
        .await
        .unwrap();
    assert!(
        point.is_empty(),
        "PK point-lookup must not see the deleted row '{id}', got {point:?}"
    );

    let scan = server
        .query_rows(&format!("SELECT id FROM {table}"))
        .await
        .unwrap();
    assert!(
        scan.iter().all(|row| row[0] != id),
        "full scan must not see the deleted row '{id}', got {scan:?}"
    );

    let count = server
        .query_text(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap();
    assert_eq!(
        count,
        vec![scan.len().to_string()],
        "COUNT(*) must agree with the scan row count"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_tx_delete_agrees_between_point_lookup_and_scan() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION pk_scan (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();

    // Seed two rows so the scan path has other rows to compare against.
    server
        .exec("INSERT INTO pk_scan (id, val) VALUES ('x', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO pk_scan (id, val) VALUES ('keep', 2)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DELETE FROM pk_scan WHERE id = 'x'")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    assert_point_and_scan_agree_absent(&server, "pk_scan", "x").await;

    // The untouched row must still be visible on both paths.
    let point_keep = server
        .query_text("SELECT id FROM pk_scan WHERE id = 'keep'")
        .await
        .unwrap();
    assert_eq!(
        point_keep.len(),
        1,
        "untouched row must still be visible via point-lookup, got {point_keep:?}"
    );
    let scan_keep = server.query_rows("SELECT id FROM pk_scan").await.unwrap();
    assert_eq!(
        scan_keep.len(),
        1,
        "scan must show exactly the one remaining row, got {scan_keep:?}"
    );
}

/// A committed row must be reachable identically whether the primary-key
/// equality is written column-first (`id = 'x'`) or literal-first
/// (`'x' = id`). These spellings are logically identical, but the planner
/// answers the column-first form with a primary-key point-lookup
/// (`SqlPlan::PointGet`) and the literal-first form with a sequential scan —
/// two different physical operators. They must never disagree: a row the
/// scan can see must also be reachable through the point-lookup, regardless
/// of operand order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn equality_operand_order_agrees_on_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_order (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO pk_order (id, val) VALUES ('smoke_32ea5f88', 1)")
        .await
        .unwrap();

    let column_first = server
        .query_text("SELECT id FROM pk_order WHERE id = 'smoke_32ea5f88'")
        .await
        .unwrap();
    let literal_first = server
        .query_text("SELECT id FROM pk_order WHERE 'smoke_32ea5f88' = id")
        .await
        .unwrap();

    assert_eq!(
        column_first,
        vec!["smoke_32ea5f88".to_string()],
        "column-first `id = 'x'` must match the committed row via the point-lookup path"
    );
    assert_eq!(
        literal_first, column_first,
        "literal-first `'x' = id` must return the identical row set as `id = 'x'`; \
         operand order must not route the same predicate to a physical operator that disagrees"
    );
}

/// Operand-order agreement must also hold in the negative: a primary key
/// that matches no row returns empty through both spellings.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn equality_operand_order_agrees_when_absent() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_order_absent (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO pk_order_absent (id, val) VALUES ('present', 1)")
        .await
        .unwrap();

    let column_first = server
        .query_text("SELECT id FROM pk_order_absent WHERE id = 'absent'")
        .await
        .unwrap();
    let literal_first = server
        .query_text("SELECT id FROM pk_order_absent WHERE 'absent' = id")
        .await
        .unwrap();

    assert!(
        column_first.is_empty(),
        "column-first `id = 'absent'` must not match any row, got {column_first:?}"
    );
    assert_eq!(
        literal_first, column_first,
        "literal-first `'absent' = id` must also return empty for an absent key"
    );
}

/// The primary-key point-lookup index and the sequential-scan surface must
/// stay in agreement across a restart. The reporter's ghost tuples arose
/// after restart cycles left the PK index diverged from the stored tuples:
/// the scan returned rows the point-lookup (and `COUNT(*)`) could not reach.
/// After a clean shutdown and WAL-replay reopen, every scanned row must
/// remain reachable by its own primary-key point-lookup, and `COUNT(*)` must
/// equal the scanned row count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_lookup_scan_and_count_agree_after_restart() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_restart (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    for i in 0..7u32 {
        server
            .exec(&format!(
                "INSERT INTO pk_restart (id, val) VALUES ('row_{i}', {i})"
            ))
            .await
            .unwrap();
    }

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let scan = server
        .query_rows("SELECT id FROM pk_restart")
        .await
        .unwrap();
    assert_eq!(
        scan.len(),
        7,
        "all 7 rows must survive the restart, got {scan:?}"
    );

    // Every scan-visible row must be reachable via its own PK point-lookup —
    // a scan-visible row unreachable by point-lookup is a ghost tuple.
    for row in &scan {
        let id = &row[0];
        let point = server
            .query_text(&format!("SELECT id FROM pk_restart WHERE id = '{id}'"))
            .await
            .unwrap();
        assert_eq!(
            point,
            vec![id.clone()],
            "scanned row '{id}' must be reachable by primary-key point-lookup after restart"
        );
    }

    let count = server
        .query_text("SELECT COUNT(*) FROM pk_restart")
        .await
        .unwrap();
    assert_eq!(
        count,
        vec![scan.len().to_string()],
        "COUNT(*) must equal the scanned row count after restart, got {count:?}"
    );
}
