// SPDX-License-Identifier: BUSL-1.1

//! #148 regression: after a committed transactional DELETE, the PK
//! point-lookup path and the full-scan path must agree — both must report
//! the row gone. A prior bug left the point-lookup index stale while the
//! scan (or vice versa) still reflected the deleted row.

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
