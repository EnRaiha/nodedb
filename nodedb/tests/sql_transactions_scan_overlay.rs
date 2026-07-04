// SPDX-License-Identifier: BUSL-1.1

//! In-transaction SCAN reads must observe the transaction's own staged point
//! writes (read-your-own-writes for scans) by merging the per-transaction
//! staging overlay into the base scan result: staged inserts appear, staged
//! deletes disappear, and a staged update that moves a row into or out of a
//! (non-indexed) predicate is reflected.
//!
//! Every predicate here is on a NON-indexed field (`n`) or absent, so the
//! query routes through the full/predicate scan path rather than the
//! primary-key point-get or a secondary-index lookup (which are out of scope
//! for the scan-overlay merge).

mod common;

use common::pgwire_harness::TestServer;

/// Collect the single-column integer result of a scan as a sorted vec, so
/// assertions are independent of base/overlay row ordering.
async fn scan_ints(server: &TestServer, sql: &str) -> Vec<i64> {
    let mut v: Vec<i64> = server
        .query_text(sql)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
    v.sort_unstable();
    v
}

async fn setup(server: &TestServer, coll: &str, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='{engine}')"
        ))
        .await
        .unwrap();
    for (id, n) in [("a", 1), ("b", 2), ("unrelated", 100)] {
        server
            .exec(&format!("INSERT INTO {coll} (id, n) VALUES ('{id}', {n})"))
            .await
            .unwrap();
    }
}

/// In-tx INSERT then a full scan (no WHERE) must include the staged row.
async fn scan_sees_own_insert(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("INSERT INTO {coll} (id, n) VALUES ('c', 3)"))
        .await
        .unwrap();

    let seen = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        seen,
        vec![1, 2, 3, 100],
        "{engine}: in-tx scan must include the staged insert"
    );

    server.client.simple_query("COMMIT").await.unwrap();
    let after = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        after,
        vec![1, 2, 3, 100],
        "{engine}: staged insert persists"
    );
}

/// In-tx DELETE then a scan must exclude the deleted row.
async fn scan_excludes_own_delete(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    // Point delete (by PK) so the write is staged; the scan merge hides it.
    server
        .exec(&format!("DELETE FROM {coll} WHERE id = 'b'"))
        .await
        .unwrap();

    let seen = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        seen,
        vec![1, 100],
        "{engine}: in-tx scan must hide the staged delete"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        after,
        vec![1, 2, 100],
        "{engine}: ROLLBACK restores the base row"
    );
}

/// In-tx UPDATE of a non-indexed predicated field must re-evaluate the scan
/// predicate: a row can move OUT of `WHERE n = v` (updated away) and another
/// can move IN (updated to `v`).
async fn scan_reflects_predicate_move(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    // Base: only 'a' matches `n = 1`.
    let base = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 1")).await;
    assert_eq!(base, vec![1], "{engine}: base predicate match");

    server.exec("BEGIN").await.unwrap();
    // Point updates (by PK) so the writes are staged; the scan re-evaluates its
    // predicate against the staged bodies. 'a' moves OUT of `n = 1`; 'b' moves IN.
    server
        .exec(&format!("UPDATE {coll} SET n = 7 WHERE id = 'a'"))
        .await
        .unwrap();
    server
        .exec(&format!("UPDATE {coll} SET n = 1 WHERE id = 'b'"))
        .await
        .unwrap();

    let matched = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 1")).await;
    assert_eq!(
        matched,
        vec![1],
        "{engine}: predicate must re-eval staged bodies — 'b' now matches, 'a' does not"
    );

    // The row that moved out must still be visible under its new value.
    let moved_out = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 7")).await;
    assert_eq!(
        moved_out,
        vec![7],
        "{engine}: staged-updated row visible under its new predicate value"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// A scan with ORDER BY + LIMIT must return the correct merged, sorted, and
/// limited result.
async fn scan_order_by_limit(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("INSERT INTO {coll} (id, n) VALUES ('c', 3)"))
        .await
        .unwrap();
    server
        .exec(&format!("DELETE FROM {coll} WHERE id = 'a'"))
        .await
        .unwrap();

    // Ascending order over the merged set {2, 3, 100}; first two.
    let ordered: Vec<i64> = server
        .query_text(&format!("SELECT n FROM {coll} ORDER BY n ASC LIMIT 2"))
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(
        ordered,
        vec![2, 3],
        "{engine}: merged result must sort + limit correctly"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_scan_sees_own_insert() {
    scan_sees_own_insert("document_schemaless", "sc_ins").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_scan_excludes_own_delete() {
    scan_excludes_own_delete("document_schemaless", "sc_del").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_scan_reflects_predicate_move() {
    scan_reflects_predicate_move("document_schemaless", "sc_mov").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_scan_order_by_limit() {
    scan_order_by_limit("document_schemaless", "sc_lim").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_scan_sees_own_insert() {
    scan_sees_own_insert("document_strict", "st_ins").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_scan_excludes_own_delete() {
    scan_excludes_own_delete("document_strict", "st_del").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_scan_reflects_predicate_move() {
    scan_reflects_predicate_move("document_strict", "st_mov").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_scan_order_by_limit() {
    scan_order_by_limit("document_strict", "st_lim").await;
}

// ── Bitemporal current-version scan ─────────────────────────────────────────

async fn setup_bitemporal(server: &TestServer, coll: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, n INT) \
             WITH (engine='document_schemaless', bitemporal=true)"
        ))
        .await
        .unwrap();
    for (id, n) in [("a", 1), ("b", 2), ("unrelated", 100)] {
        server
            .exec(&format!("INSERT INTO {coll} (id, n) VALUES ('{id}', {n})"))
            .await
            .unwrap();
    }
}

/// On a bitemporal collection, the current-version scan must also observe the
/// transaction's staged insert / delete / update (the merge folds the overlay
/// onto the versioned current-state scan).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_scan_sees_own_writes() {
    let server = TestServer::start().await;
    let coll = "bt_scan";
    setup_bitemporal(&server, coll).await;

    server.exec("BEGIN").await.unwrap();
    // Point writes (by PK) so all three are staged; the current-version scan
    // merges them.
    server
        .exec(&format!("INSERT INTO {coll} (id, n) VALUES ('c', 3)"))
        .await
        .unwrap();
    server
        .exec(&format!("DELETE FROM {coll} WHERE id = 'b'"))
        .await
        .unwrap();
    server
        .exec(&format!("UPDATE {coll} SET n = 9 WHERE id = 'a'"))
        .await
        .unwrap();

    let seen = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        seen,
        vec![3, 9, 100],
        "bitemporal current-version scan must merge staged insert+delete+update"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        after,
        vec![1, 2, 100],
        "ROLLBACK: bitemporal scan sees base only"
    );
}
