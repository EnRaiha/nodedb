// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the SQL MERGE statement.
//!
//! Each test spins up a fresh single-core NodeDB server and exercises
//! MERGE end-to-end through the pgwire protocol.

use crate::harness::TestServer;

// ── Schema helpers ────────────────────────────────────────────────────────

async fn create_target(server: &TestServer, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION merge_target (\
                id TEXT PRIMARY KEY, \
                name TEXT, \
                score INT) WITH (engine='{engine}')"
        ))
        .await
        .unwrap();
}

async fn create_source(server: &TestServer, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION merge_source (\
                id TEXT PRIMARY KEY, \
                name TEXT, \
                score INT) WITH (engine='{engine}')"
        ))
        .await
        .unwrap();
}

async fn seed_target(server: &TestServer) {
    for (id, name, score) in [("a", "alpha", 10i64), ("b", "beta", 20)] {
        server
            .exec(&format!(
                "INSERT INTO merge_target (id, name, score) VALUES ('{id}', '{name}', {score})"
            ))
            .await
            .unwrap();
    }
}

async fn seed_source(server: &TestServer) {
    // 'a' and 'c': 'a' exists in target, 'c' does not.
    for (id, name, score) in [("a", "ALPHA_UPD", 99i64), ("c", "gamma", 30)] {
        server
            .exec(&format!(
                "INSERT INTO merge_source (id, name, score) VALUES ('{id}', '{name}', {score})"
            ))
            .await
            .unwrap();
    }
}

// ── Test 1: WHEN MATCHED THEN UPDATE ─────────────────────────────────────

#[tokio::test]
async fn merge_matched_update() {
    let server = TestServer::start().await;
    create_target(&server, "document_strict").await;
    create_source(&server, "document_strict").await;
    seed_target(&server).await;
    seed_source(&server).await;

    server
        .exec(
            "MERGE INTO merge_target t \
             USING merge_source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, name, score FROM merge_target ORDER BY id")
        .await
        .unwrap();

    // 'a' should be updated, 'b' should be unchanged.
    assert_eq!(rows.len(), 2, "expected 2 rows in target");
    assert_eq!(rows[0][0], "a");
    assert_eq!(rows[0][1], "ALPHA_UPD");
    assert_eq!(rows[0][2], "99");
    assert_eq!(rows[1][0], "b");
    assert_eq!(rows[1][1], "beta"); // unchanged
}

// ── Test 2: WHEN NOT MATCHED THEN INSERT ─────────────────────────────────

#[tokio::test]
async fn merge_not_matched_insert() {
    let server = TestServer::start().await;
    create_target(&server, "document_strict").await;
    create_source(&server, "document_strict").await;
    seed_target(&server).await;
    seed_source(&server).await;

    server
        .exec(
            "MERGE INTO merge_target t \
             USING merge_source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id FROM merge_target ORDER BY id")
        .await
        .unwrap();

    // 'c' should now exist (was only in source).
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"a"), "expected 'a' in target");
    assert!(ids.contains(&"b"), "expected 'b' in target");
    assert!(
        ids.contains(&"c"),
        "expected 'c' to be inserted from source"
    );
}

// ── Test 3: UPSERT — both MATCHED UPDATE and NOT MATCHED INSERT ───────────

#[tokio::test]
async fn merge_upsert_both_arms() {
    let server = TestServer::start().await;
    create_target(&server, "document_schemaless").await;
    create_source(&server, "document_schemaless").await;
    seed_target(&server).await;
    seed_source(&server).await;

    server
        .exec(
            "MERGE INTO merge_target t \
             USING merge_source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score \
             WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, name FROM merge_target ORDER BY id")
        .await
        .unwrap();

    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"a"));
    assert!(ids.contains(&"b"));
    assert!(ids.contains(&"c"), "expected 'c' inserted");

    let a = rows.iter().find(|r| r[0] == "a").unwrap();
    assert_eq!(a[1], "ALPHA_UPD", "'a' name should be updated");
}

// ── Test 4: WHEN MATCHED AND <pred> THEN DELETE ───────────────────────────

#[tokio::test]
async fn merge_matched_predicate_delete() {
    let server = TestServer::start().await;
    create_target(&server, "document_strict").await;
    create_source(&server, "document_strict").await;
    seed_target(&server).await;
    seed_source(&server).await;

    // Delete matched target rows where target.score < 50.
    // Only 'a' (score=10) is matched (in source); score < 50 → delete.
    server
        .exec(
            "MERGE INTO merge_target t \
             USING merge_source s ON t.id = s.id \
             WHEN MATCHED AND score < 50 THEN DELETE",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id FROM merge_target ORDER BY id")
        .await
        .unwrap();

    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(!ids.contains(&"a"), "'a' should be deleted");
    assert!(ids.contains(&"b"), "'b' should remain (no source match)");
}

// ── Test 5: MATCHED arm with a predicate that never fires ────────────────
//
// Uses a predicate that is always false for matched rows; the UPDATE arm
// should not apply, leaving target unchanged.

#[tokio::test]
async fn merge_matched_predicate_no_match() {
    let server = TestServer::start().await;
    create_target(&server, "document_strict").await;
    create_source(&server, "document_strict").await;
    seed_target(&server).await;
    seed_source(&server).await;

    // score < -1 is always false, so no UPDATE fires even though 'a' is matched.
    server
        .exec(
            "MERGE INTO merge_target t \
             USING merge_source s ON t.id = s.id \
             WHEN MATCHED AND score < -1 THEN UPDATE SET name = s.name",
        )
        .await
        .expect("MERGE with non-matching predicate must not error");

    let rows = server
        .query_rows("SELECT id, name, score FROM merge_target ORDER BY id")
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    // 'a' name must be unchanged.
    let a = rows.iter().find(|r| r[0] == "a").unwrap();
    assert_eq!(a[1], "alpha", "'a' should not have been updated");
    assert_eq!(a[2], "10");
}

// ── Test 6: Source is a SELECT subquery from a real table ─────────────────

#[tokio::test]
async fn merge_source_subquery() {
    let server = TestServer::start().await;
    create_target(&server, "document_strict").await;
    create_source(&server, "document_strict").await;

    // Seed source and target.
    server
        .exec("INSERT INTO merge_target (id, name, score) VALUES ('x', 'original', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO merge_source (id, name, score) VALUES ('x', 'updated', 99)")
        .await
        .unwrap();

    // Use a SELECT subquery as the MERGE source.
    server
        .exec(
            "MERGE INTO merge_target t \
             USING (SELECT id, name, score FROM merge_source WHERE score > 0) s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score",
        )
        .await
        .expect("MERGE with subquery source must not error");

    let rows = server
        .query_rows("SELECT id, name, score FROM merge_target ORDER BY id")
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], "updated");
    assert_eq!(rows[0][2], "99");
}

// ── Test 7: MERGE on KV engine — must be rejected ────────────────────────

#[tokio::test]
async fn merge_rejected_on_kv_engine() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv_tgt (id TEXT PRIMARY KEY, val TEXT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION kv_src (id TEXT PRIMARY KEY, val TEXT) WITH (engine='kv')")
        .await
        .unwrap();

    let err = server
        .exec(
            "MERGE INTO kv_tgt t \
             USING kv_src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET val = s.val",
        )
        .await
        .expect_err("MERGE on KV engine must fail");

    assert!(
        err.to_lowercase().contains("not supported")
            || err.to_lowercase().contains("unsupported")
            || err.to_lowercase().contains("merge"),
        "error message should mention unsupported MERGE on KV: {err}"
    );
}

// ── Test 8: MERGE with no WHEN arms — parse error ─────────────────────────

#[tokio::test]
async fn merge_no_when_arms_rejected() {
    let server = TestServer::start().await;
    create_target(&server, "document_strict").await;
    create_source(&server, "document_strict").await;

    // sqlparser itself may reject a bare MERGE with no WHEN clauses, or our
    // planner will. Either way we must get an error.
    let result = server
        .exec("MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id")
        .await;

    assert!(
        result.is_err(),
        "MERGE with no WHEN arms must produce an error"
    );
}
