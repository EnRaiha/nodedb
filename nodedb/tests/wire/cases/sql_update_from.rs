// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for `UPDATE target SET ... FROM src WHERE target.id = src.id`.
//!
//! Each test spins up a fresh single-core server and exercises the UPDATE...FROM
//! path end-to-end through pgwire.

use crate::harness::TestServer;

// ---------------------------------------------------------------------------
// Seed helpers
// ---------------------------------------------------------------------------

async fn setup_target(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION uf_target (\
                id TEXT PRIMARY KEY, \
                name TEXT, \
                score INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    for (id, name, score) in [("a", "alpha", 10), ("b", "beta", 20), ("c", "gamma", 30)] {
        server
            .exec(&format!(
                "INSERT INTO uf_target (id, name, score) VALUES ('{id}', '{name}', {score})"
            ))
            .await
            .unwrap();
    }
}

async fn setup_source(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION uf_source (\
                id TEXT PRIMARY KEY, \
                new_name TEXT, \
                bonus INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    // Only 'a' and 'b' have source rows — 'c' should NOT be updated.
    server
        .exec("INSERT INTO uf_source (id, new_name, bonus) VALUES ('a', 'ALPHA_NEW', 100)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO uf_source (id, new_name, bonus) VALUES ('b', 'BETA_NEW', 200)")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Test 1: Basic update from join — SET value comes from source row
// ---------------------------------------------------------------------------

#[tokio::test]
async fn basic_update_from_join() {
    let server = TestServer::start().await;
    setup_target(&server).await;
    setup_source(&server).await;

    server
        .exec(
            "UPDATE uf_target SET name = uf_source.new_name \
             FROM uf_source \
             WHERE uf_target.id = uf_source.id",
        )
        .await
        .expect("UPDATE ... FROM should succeed");

    // Verify target rows were updated.
    let rows_a = server
        .query_rows("SELECT name FROM uf_target WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows_a.len(), 1);
    assert!(
        rows_a[0].join(",").contains("ALPHA_NEW"),
        "row 'a' name should be ALPHA_NEW, got: {:?}",
        rows_a
    );

    let rows_b = server
        .query_rows("SELECT name FROM uf_target WHERE id = 'b'")
        .await
        .unwrap();
    assert_eq!(rows_b.len(), 1);
    assert!(
        rows_b[0].join(",").contains("BETA_NEW"),
        "row 'b' name should be BETA_NEW, got: {:?}",
        rows_b
    );

    // Row 'c' has no source match — must remain unchanged.
    let rows_c = server
        .query_rows("SELECT name FROM uf_target WHERE id = 'c'")
        .await
        .unwrap();
    assert_eq!(rows_c.len(), 1);
    assert!(
        rows_c[0].join(",").contains("gamma"),
        "row 'c' name should still be gamma, got: {:?}",
        rows_c
    );
}

// ---------------------------------------------------------------------------
// Test 2: Multiple SET assignments — some from source, some constants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_assignments_mixed_source_and_literal() {
    let server = TestServer::start().await;
    setup_target(&server).await;
    setup_source(&server).await;

    // SET name from source AND score to literal 999.
    server
        .exec(
            "UPDATE uf_target SET name = uf_source.new_name, score = 999 \
             FROM uf_source \
             WHERE uf_target.id = uf_source.id",
        )
        .await
        .expect("UPDATE ... FROM with multiple SET should succeed");

    let rows = server
        .query_rows("SELECT name, score FROM uf_target WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row_str = rows[0].join(",");
    assert!(
        row_str.contains("ALPHA_NEW"),
        "name should be ALPHA_NEW: {row_str}"
    );
    assert!(row_str.contains("999"), "score should be 999: {row_str}");
}

// ---------------------------------------------------------------------------
// Test 3: WHERE clause filters out some target rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn where_clause_filters_target_rows() {
    let server = TestServer::start().await;
    setup_target(&server).await;
    setup_source(&server).await;

    // Both 'a' and 'b' have source rows, but the WHERE filter on target
    // restricts the update to only rows with score > 15 — meaning only 'b'.
    server
        .exec(
            "UPDATE uf_target SET name = uf_source.new_name \
             FROM uf_source \
             WHERE uf_target.id = uf_source.id AND uf_target.score > 15",
        )
        .await
        .expect("UPDATE ... FROM with extra target filter should succeed");

    // 'a' has score=10 which is NOT > 15 — should NOT be updated.
    let rows_a = server
        .query_rows("SELECT name FROM uf_target WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows_a.len(), 1);
    assert!(
        rows_a[0].join(",").contains("alpha"),
        "row 'a' should NOT be updated (score<=15), got: {:?}",
        rows_a
    );

    // 'b' has score=20 which IS > 15 — should be updated.
    let rows_b = server
        .query_rows("SELECT name FROM uf_target WHERE id = 'b'")
        .await
        .unwrap();
    assert_eq!(rows_b.len(), 1);
    assert!(
        rows_b[0].join(",").contains("BETA_NEW"),
        "row 'b' should be updated, got: {:?}",
        rows_b
    );
}

// ---------------------------------------------------------------------------
// Test 4: UPDATE ... FROM with RETURNING
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_from_with_returning() {
    let server = TestServer::start().await;

    // Use a schemaless collection to keep RETURNING simple.
    server
        .exec("CREATE COLLECTION ret_target TYPE DOCUMENT (id STRING, val INT)")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION ret_source TYPE DOCUMENT (id STRING, new_val INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ret_target (id, val) VALUES ('x', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ret_source (id, new_val) VALUES ('x', 42)")
        .await
        .unwrap();

    let rows = server
        .query_rows(
            "UPDATE ret_target SET val = ret_source.new_val \
             FROM ret_source \
             WHERE ret_target.id = ret_source.id \
             RETURNING *",
        )
        .await
        .expect("UPDATE ... FROM RETURNING should succeed");

    // Should return the post-update document.
    assert!(!rows.is_empty(), "RETURNING should return at least one row");
    let row_str = rows[0].join(",");
    assert!(
        row_str.contains("42"),
        "returned row should reflect updated val=42: {row_str}"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Self-join (target FROM target AS alias)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn self_join_update() {
    let server = TestServer::start().await;

    // Propagate bonus from parent row to child rows (self-join on parent_id = id).
    server
        .exec(
            "CREATE COLLECTION uf_tree (\
                id TEXT PRIMARY KEY, \
                parent_id TEXT, \
                val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO uf_tree (id, parent_id, val) VALUES ('root', '', 100)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO uf_tree (id, parent_id, val) VALUES ('child', 'root', 0)")
        .await
        .unwrap();

    // Update child.val to parent.val via self-join.
    server
        .exec(
            "UPDATE uf_tree AS t SET val = p.val \
             FROM uf_tree AS p \
             WHERE t.parent_id = p.id",
        )
        .await
        .expect("self-join UPDATE ... FROM should succeed");

    let rows = server
        .query_rows("SELECT val FROM uf_tree WHERE id = 'child'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].join(",").contains("100"),
        "child val should be propagated to 100 from parent: {:?}",
        rows
    );
}

// ---------------------------------------------------------------------------
// Test 6: Multi-table FROM is rejected with a typed error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_table_from_rejected() {
    let server = TestServer::start().await;

    // Create the tables so the planner doesn't fail on unknown collection.
    server
        .exec("CREATE COLLECTION mt_t TYPE DOCUMENT (id STRING, v INT)")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION mt_s1 TYPE DOCUMENT (id STRING, v INT)")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION mt_s2 TYPE DOCUMENT (id STRING, v INT)")
        .await
        .unwrap();

    let result = server
        .exec(
            "UPDATE mt_t SET v = mt_s1.v \
             FROM mt_s1, mt_s2 \
             WHERE mt_t.id = mt_s1.id",
        )
        .await;

    assert!(
        result.is_err(),
        "UPDATE ... FROM with multiple source tables should be rejected"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("source tables") || err.contains("not supported"),
        "error should mention unsupported multi-table FROM: {err}"
    );
}
