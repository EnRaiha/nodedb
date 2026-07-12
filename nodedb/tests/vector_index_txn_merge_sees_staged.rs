// SPDX-License-Identifier: BUSL-1.1

//! In-transaction visibility of the MERGE / `UPDATE ... FROM` RESOLVE pass: a
//! statement resolved at COMMIT must see rows staged by EARLIER statements in
//! the SAME transaction (read-your-own-writes for the resolve).
//!
//! A transactional `MERGE` / `UPDATE ... FROM` is buffered at statement time and
//! expanded at COMMIT into concrete point ops. The expander's RESOLVE pass runs
//! on the Data Plane and classifies the statement against the target collection.
//! Before this fix the resolve read committed BASE storage only, so it missed a
//! row inserted by an earlier statement in the same transaction (which lives in
//! the transaction's staging OVERLAY, not base). The fix folds the overlay into
//! the resolve scans (`collect_target_docs` for MERGE, `scan_target_rows` for
//! `UPDATE ... FROM`), so the resolve sees `base ∪ overlay`.
//!
//! Each test wraps the statements in one explicit transaction and gives every
//! row an explicit `id` primary key (no restart assertions here).

mod common;

use common::pgwire_harness::TestServer;

/// Create a schemaless target collection with a secondary cosine vector index
/// on `embedding`, plus a schemaless source collection.
async fn setup_target_and_source(srv: &TestServer, target: &str, source: &str, idx: &str) {
    srv.exec(&format!("CREATE COLLECTION {target} TYPE document"))
        .await
        .unwrap();
    srv.exec(&format!(
        "CREATE VECTOR INDEX {idx} ON {target} (embedding) METRIC cosine DIM 4"
    ))
    .await
    .unwrap();
    srv.exec(&format!("CREATE COLLECTION {source} TYPE document"))
        .await
        .unwrap();
}

/// Insert one source row `(id, sku, new_embedding)` (autocommit). The embedding
/// column is deliberately named differently from the target's.
async fn insert_source(srv: &TestServer, coll: &str, id: &str, sku: &str, emb: [f32; 4]) {
    srv.exec(&format!(
        "INSERT INTO {coll} (id, sku, new_embedding) VALUES \
         ('{id}', '{sku}', ARRAY[{},{},{},{}])",
        emb[0], emb[1], emb[2], emb[3]
    ))
    .await
    .unwrap();
}

/// Nearest-neighbour `id` to `axis` on the target's vector index (empty when the
/// index has no rows).
async fn nearest(srv: &TestServer, target: &str, axis: [f32; 4]) -> Vec<String> {
    srv.query_text(&format!(
        "SELECT id FROM {target} \
         ORDER BY vector_distance(embedding, ARRAY[{},{},{},{}]) LIMIT 1",
        axis[0], axis[1], axis[2], axis[3]
    ))
    .await
    .unwrap()
}

/// (1) `BEGIN; INSERT r1; MERGE ... WHEN MATCHED UPDATE WHEN NOT MATCHED INSERT;
/// COMMIT`, where the source join key equals the just-inserted row's key. The
/// MERGE must MATCH the staged 'r1' (UPDATE arm, reusing its surrogate) — NOT
/// resolve against empty base and INSERT a duplicate. After COMMIT: exactly one
/// row with sku='k1' (id='r1'), searchable at the NEW (source) embedding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_merge_sees_row_inserted_earlier_in_same_txn() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "tms_target", "tms_source", "idx_tms").await;
    // Source row joined on `sku='k1'`; its `new_embedding` sits on the z-axis.
    insert_source(&srv, "tms_source", "src1", "k1", [0.0, 0.0, 1.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    // Stage 'r1' (sku='k1') on the x-axis, in the SAME transaction as the MERGE.
    srv.exec(
        "INSERT INTO tms_target (id, sku, embedding) VALUES \
         ('r1', 'k1', ARRAY[1.0,0.0,0.0,0.0])",
    )
    .await
    .unwrap();
    srv.exec(
        "MERGE INTO tms_target t \
         USING tms_source s ON t.sku = s.sku \
         WHEN MATCHED THEN UPDATE SET embedding = s.new_embedding \
         WHEN NOT MATCHED THEN INSERT (id, sku, embedding) \
             VALUES (s.id, s.sku, s.new_embedding)",
    )
    .await
    .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // Exactly one row carries sku='k1', and it is the staged 'r1' — the MERGE
    // took the UPDATE arm against the staged row rather than inserting 'src1' as
    // a duplicate (the pre-fix behavior, which resolved against empty base).
    let k1_rows = srv
        .query_text("SELECT id FROM tms_target WHERE sku = 'k1' ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        k1_rows,
        vec!["r1".to_string()],
        "MERGE must match the staged 'r1' (one row, no 'src1' duplicate); got {k1_rows:?}"
    );

    // The row is searchable at the NEW (source) embedding: the UPDATE arm moved
    // 'r1' onto the z-axis.
    let near_z = nearest(&srv, "tms_target", [0.0, 0.0, 1.0, 0.0]).await;
    assert_eq!(
        near_z.first().map(String::as_str),
        Some("r1"),
        "z-axis (updated embedding) must return 'r1'; got {near_z:?}"
    );
}

/// (2) `BEGIN; INSERT r1; UPDATE t SET embedding = s.new_embedding FROM s WHERE
/// t.sku = s.sku; COMMIT`. The `UPDATE ... FROM` must affect the staged 'r1'
/// (its resolve reads base ∪ overlay). After COMMIT the row sits at the NEW
/// (source) embedding; pre-fix the resolve read empty base and 'r1' stayed put.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_update_from_join_sees_staged_insert() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "tus_target", "tus_source", "idx_tus").await;
    // Source row joined on `sku='k1'`; its `new_embedding` sits on the z-axis.
    insert_source(&srv, "tus_source", "src1", "k1", [0.0, 0.0, 1.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    // Stage 'r1' (sku='k1') on the x-axis, in the SAME transaction as the UPDATE.
    srv.exec(
        "INSERT INTO tus_target (id, sku, embedding) VALUES \
         ('r1', 'k1', ARRAY[1.0,0.0,0.0,0.0])",
    )
    .await
    .unwrap();
    srv.exec(
        "UPDATE tus_target t SET embedding = s.new_embedding \
         FROM tus_source s WHERE t.sku = s.sku",
    )
    .await
    .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // Exactly one target row, and it is 'r1'.
    let ids = srv
        .query_text("SELECT id FROM tus_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        ids,
        vec!["r1".to_string()],
        "target must hold only 'r1'; got {ids:?}"
    );

    // The UPDATE affected the staged row: 'r1' now sits on the z-axis (the NEW
    // source embedding). Pre-fix, the resolve read empty base, matched nothing,
    // and 'r1' would still resolve to the x-axis.
    let near_z = nearest(&srv, "tus_target", [0.0, 0.0, 1.0, 0.0]).await;
    assert_eq!(
        near_z.first().map(String::as_str),
        Some("r1"),
        "z-axis (updated embedding) must return the staged 'r1'; got {near_z:?}"
    );
}
