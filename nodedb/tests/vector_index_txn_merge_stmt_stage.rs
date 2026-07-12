// SPDX-License-Identifier: BUSL-1.1

//! In-transaction `MERGE` is resolved + staged at STATEMENT time
//! (`control::server::shared::session::expander_stage`), not expanded at COMMIT.
//!
//! Statement-time staging has two observable consequences a COMMIT-time expander
//! could never provide, both proven here:
//!
//! 1. **Read-your-own-writes.** A `SELECT` issued after the `MERGE` but still
//!    inside the same transaction sees the merge-inserted row — because the
//!    concrete `PointInsert` the MERGE expands to was staged into the
//!    transaction's overlay the moment the MERGE ran (not held as a raw `Merge`
//!    plan until COMMIT).
//! 2. **base == overlay for later statements.** A later `UPDATE` of a
//!    merge-inserted row in the same transaction resolves against an overlay that
//!    already holds the merge's post-image, so it moves the SAME surrogate the
//!    MERGE created — the shared-surrogate case a COMMIT-time expander got wrong.
//!
//! Every inserted row carries an explicit `id` primary key; no restart
//! assertions (durability of in-transaction writes is a separate unit).

mod common;

use common::pgwire_harness::TestServer;

/// Create a schemaless target collection with a secondary cosine vector index
/// plus a schemaless source collection.
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

/// Insert one schemaless source row `(id, sku, new_embedding)` (autocommit).
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

/// The NOT-MATCHED insert clause shared by both tests.
const MERGE_INSERT: &str = "WHEN NOT MATCHED THEN INSERT (id, sku, embedding) \
     VALUES (s.id, s.sku, s.new_embedding)";

/// (1) An in-transaction `SELECT` issued AFTER the `MERGE`, still inside the
/// transaction, must SEE the merge-inserted row (read-your-own-writes). This is
/// only possible because the MERGE was staged at statement time — under
/// COMMIT-time expansion the row would not exist until after COMMIT. After
/// COMMIT the row is independently searchable at its embedding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_merge_staged_visible_to_later_statement_in_same_txn() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "tss_target", "tss_source", "idx_tss").await;
    insert_source(&srv, "tss_source", "r1", "k1", [1.0, 0.0, 0.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec(&format!(
        "MERGE INTO tss_target t USING tss_source s ON t.sku = s.sku {MERGE_INSERT}"
    ))
    .await
    .unwrap();

    // Read-your-own-writes: the merge-inserted row is visible to this later
    // statement WITHIN the same transaction.
    let in_txn = srv
        .query_text("SELECT id FROM tss_target WHERE sku = 'k1'")
        .await
        .unwrap();
    assert_eq!(
        in_txn,
        vec!["r1".to_string()],
        "in-txn SELECT after MERGE must see the merge-staged row (read-your-own-writes); \
         got {in_txn:?} (impossible under COMMIT-time expansion)"
    );

    srv.exec("COMMIT").await.unwrap();

    // Post-commit the row is scannable AND searchable at its embedding.
    let scanned = srv.query_text("SELECT id FROM tss_target").await.unwrap();
    assert_eq!(
        scanned,
        vec!["r1".to_string()],
        "post-commit scan: {scanned:?}"
    );

    let near = nearest(&srv, "tss_target", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        near.first().map(String::as_str),
        Some("r1"),
        "x-axis vector search must return the merge-inserted 'r1'; got {near:?}"
    );
}

/// (2) A `MERGE` inserts `r1` (k1, x-axis), then a later `UPDATE` in the SAME
/// transaction moves it to the z-axis. After COMMIT the row is at the z-axis —
/// the UPDATE saw the merge-staged `r1` (base == overlay) and moved the SAME
/// surrogate the MERGE created. This is the shared-surrogate case a COMMIT-time
/// expander got wrong (the UPDATE would have resolved against base, seen no `r1`,
/// and been a no-op).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_merge_then_update_same_row() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "tmu_target", "tmu_source", "idx_tmu").await;
    insert_source(&srv, "tmu_source", "r1", "k1", [1.0, 0.0, 0.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec(&format!(
        "MERGE INTO tmu_target t USING tmu_source s ON t.sku = s.sku {MERGE_INSERT}"
    ))
    .await
    .unwrap();
    // The UPDATE resolves against base ∪ overlay: it sees the merge-staged 'r1'
    // and moves it to the z-axis.
    srv.exec("UPDATE tmu_target SET embedding = ARRAY[0.0,0.0,1.0,0.0] WHERE sku = 'k1'")
        .await
        .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // Exactly one row survives (the UPDATE moved 'r1', did not create a second).
    let scanned = srv.query_text("SELECT id FROM tmu_target").await.unwrap();
    assert_eq!(
        scanned,
        vec!["r1".to_string()],
        "exactly the one merge-inserted row must remain: {scanned:?}"
    );

    // The row is at the z-axis (the UPDATE saw the merge-staged row and moved
    // its surrogate); the x-axis no longer resolves to it.
    let z = nearest(&srv, "tmu_target", [0.0, 0.0, 1.0, 0.0]).await;
    assert_eq!(
        z.first().map(String::as_str),
        Some("r1"),
        "z-axis (post-UPDATE embedding) must return 'r1'; got {z:?} \
         (pre-fix: COMMIT-time expansion left the UPDATE a no-op, row stayed on x-axis)"
    );
}
