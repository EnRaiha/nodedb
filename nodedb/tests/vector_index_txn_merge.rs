// SPDX-License-Identifier: BUSL-1.1

//! In-transaction `BEGIN; MERGE ...; COMMIT` must be indexed (visible to
//! vector/FTS search) and atomic — identical to autocommit MERGE.
//!
//! A transactional MERGE is resolved + staged at STATEMENT time
//! (`control::server::shared::session::expander_stage`) into concrete
//! `PointInsert` / `PointPut` / `PointDelete` ops carrying Control-Plane-assigned
//! surrogates. Before that staging the buffered MERGE replayed through the
//! legacy Data-Plane passthrough, which wrote NOT-MATCHED inserts with NO
//! surrogate (never indexed — invisible to vector/FTS search) and ran outside
//! the COMMIT batch's undo log (not atomic with siblings / ROLLBACK).
//!
//! Restart durability of in-transaction writes is covered separately (it depends
//! on the single-shard commit journaling a replayable transaction-redo record).
//!
//! Each test wraps the MERGE in an explicit transaction and every inserted row
//! carries an explicit `id` primary key.

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

/// Insert one schemaless row `(id, sku, embedding)` into `coll` (autocommit).
async fn insert_row(srv: &TestServer, coll: &str, id: &str, sku: &str, emb: [f32; 4]) {
    srv.exec(&format!(
        "INSERT INTO {coll} (id, sku, embedding) VALUES \
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

/// (1) A `BEGIN; MERGE ... WHEN NOT MATCHED THEN INSERT; COMMIT` into a
/// vector-indexed target: after COMMIT a `vector_distance` search returns the
/// merge-inserted row (proves the inserted row got a surrogate and was indexed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_merge_insert_indexed_and_searchable() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "tmi_target", "tmi_source", "idx_tmi").await;
    insert_row(&srv, "tmi_source", "ins_x", "ins_x", [1.0, 0.0, 0.0, 0.0]).await;
    insert_row(&srv, "tmi_source", "ins_z", "ins_z", [0.0, 0.0, 1.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec(
        "MERGE INTO tmi_target t \
         USING tmi_source s ON t.sku = s.sku \
         WHEN NOT MATCHED THEN INSERT (id, sku, embedding) \
             VALUES (s.id, s.sku, s.embedding)",
    )
    .await
    .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // Both merge-inserted rows are scannable AND independently searchable.
    let scanned = srv
        .query_text("SELECT id FROM tmi_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        scanned,
        vec!["ins_x".to_string(), "ins_z".to_string()],
        "scan must return both merge-inserted rows; got {scanned:?}"
    );

    let near_x = nearest(&srv, "tmi_target", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        near_x.first().map(String::as_str),
        Some("ins_x"),
        "x-axis vector search must return the merge-inserted 'ins_x'; got {near_x:?} \
         (pre-fix: no surrogate → not in HNSW → empty)"
    );
    let near_z = nearest(&srv, "tmi_target", [0.0, 0.0, 1.0, 0.0]).await;
    assert_eq!(
        near_z.first().map(String::as_str),
        Some("ins_z"),
        "z-axis vector search must return the merge-inserted 'ins_z'; got {near_z:?}"
    );
}

/// (2) An in-transaction MERGE exercising all three arms (matched UPDATE,
/// matched DELETE, not-matched INSERT). After COMMIT the vector index reflects
/// the post-merge truth: the updated row at its NEW axis, the deleted row gone
/// (its old axis resolves to an off-axis anchor), the inserted row searchable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_merge_all_arms() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION tma_target TYPE document")
        .await
        .unwrap();
    srv.exec("CREATE VECTOR INDEX idx_tma ON tma_target (embedding) METRIC cosine DIM 4")
        .await
        .unwrap();
    srv.exec("CREATE COLLECTION tma_source TYPE document")
        .await
        .unwrap();

    // Pre-existing (autocommit) target rows. `upd1` (grp='move') starts on the
    // x-axis and moves to the w-axis; `del1` (grp='del') sits on the y-axis and
    // is removed; `anchor_y` (grp='keep') sits just off the y-axis so it becomes
    // the unique nearest neighbour once `del1` is gone.
    for (id, sku, grp, emb) in [
        ("upd1", "upd1", "move", [1.0, 0.0, 0.0, 0.0]),
        ("del1", "del1", "del", [0.0, 1.0, 0.0, 0.0]),
        ("anchor_y", "ay", "keep", [0.1, 0.85, 0.0, 0.0]),
    ] {
        srv.exec(&format!(
            "INSERT INTO tma_target (id, sku, grp, embedding) VALUES \
             ('{id}', '{sku}', '{grp}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }
    // Source rows joined on `sku`. `upd1`/`del1` match; `ins1` is unmatched.
    for (id, sku, grp, emb) in [
        ("s_upd1", "upd1", "move", [0.0, 0.0, 0.0, 1.0]),
        ("s_del1", "del1", "del", [0.0, 0.0, 0.0, 0.0]),
        ("ins1", "ins1", "keep", [0.0, 0.0, 1.0, 0.0]),
    ] {
        srv.exec(&format!(
            "INSERT INTO tma_source (id, sku, grp, new_embedding) VALUES \
             ('{id}', '{sku}', '{grp}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    srv.exec("BEGIN").await.unwrap();
    srv.exec(
        "MERGE INTO tma_target t \
         USING tma_source s ON t.sku = s.sku \
         WHEN MATCHED AND grp = 'del' THEN DELETE \
         WHEN MATCHED AND grp = 'move' THEN UPDATE SET embedding = s.new_embedding \
         WHEN NOT MATCHED THEN INSERT (id, sku, grp, embedding) \
             VALUES (s.id, s.sku, s.grp, s.new_embedding)",
    )
    .await
    .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // The deleted row is gone from the scan; the other three remain.
    let mut scanned = srv.query_text("SELECT id FROM tma_target").await.unwrap();
    scanned.sort();
    assert_eq!(
        scanned,
        vec![
            "anchor_y".to_string(),
            "ins1".to_string(),
            "upd1".to_string()
        ],
        "post-merge scan must exclude the deleted 'del1': {scanned:?}"
    );

    // INSERT arm: z-axis returns the inserted row.
    let z = nearest(&srv, "tma_target", [0.0, 0.0, 1.0, 0.0]).await;
    assert_eq!(
        z.first().map(String::as_str),
        Some("ins1"),
        "z-axis (INSERT) must return 'ins1': {z:?}"
    );
    // UPDATE arm: w-axis (the NEW embedding) returns the updated row.
    let w = nearest(&srv, "tma_target", [0.0, 0.0, 0.0, 1.0]).await;
    assert_eq!(
        w.first().map(String::as_str),
        Some("upd1"),
        "w-axis (UPDATE new embedding) must return 'upd1': {w:?}"
    );
    // DELETE arm: the y-axis now resolves to the anchor, not the removed 'del1'.
    let y = nearest(&srv, "tma_target", [0.0, 1.0, 0.0, 0.0]).await;
    assert_eq!(
        y.first().map(String::as_str),
        Some("anchor_y"),
        "y-axis (DELETE) must resolve to the anchor, not the removed 'del1': {y:?}"
    );
}

/// (3) A `BEGIN; MERGE ... INSERT; ROLLBACK` leaves NO row and NO index entry —
/// the surrogate/index writes of the expanded point ops unwind with the aborted
/// transaction (proves atomicity: the expansion rides the undo-tracked path).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_merge_rollback_leaves_nothing() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "tmr_target", "tmr_source", "idx_tmr").await;
    insert_row(&srv, "tmr_source", "ghost", "ghost", [1.0, 0.0, 0.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec(
        "MERGE INTO tmr_target t \
         USING tmr_source s ON t.sku = s.sku \
         WHEN NOT MATCHED THEN INSERT (id, sku, embedding) \
             VALUES (s.id, s.sku, s.embedding)",
    )
    .await
    .unwrap();
    srv.exec("ROLLBACK").await.unwrap();

    // No row survives the rollback.
    let scanned = srv.query_text("SELECT id FROM tmr_target").await.unwrap();
    assert!(
        scanned.is_empty(),
        "rolled-back MERGE must leave no row: {scanned:?}"
    );
    // No index entry survives either — the vector search finds nothing.
    let near = nearest(&srv, "tmr_target", [1.0, 0.0, 0.0, 0.0]).await;
    assert!(
        near.is_empty(),
        "rolled-back MERGE must leave no vector index entry: {near:?}"
    );
}
