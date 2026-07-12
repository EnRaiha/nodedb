// SPDX-License-Identifier: BUSL-1.1

//! In-transaction `UPDATE ... FROM <source>` is resolved + staged at STATEMENT
//! time (`control::server::shared::session::expander_stage`), not expanded at
//! COMMIT.
//!
//! Statement-time staging has an observable consequence a COMMIT-time expander
//! could never provide, proven here: **read-your-own-writes**. A `SELECT` (or any
//! later statement) issued after the `UPDATE ... FROM` but still inside the same
//! transaction sees the row at its NEW post-image — because the concrete
//! `PointPut` the update expands to was staged into the transaction's overlay the
//! moment the `UPDATE ... FROM` ran (not held as a raw `UpdateFromJoin` plan until
//! COMMIT).
//!
//! Every row carries an explicit `id` primary key; no restart assertions
//! (durability of in-transaction writes is a separate unit).

mod common;

use common::pgwire_harness::TestServer;

/// Create a schemaless target collection with a secondary cosine vector index on
/// `embedding`, plus a schemaless source collection.
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

/// Insert one target row `(id, sku, embedding)` (autocommit).
async fn insert_target(srv: &TestServer, coll: &str, id: &str, sku: &str, emb: [f32; 4]) {
    srv.exec(&format!(
        "INSERT INTO {coll} (id, sku, embedding) VALUES \
         ('{id}', '{sku}', ARRAY[{},{},{},{}])",
        emb[0], emb[1], emb[2], emb[3]
    ))
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

/// The `UPDATE ... FROM` shared by both tests: copy the source's `new_embedding`
/// into the target's `embedding`, joined on `sku`.
fn update_from(target: &str, source: &str) -> String {
    format!(
        "UPDATE {target} SET embedding = s.new_embedding \
         FROM {source} s WHERE {target}.sku = s.sku"
    )
}

/// (1) An in-transaction `SELECT` issued AFTER the `UPDATE ... FROM`, still inside
/// the transaction, must SEE the row at its NEW embedding (read-your-own-writes).
/// This is only possible because the `UPDATE ... FROM` was staged at statement
/// time — under COMMIT-time expansion the row would still hold its OLD embedding
/// until after COMMIT. After COMMIT the row is independently searchable at its
/// new embedding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_update_from_join_staged_visible_to_later_statement() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "ufs_target", "ufs_source", "idx_ufs").await;
    // `r1` starts on the x-axis; `anchor_x` sits just off the x-axis so that once
    // `r1` moves, an x-axis search resolves to the anchor rather than `r1`.
    insert_target(&srv, "ufs_target", "r1", "k1", [1.0, 0.0, 0.0, 0.0]).await;
    insert_target(&srv, "ufs_target", "anchor_x", "ax", [0.9, 0.1, 0.0, 0.0]).await;
    // Source row joined on `sku`; its `new_embedding` sits on the z-axis.
    insert_source(&srv, "ufs_source", "s_r1", "k1", [0.0, 0.0, 1.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec(&update_from("ufs_target", "ufs_source"))
        .await
        .unwrap();

    // Read-your-own-writes: the updated post-image is visible to this later
    // statement WITHIN the same transaction. A z-axis search returns 'r1' — its
    // NEW embedding — proving the `UPDATE ... FROM` was staged at statement time.
    let in_txn = nearest(&srv, "ufs_target", [0.0, 0.0, 1.0, 0.0]).await;
    assert_eq!(
        in_txn.first().map(String::as_str),
        Some("r1"),
        "in-txn search after UPDATE ... FROM must see 'r1' at its NEW (z-axis) \
         embedding (read-your-own-writes); got {in_txn:?} \
         (impossible under COMMIT-time expansion)"
    );

    srv.exec("COMMIT").await.unwrap();

    // Post-commit the row is independently searchable at its new embedding, and an
    // x-axis search resolves to the anchor (proving 'r1' left the x-axis).
    let near_new = nearest(&srv, "ufs_target", [0.0, 0.0, 1.0, 0.0]).await;
    assert_eq!(
        near_new.first().map(String::as_str),
        Some("r1"),
        "post-commit z-axis search must return 'r1'; got {near_new:?}"
    );
    let near_old = nearest(&srv, "ufs_target", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        near_old.first().map(String::as_str),
        Some("anchor_x"),
        "post-commit x-axis search must resolve to 'anchor_x', not the moved 'r1': {near_old:?}"
    );
}

/// (2) After the `UPDATE ... FROM`, a follow-up in-transaction `UPDATE ... WHERE`
/// observes the staged post-image and moves the SAME row again. Both writes
/// resolve against base ∪ overlay, so the second update sees the first's effect
/// (base == overlay) and moves the SAME surrogate — impossible if the
/// `UPDATE ... FROM` were held as a raw plan until COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_update_from_join_then_read_same_txn() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "uft_target", "uft_source", "idx_uft").await;
    insert_target(&srv, "uft_target", "r1", "k1", [1.0, 0.0, 0.0, 0.0]).await;
    // Source row moves 'r1' onto the z-axis.
    insert_source(&srv, "uft_source", "s_r1", "k1", [0.0, 0.0, 1.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec(&update_from("uft_target", "uft_source"))
        .await
        .unwrap();
    // A follow-up in-txn UPDATE resolves against base ∪ overlay: it sees the
    // staged 'r1' (post `UPDATE ... FROM`) and moves it onto the w-axis.
    srv.exec("UPDATE uft_target SET embedding = ARRAY[0.0,0.0,0.0,1.0] WHERE sku = 'k1'")
        .await
        .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // Exactly one row survives (the follow-up UPDATE moved 'r1', did not create a
    // second).
    let scanned = srv.query_text("SELECT id FROM uft_target").await.unwrap();
    assert_eq!(
        scanned,
        vec!["r1".to_string()],
        "exactly the one row must remain: {scanned:?}"
    );

    // The row is at the w-axis (the follow-up UPDATE saw the staged row and moved
    // its surrogate); neither the original x-axis nor the intermediate z-axis
    // resolves to it as its final position.
    let w = nearest(&srv, "uft_target", [0.0, 0.0, 0.0, 1.0]).await;
    assert_eq!(
        w.first().map(String::as_str),
        Some("r1"),
        "w-axis (final embedding) must return 'r1'; got {w:?} \
         (pre-fix: the follow-up UPDATE would have resolved against a base that \
         still held the x-axis 'r1')"
    );
}
