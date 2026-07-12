// SPDX-License-Identifier: BUSL-1.1

//! In-transaction `BEGIN; UPDATE t SET ... FROM s WHERE ...; COMMIT` into a
//! vector-indexed target must be indexed (visible to vector search) and atomic —
//! identical to autocommit `UPDATE ... FROM`.
//!
//! A transactional `UPDATE ... FROM` is resolved + staged at STATEMENT time
//! (`control::server::shared::session::expander_stage`) into concrete `PointPut`
//! ops carrying each target row's existing surrogate, buffered for COMMIT's
//! durable replay. Before that expansion the buffered update replayed through the
//! legacy Data-Plane passthrough, whose `sparse.put` ran in its own redb txn
//! OUTSIDE the COMMIT batch's undo log — not atomic with siblings / ROLLBACK.
//!
//! Restart durability of in-transaction writes is covered separately (it depends
//! on the single-shard commit journaling a replayable transaction-redo record).
//!
//! Each test wraps the UPDATE in an explicit transaction; the source carries a
//! differently-named embedding column (`new_embedding`) copied into the target's
//! `embedding` field. Every row carries an explicit `id` primary key.

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

/// (1) A `BEGIN; UPDATE target SET embedding = s.new_embedding FROM src s WHERE
/// target.sku = s.sku; COMMIT` into a vector-indexed target: after COMMIT a
/// `vector_distance` search near the NEW embedding returns the updated row, and
/// near the OLD embedding does NOT — the live HNSW was reindexed by the expanded
/// `PointPut`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_update_from_join_indexed_and_searchable() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "tuj_target", "tuj_source", "idx_tuj").await;
    // `moved` starts on the x-axis and is updated onto the z-axis. `anchor_x`
    // sits just off the x-axis so that once `moved` leaves, an x-axis search
    // resolves to the anchor rather than the moved row.
    insert_target(&srv, "tuj_target", "moved", "m1", [1.0, 0.0, 0.0, 0.0]).await;
    insert_target(&srv, "tuj_target", "anchor_x", "ax", [0.9, 0.1, 0.0, 0.0]).await;
    // Source row joined on `sku`; its `new_embedding` sits on the z-axis.
    insert_source(&srv, "tuj_source", "s_m1", "m1", [0.0, 0.0, 1.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec(
        "UPDATE tuj_target SET embedding = s.new_embedding \
         FROM tuj_source s WHERE tuj_target.sku = s.sku",
    )
    .await
    .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // Near the NEW (z-axis) embedding: the updated row is returned — it was
    // reindexed into the live HNSW at its new position.
    let near_new = nearest(&srv, "tuj_target", [0.0, 0.0, 1.0, 0.0]).await;
    assert_eq!(
        near_new.first().map(String::as_str),
        Some("moved"),
        "z-axis (new embedding) search must return the updated 'moved'; got {near_new:?} \
         (pre-fix: buffered UPDATE...FROM replayed outside the batch)"
    );

    // Near the OLD (x-axis) embedding: the anchor is returned, NOT the moved row
    // — proving the HNSW no longer holds 'moved' at the x-axis.
    let near_old = nearest(&srv, "tuj_target", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        near_old.first().map(String::as_str),
        Some("anchor_x"),
        "x-axis (old embedding) search must resolve to 'anchor_x', not the moved row: {near_old:?}"
    );
}

/// (2) A `BEGIN; UPDATE ... FROM ...; ROLLBACK` leaves the target row with its
/// ORIGINAL embedding: the expanded `PointPut` rode the undo-tracked COMMIT batch
/// and unwound with the abort, so search near the NEW embedding resolves to
/// another row and search near the OLD embedding still returns the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_update_from_join_rollback_leaves_original() {
    let srv = TestServer::start().await;
    setup_target_and_source(&srv, "tur_target", "tur_source", "idx_tur").await;
    // `stay` starts on the x-axis; the update would move it to the w-axis.
    // `anchor_w` sits on the w-axis so that, IF the update had applied, a w-axis
    // search would be ambiguous — but after ROLLBACK it cleanly returns the
    // anchor and `stay` remains the unique x-axis neighbour.
    insert_target(&srv, "tur_target", "stay", "k1", [1.0, 0.0, 0.0, 0.0]).await;
    insert_target(&srv, "tur_target", "anchor_w", "aw", [0.0, 0.0, 0.0, 1.0]).await;
    insert_source(&srv, "tur_source", "s_k1", "k1", [0.0, 0.0, 0.0, 1.0]).await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec(
        "UPDATE tur_target SET embedding = s.new_embedding \
         FROM tur_source s WHERE tur_target.sku = s.sku",
    )
    .await
    .unwrap();
    srv.exec("ROLLBACK").await.unwrap();

    // Near the OLD (x-axis) embedding: `stay` is still there — its embedding was
    // never changed because the update rolled back.
    let near_old = nearest(&srv, "tur_target", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        near_old.first().map(String::as_str),
        Some("stay"),
        "x-axis (original embedding) must still return 'stay' after ROLLBACK: {near_old:?}"
    );

    // Near the NEW (w-axis) embedding: the anchor is returned, NOT `stay` —
    // proving `stay` never moved to the w-axis (the write unwound with the abort).
    let near_new = nearest(&srv, "tur_target", [0.0, 0.0, 0.0, 1.0]).await;
    assert_eq!(
        near_new.first().map(String::as_str),
        Some("anchor_w"),
        "w-axis (new embedding) must resolve to 'anchor_w', proving 'stay' did not move: {near_new:?}"
    );
}
