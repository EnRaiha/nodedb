// SPDX-License-Identifier: BUSL-1.1

//! Restart durability for a secondary vector index under
//! `DocumentOp::UpdateFromJoin` — a multi-row `UPDATE target SET ... FROM
//! source WHERE target.col = source.col` whose SET values come from a joined
//! source collection.
//!
//! Structurally this is `BulkUpdate` with the assignment RHS sourced from a
//! join instead of a literal/expr over the target row alone: each matched
//! target row's `sparse.put` reconciles storage + the btree/FTS/graph
//! overlays but mints no WAL redo carrying the new body. The HNSW index is an
//! in-memory side-effect rebuilt from document `Put` records on a WAL-only
//! restart, so without a post-apply redo per touched row, an `UPDATE ...
//! FROM` that rewrites an embedding would rebuild the HNSW from the
//! pre-update `Put` and resurrect the stale vector.

mod common;

use common::pgwire_harness::TestServer;

/// A joined UPDATE moves the rows whose `sku` matches a source row to a new
/// axis carried on the source's `new_embedding` column. After a WAL-only
/// restart, the moved rows must be searchable at the NEW axis while their OLD
/// axis resolves to an off-axis anchor, not a resurrected pre-update vector.
#[tokio::test]
async fn update_from_join_vector_index_restart_no_resurrection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION docs_ufj TYPE document")
        .await
        .unwrap();
    srv.exec(
        "CREATE VECTOR INDEX idx_docs_ufj ON docs_ufj (embedding) \
         METRIC cosine DIM 4",
    )
    .await
    .unwrap();
    srv.exec("CREATE COLLECTION src_ufj TYPE document")
        .await
        .unwrap();

    // Target rows. `u1`/`u2` (grp='move') start on the x/y axes and are moved
    // to the w-axis via the join. `anchor_x`/`anchor_y` (grp='keep') sit just
    // off those axes so that, once the moved rows leave, each anchor is the
    // UNIQUE nearest neighbour of its old-axis query — a resurrected
    // pre-update vector (distance 0) would beat it.
    let rows: &[(&str, &str, &str, [f32; 4])] = &[
        ("u1", "u1", "move", [1.0, 0.0, 0.0, 0.0]),
        ("u2", "u2", "move", [0.0, 1.0, 0.0, 0.0]),
        ("anchor_x", "ax", "keep", [0.85, 0.1, 0.0, 0.0]),
        ("anchor_y", "ay", "keep", [0.1, 0.85, 0.0, 0.0]),
        ("filler", "f", "keep", [0.0, 0.0, 1.0, 0.0]),
    ];
    for (id, sku, grp, emb) in rows {
        srv.exec(&format!(
            "INSERT INTO docs_ufj (id, sku, grp, embedding) VALUES \
             ('{id}', '{sku}', '{grp}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // Source rows carry the new (w-axis) embedding for u1/u2's sku only. Each
    // needs an explicit `id` (the document PK) — two rows without one would
    // collide on the empty-string default key.
    for sku in ["u1", "u2"] {
        srv.exec(&format!(
            "INSERT INTO src_ufj (id, sku, new_embedding) VALUES \
             ('src_{sku}', '{sku}', ARRAY[0.0, 0.0, 0.0, 1.0])"
        ))
        .await
        .unwrap();
    }

    // One joined UPDATE moves both grp='move' rows to the w-axis, sourcing
    // the new embedding from the joined source row. `grp = 'move'` is a
    // target-only filter alongside the required equi-join predicate.
    srv.exec(
        "UPDATE docs_ufj SET embedding = s.new_embedding \
         FROM src_ufj s WHERE docs_ufj.sku = s.sku AND docs_ufj.grp = 'move'",
    )
    .await
    .unwrap();

    // WAL-only restart (no vector checkpoint) — the exact path the redo targets.
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // (a) The new (w) axis must return a moved row — proving the joined
    // update's new embeddings were rebuilt into the HNSW post-restart.
    let new_aligned = srv2
        .query_rows(
            "SELECT id FROM docs_ufj \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 0.0, 1.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(new_aligned.len(), 1, "new-axis query must return a row");
    assert!(
        matches!(new_aligned[0][0].as_str(), "u1" | "u2"),
        "post-restart new-axis query must return one of the moved rows: {new_aligned:?}"
    );

    // (b) Each moved row's OLD axis must return its anchor, not the moved row —
    // the pre-update embeddings must not resurrect.
    let old_x = srv2
        .query_rows(
            "SELECT id FROM docs_ufj \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        old_x[0][0], "anchor_x",
        "u1's pre-update x-axis vector must not resurrect after restart: {old_x:?}"
    );
    let old_y = srv2
        .query_rows(
            "SELECT id FROM docs_ufj \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        old_y[0][0], "anchor_y",
        "u2's pre-update y-axis vector must not resurrect after restart: {old_y:?}"
    );
}
