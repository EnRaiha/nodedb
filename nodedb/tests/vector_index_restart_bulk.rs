// SPDX-License-Identifier: BUSL-1.1

//! Restart durability for a secondary vector index under MULTI-row document DML:
//! `BulkUpdate`, `BulkDelete`, and the single-row `PointDelete`.
//!
//! Each per-row mutation these handlers apply (`sparse.put` / `sparse.delete`)
//! reconciles storage + the btree/FTS/graph overlays but mints no WAL redo
//! carrying the vector-relevant post-image. The HNSW index is an in-memory
//! side-effect rebuilt from document `Put` records on a WAL-only restart, so
//! without a post-apply redo per touched row:
//!
//! * a bulk UPDATE that rewrote an embedding would rebuild the HNSW from the
//!   pre-update `INSERT` `Put` and resurrect the stale vector; and
//! * a bulk (or point) DELETE would replay the row's original `INSERT` `Put`
//!   back into the HNSW with no `Delete` record to remove it — the deleted
//!   vector resurrects.
//!
//! The Data Plane carries the surrogate (+ post-image, for updates) back in the
//! response write-set; the Control Plane mints a durable `Put` / `Delete` redo
//! per row. A `Delete` redo replays through `apply_point_delete`, whose cascade
//! soft-deletes the row's HNSW nodes, so the vector does not resurrect.

mod common;

use common::pgwire_harness::TestServer;

/// BulkUpdate: several rows moved to a new axis by one `UPDATE ... WHERE` must,
/// after a WAL-only restart, be searchable at the NEW axis while neither moved
/// row's OLD axis resurrects — the off-axis anchors must win the old-axis
/// queries (they would lose to a resurrected distance-0 pre-update vector).
#[tokio::test]
async fn bulk_update_vector_index_restart_no_resurrection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION docs_bulk_upd TYPE document")
        .await
        .unwrap();
    srv.exec(
        "CREATE VECTOR INDEX idx_docs_bulk_upd ON docs_bulk_upd (embedding) \
         METRIC cosine DIM 4",
    )
    .await
    .unwrap();

    // `u1`/`u2` (grp='move') start on the x/y axes and are moved to the w-axis.
    // `anchor_x`/`anchor_y` (grp='keep') sit just off those axes so that, once
    // the moved rows leave, each anchor is the UNIQUE nearest neighbour of its
    // old-axis query — but a resurrected pre-update vector (distance 0) beats it.
    let rows: &[(&str, &str, [f32; 4])] = &[
        ("u1", "move", [1.0, 0.0, 0.0, 0.0]),
        ("u2", "move", [0.0, 1.0, 0.0, 0.0]),
        ("anchor_x", "keep", [0.85, 0.1, 0.0, 0.0]),
        ("anchor_y", "keep", [0.1, 0.85, 0.0, 0.0]),
        ("filler", "keep", [0.0, 0.0, 1.0, 0.0]),
    ];
    for (id, grp, emb) in rows {
        srv.exec(&format!(
            "INSERT INTO docs_bulk_upd (id, grp, embedding) VALUES \
             ('{id}', '{grp}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // One multi-row UPDATE moves both grp='move' rows to the w-axis.
    srv.exec(
        "UPDATE docs_bulk_upd SET embedding = ARRAY[0.0, 0.0, 0.0, 1.0] \
         WHERE grp = 'move'",
    )
    .await
    .unwrap();

    // WAL-only restart (no vector checkpoint) — the exact path the redo targets.
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // (a) The new (w) axis must return a moved row — proving the updated
    // embeddings were rebuilt into the HNSW post-restart.
    let new_aligned = srv2
        .query_rows(
            "SELECT id FROM docs_bulk_upd \
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
            "SELECT id FROM docs_bulk_upd \
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
            "SELECT id FROM docs_bulk_upd \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        old_y[0][0], "anchor_y",
        "u2's pre-update y-axis vector must not resurrect after restart: {old_y:?}"
    );
}

/// BulkDelete: rows removed by one `DELETE ... WHERE` must not resurrect in the
/// HNSW after a WAL-only restart (a query aligned with a deleted row's old
/// embedding must return a survivor, never the deleted row), while surviving
/// rows remain searchable. This pins the post-apply `Delete` redo + its
/// replay-side vector removal.
#[tokio::test]
async fn bulk_delete_vector_index_restart_no_resurrection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION docs_bulk_del TYPE document")
        .await
        .unwrap();
    srv.exec(
        "CREATE VECTOR INDEX idx_docs_bulk_del ON docs_bulk_del (embedding) \
         METRIC cosine DIM 4",
    )
    .await
    .unwrap();

    // `d1`/`d2` (grp='del') are deleted. `s1`/`s2` sit just off d1/d2's axes so
    // each becomes the UNIQUE nearest neighbour of the deleted row's old-axis
    // query once the deleted row is gone — a resurrected deleted vector (distance
    // 0) would beat it. `s3` is a distinct survivor on the z-axis.
    let rows: &[(&str, &str, [f32; 4])] = &[
        ("d1", "del", [1.0, 0.0, 0.0, 0.0]),
        ("d2", "del", [0.0, 1.0, 0.0, 0.0]),
        ("s1", "keep", [0.85, 0.1, 0.0, 0.0]),
        ("s2", "keep", [0.1, 0.85, 0.0, 0.0]),
        ("s3", "keep", [0.0, 0.0, 1.0, 0.0]),
    ];
    for (id, grp, emb) in rows {
        srv.exec(&format!(
            "INSERT INTO docs_bulk_del (id, grp, embedding) VALUES \
             ('{id}', '{grp}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // One multi-row DELETE removes both grp='del' rows.
    srv.exec("DELETE FROM docs_bulk_del WHERE grp = 'del'")
        .await
        .unwrap();

    // WAL-only restart (no vector checkpoint).
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // (a) Each deleted row's old axis must now return its survivor anchor, never
    // the deleted row — the deleted vectors must not resurrect.
    let old_x = srv2
        .query_rows(
            "SELECT id FROM docs_bulk_del \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        old_x[0][0], "s1",
        "d1's deleted vector must not resurrect after restart: {old_x:?}"
    );
    let old_y = srv2
        .query_rows(
            "SELECT id FROM docs_bulk_del \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        old_y[0][0], "s2",
        "d2's deleted vector must not resurrect after restart: {old_y:?}"
    );

    // (b) A distinct survivor remains searchable at its own axis.
    let survivor = srv2
        .query_rows(
            "SELECT id FROM docs_bulk_del \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 1.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        survivor[0][0], "s3",
        "surviving rows must remain in the rebuilt HNSW after restart: {survivor:?}"
    );
}

/// PointDelete: a single-row `DELETE ... WHERE id = ...` must likewise not
/// resurrect the deleted row's vector after a WAL-only restart. PointDelete
/// already emits a `Delete` redo whose replay soft-deletes the HNSW node
/// through the shared `apply_point_delete` cascade, so this holds without any
/// post-apply write-set — this test pins that shared replay behaviour.
#[tokio::test]
async fn point_delete_vector_index_restart_no_resurrection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION docs_point_del TYPE document")
        .await
        .unwrap();
    srv.exec(
        "CREATE VECTOR INDEX idx_docs_point_del ON docs_point_del (embedding) \
         METRIC cosine DIM 4",
    )
    .await
    .unwrap();

    let rows: &[(&str, [f32; 4])] = &[
        ("p1", [1.0, 0.0, 0.0, 0.0]),
        ("anchor", [0.85, 0.1, 0.0, 0.0]),
        ("s2", [0.0, 1.0, 0.0, 0.0]),
    ];
    for (id, emb) in rows {
        srv.exec(&format!(
            "INSERT INTO docs_point_del (id, embedding) VALUES \
             ('{id}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    srv.exec("DELETE FROM docs_point_del WHERE id = 'p1'")
        .await
        .unwrap();

    // WAL-only restart (no vector checkpoint).
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // p1's old axis must now return the off-axis anchor, not the deleted p1.
    let old_x = srv2
        .query_rows(
            "SELECT id FROM docs_point_del \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        old_x[0][0], "anchor",
        "p1's deleted vector must not resurrect after restart: {old_x:?}"
    );

    // The remaining survivor stays searchable.
    let survivor = srv2
        .query_rows(
            "SELECT id FROM docs_point_del \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        survivor[0][0], "s2",
        "surviving rows must remain searchable after restart: {survivor:?}"
    );
}
