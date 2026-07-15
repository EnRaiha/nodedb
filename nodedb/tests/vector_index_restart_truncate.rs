// SPDX-License-Identifier: BUSL-1.1

//! Restart durability for a secondary vector index under `TRUNCATE`.
//!
//! `execute_truncate` deletes every document in a collection via
//! `sparse.delete`, which mints no WAL redo of its own (`wal_append_document_op`
//! returns `None` for `DocumentOp::Truncate` — row durability is
//! redb-synchronous). The HNSW index is an in-memory side-effect rebuilt from
//! document `Put` records on a WAL-only restart, so without a post-apply
//! write-set (mirroring `BulkDelete`) a truncated row's original `INSERT`
//! `Put` would replay with no matching `Delete` to cancel it and the deleted
//! vector would resurrect in the rebuilt HNSW.

mod common;

use common::pgwire_harness::TestServer;

/// TRUNCATE removes every row from a vector-indexed collection; after a
/// WAL-only restart the truncated rows' vectors must not resurrect, and rows
/// inserted AFTER the truncate must remain searchable at their own axes.
#[tokio::test]
async fn truncate_vector_index_restart_no_resurrection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION docs_trunc TYPE document")
        .await
        .unwrap();
    srv.exec(
        "CREATE VECTOR INDEX idx_docs_trunc ON docs_trunc (embedding) \
         METRIC cosine DIM 4",
    )
    .await
    .unwrap();

    // t1/t2 will be wiped by TRUNCATE.
    let truncated_rows: &[(&str, [f32; 4])] =
        &[("t1", [1.0, 0.0, 0.0, 0.0]), ("t2", [0.0, 1.0, 0.0, 0.0])];
    for (id, emb) in truncated_rows {
        srv.exec(&format!(
            "INSERT INTO docs_trunc (id, embedding) VALUES \
             ('{id}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    srv.exec("TRUNCATE TABLE docs_trunc").await.unwrap();

    // Post-truncate anchors sit just off t1/t2's old axes, so each becomes the
    // UNIQUE nearest neighbour of the old-axis query once t1/t2 are truly gone
    // — a resurrected pre-truncate vector (distance 0) would beat it. `s3` is a
    // distinct survivor on the z-axis, inserted post-truncate.
    let post_truncate_rows: &[(&str, [f32; 4])] = &[
        ("anchor_x", [0.85, 0.1, 0.0, 0.0]),
        ("anchor_y", [0.1, 0.85, 0.0, 0.0]),
        ("s3", [0.0, 0.0, 1.0, 0.0]),
    ];
    for (id, emb) in post_truncate_rows {
        srv.exec(&format!(
            "INSERT INTO docs_trunc (id, embedding) VALUES \
             ('{id}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // WAL-only restart (no vector checkpoint).
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // (a) Each truncated row's old axis must now return its post-truncate
    // anchor, never the truncated row — the truncated vectors must not
    // resurrect.
    let old_x = srv2
        .query_rows(
            "SELECT id FROM docs_trunc \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        old_x[0][0], "anchor_x",
        "t1's truncated vector must not resurrect after restart: {old_x:?}"
    );
    let old_y = srv2
        .query_rows(
            "SELECT id FROM docs_trunc \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        old_y[0][0], "anchor_y",
        "t2's truncated vector must not resurrect after restart: {old_y:?}"
    );

    // (b) A row inserted after TRUNCATE remains searchable at its own axis.
    let survivor = srv2
        .query_rows(
            "SELECT id FROM docs_trunc \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 1.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        survivor[0][0], "s3",
        "rows inserted after TRUNCATE must remain in the rebuilt HNSW after restart: {survivor:?}"
    );

    // (c) Exactly the three post-truncate rows exist — TRUNCATE must not
    // leave any truncated row queryable by full scan either.
    let all = srv2
        .query_rows("SELECT id FROM docs_trunc ORDER BY id")
        .await
        .unwrap();
    let ids: Vec<&str> = all.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(
        ids,
        vec!["anchor_x", "anchor_y", "s3"],
        "TRUNCATE must remove t1/t2 durably across restart: {ids:?}"
    );
}
