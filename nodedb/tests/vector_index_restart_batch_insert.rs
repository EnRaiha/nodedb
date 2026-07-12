// SPDX-License-Identifier: BUSL-1.1

//! Restart durability for a secondary vector index under `DocumentOp::BatchInsert`
//! (multi-row atomic document insert).
//!
//! `INSERT INTO <target> SELECT ... FROM <source>` is the only SQL shape that
//! reaches `DocumentOp::BatchInsert`: the Control-Plane `insert_select`
//! orchestrator scans the source page-by-page, assigns each row its OWN fresh,
//! registered surrogate, and writes each page as ONE atomic `BatchInsert` (see
//! `insert_select_cross_engine.rs` for the live, no-restart coverage of that
//! copy). A plain multi-row `INSERT ... VALUES (...),(...),(...)` does NOT
//! reach `BatchInsert` — it expands into one `PointInsert` per row on the
//! Control Plane, which already carries its own WAL `Put` redo.
//!
//! `BatchInsert` rows are durable in `redb` (each row lands inside the same
//! write transaction as every other row in its page), but `wal_append_document_op`
//! mints NO WAL redo record for `BatchInsert` — row durability there is
//! `redb`-synchronous, not WAL-replay-based. The HNSW vector index is an
//! in-memory side effect rebuilt ONLY from WAL `Put` redo records on a
//! WAL-only restart, so without a post-apply `Put` redo per inserted row, EVERY
//! row's vector inserted via `BatchInsert` would be silently absent — not
//! stale, gone — after a WAL-only restart.
//!
//! The Data Plane carries the surrogate + post-image of each inserted row back
//! in the response write-set (gated on the target collection having a vector
//! field); the Control Plane mints a durable `Put` redo per row from it — both
//! on the pgwire autocommit funnel (`plan_post_apply_redo`) and on the
//! `insert_select` orchestrator's own page-dispatch path, which bypasses that
//! funnel and mints the redo itself.

mod common;

use common::pgwire_harness::TestServer;

/// Several rows landed by ONE `INSERT ... SELECT` (a single atomic
/// `BatchInsert` page) must each remain independently searchable by their own
/// embedding after a WAL-only restart — proving every row's vector, not just
/// one, was rebuilt into the HNSW from a post-apply redo record.
#[tokio::test]
async fn batch_insert_vector_index_restart_all_rows_survive() {
    let srv = TestServer::start().await;

    // Source: a plain document collection, no vector index — rows land via
    // ordinary single-row `PointInsert` (already redo-durable), so any loss
    // observed after restart is attributable to the TARGET's `BatchInsert`
    // path, not the source population step.
    srv.exec("CREATE COLLECTION bi_source TYPE document")
        .await
        .unwrap();

    // Target: vector-indexed, initially empty — every row it ends up holding
    // arrived via the ONE `INSERT ... SELECT` below, i.e. one `BatchInsert`.
    srv.exec("CREATE COLLECTION bi_target TYPE document")
        .await
        .unwrap();
    srv.exec("CREATE VECTOR INDEX idx_bi_target ON bi_target (embedding) METRIC cosine DIM 4")
        .await
        .unwrap();

    // Three rows on distinct axes, plus an off-axis anchor for each so that,
    // if a row's post-restart vector were silently absent (not just stale),
    // its axis query would return the anchor instead — a resurrection bug
    // would return nothing new, but a LOSS bug also returns the anchor, so
    // the anchors alone cannot distinguish loss from success. The real proof
    // is assertion (a): the row's OWN id must be the nearest neighbour of its
    // own embedding, which only holds if that row's vector survived restart.
    let rows: &[(&str, [f32; 4])] = &[
        ("r1", [1.0, 0.0, 0.0, 0.0]),
        ("r2", [0.0, 1.0, 0.0, 0.0]),
        ("r3", [0.0, 0.0, 1.0, 0.0]),
    ];
    for (id, emb) in rows {
        srv.exec(&format!(
            "INSERT INTO bi_source (id, embedding) VALUES \
             ('{id}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // ONE `INSERT ... SELECT` copies all three source rows into the target as
    // a single atomic `BatchInsert` page.
    srv.exec("INSERT INTO bi_target SELECT * FROM bi_source")
        .await
        .unwrap();

    // Live (pre-restart) sanity: all three rows are present.
    let live = srv
        .query_rows("SELECT id FROM bi_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        live.len(),
        3,
        "all three batch-inserted rows must be visible before restart: {live:?}"
    );

    // WAL-only restart (no vector checkpoint) — the exact path the post-apply
    // redo targets.
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // (a) EACH row's own embedding must return that row as its own nearest
    // neighbour post-restart — proving every row's vector (not just one) was
    // rebuilt into the HNSW from a post-apply `Put` redo record.
    for (id, emb) in rows {
        let hit = srv2
            .query_rows(&format!(
                "SELECT id FROM bi_target \
                 ORDER BY vector_distance(embedding, ARRAY[{},{},{},{}]) LIMIT 1",
                emb[0], emb[1], emb[2], emb[3]
            ))
            .await
            .unwrap();
        assert_eq!(
            hit.len(),
            1,
            "axis query for row '{id}' must return a row after restart: {hit:?}"
        );
        assert_eq!(
            hit[0][0], *id,
            "row '{id}''s batch-inserted vector must survive a WAL-only restart \
             (absent, not just stale, would fail this): {hit:?}"
        );
    }

    // (b) The full row set (document bodies, not just vectors) survives too.
    let post_restart = srv2
        .query_rows("SELECT id FROM bi_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        post_restart.len(),
        3,
        "all three batch-inserted rows must remain visible after restart: {post_restart:?}"
    );
}
