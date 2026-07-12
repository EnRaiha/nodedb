// SPDX-License-Identifier: BUSL-1.1

//! Restart durability for a secondary vector index under an autocommit
//! `MERGE INTO ... USING ...` that touches all three arms.
//!
//! The MERGE apply handler applies every arm to the live HNSW index (via
//! `apply_point_put` / `apply_point_delete` on the CP-assigned surrogates) but
//! mints NO WAL redo. The HNSW is an in-memory side-effect rebuilt from
//! document `Put` records on a WAL-only restart, so without a post-apply redo
//! per touched row the merge would rebuild the HNSW from the pre-merge `Put`
//! records — resurrecting the UPDATE's stale embedding and the DELETE's removed
//! embedding, and losing the INSERT's new embedding. The DP carries a per-row
//! Put/Delete write-set back to the CP orchestrator, which mints the durable
//! redo; this test pins that the rebuilt index matches the post-merge truth.

mod common;

use common::pgwire_harness::TestServer;

/// One MERGE drives a MATCHED-UPDATE (moved to the w-axis), a MATCHED-DELETE
/// (removed), and a NOT-MATCHED-INSERT (new row on the z-axis). After a
/// WAL-only restart the HNSW must reflect the post-merge state: the inserted
/// row reachable at its axis, the updated row at its NEW axis with its OLD axis
/// resolving to an off-axis anchor, and the deleted row's axis resolving to its
/// anchor — no resurrection of the pre-merge embeddings.
#[tokio::test]
async fn merge_vector_index_restart_no_resurrection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION docs_merge TYPE document")
        .await
        .unwrap();
    srv.exec(
        "CREATE VECTOR INDEX idx_docs_merge ON docs_merge (embedding) \
         METRIC cosine DIM 4",
    )
    .await
    .unwrap();
    srv.exec("CREATE COLLECTION src_merge TYPE document")
        .await
        .unwrap();

    // Target rows. `upd1` (grp='move') starts on the x-axis and is moved to the
    // w-axis by the MATCHED-UPDATE arm. `del1` (grp='del') sits on the y-axis
    // and is removed by the MATCHED-DELETE arm. `anchor_x`/`anchor_y`
    // (grp='keep') sit just off those axes so that, once the merged rows leave,
    // each anchor is the UNIQUE nearest neighbour of its old-axis query — a
    // resurrected pre-merge vector (cosine distance 0) would beat it. `filler`
    // is an off-axis distractor. Every row carries an explicit `id` (the
    // document PK) so no two rows collide on the empty-string default key.
    let rows: &[(&str, &str, &str, [f32; 4])] = &[
        ("upd1", "upd1", "move", [1.0, 0.0, 0.0, 0.0]),
        ("del1", "del1", "del", [0.0, 1.0, 0.0, 0.0]),
        ("anchor_x", "ax", "keep", [0.85, 0.1, 0.0, 0.0]),
        ("anchor_y", "ay", "keep", [0.1, 0.85, 0.0, 0.0]),
        ("filler", "fill", "keep", [0.5, 0.5, 0.5, 0.5]),
    ];
    for (id, sku, grp, emb) in rows {
        srv.exec(&format!(
            "INSERT INTO docs_merge (id, sku, grp, embedding) VALUES \
             ('{id}', '{sku}', '{grp}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // Source rows joined on `sku`. `upd1`/`del1` match target rows (driving the
    // UPDATE / DELETE arms); `ins1`'s sku is absent from the target so it drives
    // the NOT-MATCHED INSERT of a brand-new row on the z-axis. Each carries an
    // explicit `id` PK; `upd1` carries the new (w-axis) embedding, `ins1` the
    // new (z-axis) embedding, `del1`'s embedding is unused by the DELETE arm.
    let src_rows: &[(&str, &str, &str, [f32; 4])] = &[
        ("s_upd1", "upd1", "move", [0.0, 0.0, 0.0, 1.0]),
        ("s_del1", "del1", "del", [0.0, 0.0, 0.0, 0.0]),
        ("ins1", "ins1", "keep", [0.0, 0.0, 1.0, 0.0]),
    ];
    for (id, sku, grp, emb) in src_rows {
        srv.exec(&format!(
            "INSERT INTO src_merge (id, sku, grp, new_embedding) VALUES \
             ('{id}', '{sku}', '{grp}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // One MERGE exercising all three arms. UPDATE vs DELETE is distinguished by
    // the bare target column `grp` (the same predicate style as sql_merge.rs's
    // `WHEN MATCHED AND score < 50 THEN DELETE`); the NOT-MATCHED arm inserts a
    // fresh row from the unmatched source row.
    srv.exec(
        "MERGE INTO docs_merge t \
         USING src_merge s ON t.sku = s.sku \
         WHEN MATCHED AND grp = 'del' THEN DELETE \
         WHEN MATCHED AND grp = 'move' THEN UPDATE SET embedding = s.new_embedding \
         WHEN NOT MATCHED THEN INSERT (id, sku, grp, embedding) \
             VALUES (s.id, s.sku, s.grp, s.new_embedding)",
    )
    .await
    .unwrap();

    // PRE-RESTART PROBES: confirm the MERGE applied all arms to the LIVE HNSW
    // before we test durability. Localizes apply-bug vs durability-bug.
    let pre_z = srv
        .query_rows(
            "SELECT id FROM docs_merge \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 1.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        pre_z[0][0], "ins1",
        "PRE-RESTART z-axis (INSERT) must be ins1: {pre_z:?}"
    );
    let pre_w = srv
        .query_rows(
            "SELECT id FROM docs_merge \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 0.0, 1.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        pre_w[0][0], "upd1",
        "PRE-RESTART w-axis (UPDATE) must be upd1: {pre_w:?}"
    );
    let pre_y = srv
        .query_rows(
            "SELECT id FROM docs_merge \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        pre_y[0][0], "anchor_y",
        "PRE-RESTART y-axis (DELETE) must be anchor_y: {pre_y:?}"
    );

    // WAL-only restart (no vector checkpoint) — the exact path the redo targets.
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // (a) INSERT: the z-axis must return the newly inserted row, proving the
    // NOT-MATCHED arm's new embedding was rebuilt into the HNSW post-restart.
    let z_axis = srv2
        .query_rows(
            "SELECT id FROM docs_merge \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 1.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        z_axis[0][0], "ins1",
        "post-restart z-axis query must return the merge-inserted row: {z_axis:?}"
    );

    // (b) UPDATE new axis: the w-axis must return the moved row.
    let w_axis = srv2
        .query_rows(
            "SELECT id FROM docs_merge \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 0.0, 1.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        w_axis[0][0], "upd1",
        "post-restart w-axis query must return the merge-updated row: {w_axis:?}"
    );

    // (c) UPDATE old axis: `upd1`'s pre-merge x-axis vector must not resurrect —
    // the x-axis query must resolve to its off-axis anchor.
    let x_axis = srv2
        .query_rows(
            "SELECT id FROM docs_merge \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        x_axis[0][0], "anchor_x",
        "upd1's pre-merge x-axis vector must not resurrect after restart: {x_axis:?}"
    );

    // (d) DELETE: `del1`'s removed y-axis vector must not resurrect — the y-axis
    // query must resolve to its off-axis anchor.
    let y_axis = srv2
        .query_rows(
            "SELECT id FROM docs_merge \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        y_axis[0][0], "anchor_y",
        "del1's deleted y-axis vector must not resurrect after restart: {y_axis:?}"
    );
}
