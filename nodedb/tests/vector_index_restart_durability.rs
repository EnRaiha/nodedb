// SPDX-License-Identifier: BUSL-1.1

//! Restart durability for a secondary vector index over a document collection.
//!
//! `CREATE COLLECTION docs TYPE document; CREATE VECTOR INDEX ... ON docs`
//! journals each write only as a document `Put` record — there is no separate
//! `VectorPut` record on this path, and the HNSW index is an in-memory
//! side-effect. After a restart the index must be rebuilt from the WAL with
//! each vector node bound to its row's real global surrogate, so that vector
//! search projects the user primary key (e.g. `'persisted'`) rather than a
//! headless internal id — or nothing at all.
//!
//! Before the fix, a WAL-only restart leaves the secondary vector index empty
//! (the document `Put` records were never replayed into the HNSW), so the
//! search below returns no row and the PK assertion fails.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test]
async fn document_secondary_vector_index_restart_projects_pk() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION docs_vec_restart TYPE document")
        .await
        .unwrap();
    srv.exec(
        "CREATE VECTOR INDEX idx_docs_vec_restart ON docs_vec_restart (embedding) \
         METRIC cosine DIM 4",
    )
    .await
    .unwrap();

    // Known PKs with orthogonal embeddings so the nearest neighbour of a query
    // aligned with one row is unambiguous.
    let rows: &[(&str, [f32; 4])] = &[
        ("persisted", [1.0, 0.0, 0.0, 0.0]),
        ("second", [0.0, 1.0, 0.0, 0.0]),
        ("third", [0.0, 0.0, 1.0, 0.0]),
    ];
    for (id, emb) in rows {
        srv.exec(&format!(
            "INSERT INTO docs_vec_restart (id, embedding) VALUES \
             ('{id}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // Restart against the same data directory: releases every WAL/redb handle,
    // then reopens and replays the WAL (no vector checkpoint is taken here, so
    // recovery is WAL-only — the exact path the fix targets).
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    let ids = srv2
        .query_rows(
            "SELECT id FROM docs_vec_restart \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();

    assert_eq!(
        ids.len(),
        1,
        "secondary vector index must survive restart and return the nearest row; got {ids:?}"
    );
    assert_eq!(
        ids[0][0], "persisted",
        "post-restart vector search must project the user PK, not a numeric id: {ids:?}"
    );
}

/// UPDATE variant: an autocommit `PointUpdate` that changes a row's embedding on
/// a collection with a secondary vector index mints no WAL redo of its own on
/// the pre-dispatch path. The updated embedding survives a WAL-only restart only
/// because the Data Plane carries the surrogate + post-image back in the
/// response write-set and the Control Plane appends a post-apply `Put` redo.
///
/// Without that redo, WAL-only replay rebuilds the HNSW from the original
/// `INSERT` `Put` record and the PRE-update embedding resurrects: a query aligned
/// with the new embedding misses the row, and a query aligned with the old
/// embedding wrongly returns it.
#[tokio::test]
async fn document_secondary_vector_index_update_restart_projects_pk() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION docs_vec_upd_restart TYPE document")
        .await
        .unwrap();
    srv.exec(
        "CREATE VECTOR INDEX idx_docs_vec_upd_restart ON docs_vec_upd_restart (embedding) \
         METRIC cosine DIM 4",
    )
    .await
    .unwrap();

    // `persisted` starts aligned with the x-axis. `anchor` sits just off the
    // x-axis so that, once `persisted` is moved away, `anchor` is the UNIQUE
    // nearest neighbour of the old-axis query — but if the pre-update embedding
    // resurrects, `persisted` (at distance 0) would beat it. `second` / `third`
    // are orthogonal fillers.
    let rows: &[(&str, [f32; 4])] = &[
        ("persisted", [1.0, 0.0, 0.0, 0.0]),
        ("anchor", [0.9, 0.1, 0.0, 0.0]),
        ("second", [0.0, 1.0, 0.0, 0.0]),
        ("third", [0.0, 0.0, 1.0, 0.0]),
    ];
    for (id, emb) in rows {
        srv.exec(&format!(
            "INSERT INTO docs_vec_upd_restart (id, embedding) VALUES \
             ('{id}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // Move `persisted` to the w-axis — orthogonal to its original embedding.
    srv.exec(
        "UPDATE docs_vec_upd_restart SET embedding = ARRAY[0.0, 0.0, 0.0, 1.0] \
         WHERE id = 'persisted'",
    )
    .await
    .unwrap();

    // WAL-only restart (no vector checkpoint) — the exact path the fix targets.
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // (a) A query aligned with the NEW embedding must return the updated row.
    let new_aligned = srv2
        .query_rows(
            "SELECT id FROM docs_vec_upd_restart \
             ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 0.0, 1.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        new_aligned.len(),
        1,
        "vector index must survive restart and return the nearest row; got {new_aligned:?}"
    );
    assert_eq!(
        new_aligned[0][0], "persisted",
        "post-restart search aligned with the UPDATED embedding must return the updated row \
         (the post-apply redo preserved the new vector): {new_aligned:?}"
    );

    // (b) A query aligned with the OLD embedding must NOT return the updated row
    // first — its old vector must not have resurrected. `anchor` is the correct
    // nearest neighbour of the old axis once `persisted` has moved away.
    let old_aligned = srv2
        .query_rows(
            "SELECT id FROM docs_vec_upd_restart \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        old_aligned.len(),
        1,
        "old-axis query must return a nearest row; got {old_aligned:?}"
    );
    assert_ne!(
        old_aligned[0][0], "persisted",
        "the pre-update embedding must not resurrect after WAL-only restart: a query aligned \
         with the OLD vector wrongly returned the updated row first: {old_aligned:?}"
    );
}
