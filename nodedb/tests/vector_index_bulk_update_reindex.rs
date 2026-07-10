// SPDX-License-Identifier: BUSL-1.1

//! Live (no-restart) regression coverage for the secondary vector index on a
//! predicate (bulk) `UPDATE`.
//!
//! An `UPDATE ... SET embedding = <new> WHERE <non-PK predicate>` routes to
//! `execute_bulk_update` (not the single-key point path — the predicate is on a
//! non-primary-key field, so no `target_keys` are extracted). That handler
//! rewrites each matched row's stored body and reconciles the secondary btree /
//! FTS / graph overlays, but historically NOT the HNSW vector index. The
//! pre-update embedding therefore stayed indexed under the row's stable
//! surrogate: a KNN search kept returning the stale vector and the new embedding
//! was never searchable — all in the same process with no restart.
//!
//! Both assertions below fail on the pre-fix code: near the new point the search
//! finds a decoy instead of the moved row, and near the old point the stale
//! vector still wins over the untouched anchor.

mod common;

use common::pgwire_harness::TestServer;

async fn setup(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{name}_emb ON {name} METRIC cosine DIM 4"
        ))
        .await
        .unwrap();
    // `anchor_e1` sits near E1 but is never touched — post-fix it becomes the
    // nearest row to E1 once `target` moves away. `target` starts EXACTLY at E1
    // and carries tag='move' so the predicate UPDATE (a non-PK filter → bulk
    // path) selects it. `decoy` sits near E2 but not exactly.
    for (id, tag, v) in [
        ("anchor_e1", "keep", [0.9f32, 0.1, 0.0, 0.0]),
        ("target", "move", [1.0, 0.0, 0.0, 0.0]),
        ("decoy", "keep", [0.1, 0.0, 0.0, 0.9]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {name} (id, tag, embedding) VALUES \
                 ('{id}', '{tag}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_update_embedding_reindexes_vector_same_process() {
    let server = TestServer::start().await;
    setup(&server, "vec_bulk_upd").await;

    // Precondition: near E1, the exact-match `target` is the nearest row.
    let pre = server
        .query_text(
            "SELECT id FROM vec_bulk_upd \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        pre.first().map(String::as_str),
        Some("target"),
        "precondition: target (exact E1) must be nearest to E1; got {pre:?}"
    );

    // Predicate UPDATE on a non-PK field → `execute_bulk_update`. Move the
    // embedding to E2 = [0,0,0,1], far from E1.
    server
        .exec(
            "UPDATE vec_bulk_upd SET embedding = ARRAY[0.0, 0.0, 0.0, 1.0] \
             WHERE tag = 'move'",
        )
        .await
        .unwrap();

    // New embedding must be searchable: near E2, `target` (now exact E2) must
    // beat `decoy`. Pre-fix the stale E1 vector is still indexed, so the nearest
    // to E2 is `decoy` — this fails.
    let near_e2 = server
        .query_text(
            "SELECT id FROM vec_bulk_upd \
             WHERE embedding <-> ARRAY[0.0, 0.0, 0.0, 1.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e2.first().map(String::as_str),
        Some("target"),
        "after bulk UPDATE, target's new embedding (exact E2) must be nearest to \
         E2; got {near_e2:?} (stale index would return 'decoy')"
    );

    // Old embedding must be gone: near E1, the nearest row must be `anchor_e1`,
    // NOT the moved `target`. Pre-fix the stale [1,0,0,0] vector remains indexed
    // under target's surrogate (distance 0 to E1), so it still wins — failing.
    let near_e1 = server
        .query_text(
            "SELECT id FROM vec_bulk_upd \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("anchor_e1"),
        "after bulk UPDATE, target's OLD embedding must be gone from the HNSW; \
         nearest to E1 must be anchor_e1, not the moved 'target'; got {near_e1:?}"
    );
}
