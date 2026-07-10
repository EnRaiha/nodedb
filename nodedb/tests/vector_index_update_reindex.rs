// SPDX-License-Identifier: BUSL-1.1

//! Live (no-restart) regression coverage for the secondary vector index on an
//! autocommit `UPDATE`.
//!
//! An `UPDATE ... SET embedding = <new>` on a document collection with a
//! secondary vector index routes to `execute_point_update`, which rewrites the
//! stored body and reconciles the secondary btree / FTS / graph overlays — but
//! historically NOT the HNSW vector index. That left the pre-update embedding
//! indexed under the row's (stable) surrogate: a KNN search kept returning the
//! stale vector, and the new embedding was never searchable, all in the same
//! process with no restart.
//!
//! These tests pin both halves of the correct behavior after an in-place
//! embedding UPDATE: the NEW embedding is searchable, and the OLD embedding is
//! gone from the index. Both assertions fail on the pre-fix code (the stale
//! vector wins the search near the old point, and the new point finds a decoy
//! instead of the updated row).

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
    // anchor_e1 sits close to E1 but is never updated — post-fix it becomes the
    // nearest row to E1 once the target moves away.
    // target starts EXACTLY at E1, so it beats anchor_e1 for a query near E1.
    // decoy sits near E2 but not exactly, so post-fix the target (moved to
    // exactly E2) beats it for a query near E2.
    for (id, v) in [
        ("anchor_e1", [0.9f32, 0.1, 0.0, 0.0]),
        ("target", [1.0, 0.0, 0.0, 0.0]),
        ("decoy", [0.1, 0.0, 0.0, 0.9]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {name} (id, embedding) VALUES \
                 ('{id}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_embedding_makes_new_vector_searchable_same_process() {
    let server = TestServer::start().await;
    setup(&server, "vec_upd_new").await;

    // Precondition: near E1, the exact-match `target` is the nearest row.
    let pre = server
        .query_text(
            "SELECT id FROM vec_upd_new \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        pre.first().map(String::as_str),
        Some("target"),
        "precondition: target (exact E1) must be nearest to E1; got {pre:?}"
    );

    // Move the embedding to E2 = [0,0,0,1], far from E1.
    server
        .exec(
            "UPDATE vec_upd_new SET embedding = ARRAY[0.0, 0.0, 0.0, 1.0] \
             WHERE id = 'target'",
        )
        .await
        .unwrap();

    // Same process, no restart: a search near E2 must now surface `target`
    // (moved to exactly E2, beating the `decoy` near E2). Pre-fix the stale E1
    // vector is still indexed, so the nearest to E2 is `decoy` — this fails.
    let near_e2 = server
        .query_text(
            "SELECT id FROM vec_upd_new \
             WHERE embedding <-> ARRAY[0.0, 0.0, 0.0, 1.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e2.first().map(String::as_str),
        Some("target"),
        "after UPDATE, target's new embedding (exact E2) must be searchable and \
         nearest to E2; got {near_e2:?} (stale index would return 'decoy')"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_embedding_removes_stale_vector_same_process() {
    let server = TestServer::start().await;
    setup(&server, "vec_upd_stale").await;

    // Move the target's embedding from E1 to E2.
    server
        .exec(
            "UPDATE vec_upd_stale SET embedding = ARRAY[0.0, 0.0, 0.0, 1.0] \
             WHERE id = 'target'",
        )
        .await
        .unwrap();

    // Same process, no restart: a search near E1 must now return `anchor_e1`,
    // NOT the moved `target`. Pre-fix the stale [1,0,0,0] vector remains indexed
    // under target's surrogate (distance 0 to E1), so it still wins — failing.
    let near_e1 = server
        .query_text(
            "SELECT id FROM vec_upd_stale \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("anchor_e1"),
        "after UPDATE, target's OLD embedding must be gone from the HNSW; the \
         nearest row to E1 must be anchor_e1, not the moved 'target'; got {near_e1:?}"
    );
    assert_ne!(
        near_e1.first().map(String::as_str),
        Some("target"),
        "stale pre-update embedding still indexed for 'target' after UPDATE"
    );
}
