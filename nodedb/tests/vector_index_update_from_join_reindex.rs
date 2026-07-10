// SPDX-License-Identifier: BUSL-1.1

//! Live (no-restart) regression coverage for the secondary vector index on an
//! `UPDATE ... FROM` (join) statement.
//!
//! `UPDATE target SET embedding = source.embedding FROM source WHERE
//! target.id = source.id` routes to `execute_update_from_join`, which rewrites
//! each joined target row's stored body but historically NOT the HNSW vector
//! index. The pre-update embedding stayed indexed under the row's stable
//! surrogate: a KNN search kept returning the stale vector and the joined-in
//! embedding was never searchable — same process, no restart.
//!
//! Both assertions fail on the pre-fix code: near the new point the search finds
//! a decoy, and near the old point the stale vector still wins over the anchor.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_from_join_reindexes_vector_same_process() {
    let server = TestServer::start().await;

    // Target: a vector-indexed document collection.
    server.exec("CREATE COLLECTION uj_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_uj_target_emb ON uj_target METRIC cosine DIM 4")
        .await
        .unwrap();
    for (id, v) in [
        ("anchor_e1", [0.9f32, 0.1, 0.0, 0.0]),
        ("target", [1.0, 0.0, 0.0, 0.0]),
        ("decoy", [0.1, 0.0, 0.0, 0.9]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO uj_target (id, embedding) VALUES \
                 ('{id}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }

    // Source: one row whose id matches `target`, carrying the new embedding E2.
    server.exec("CREATE COLLECTION uj_source").await.unwrap();
    server
        .exec(
            "INSERT INTO uj_source (id, embedding) VALUES \
             ('target', ARRAY[0.0, 0.0, 0.0, 1.0])",
        )
        .await
        .unwrap();

    // Precondition: near E1, the exact-match `target` is the nearest row.
    let pre = server
        .query_text(
            "SELECT id FROM uj_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        pre.first().map(String::as_str),
        Some("target"),
        "precondition: target (exact E1) must be nearest to E1; got {pre:?}"
    );

    // UPDATE ... FROM: move target's embedding to E2 via the source join.
    server
        .exec(
            "UPDATE uj_target SET embedding = uj_source.embedding \
             FROM uj_source \
             WHERE uj_target.id = uj_source.id",
        )
        .await
        .unwrap();

    // New embedding searchable: near E2, `target` (now exact E2) beats `decoy`.
    let near_e2 = server
        .query_text(
            "SELECT id FROM uj_target \
             WHERE embedding <-> ARRAY[0.0, 0.0, 0.0, 1.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e2.first().map(String::as_str),
        Some("target"),
        "after UPDATE...FROM, target's new embedding (exact E2) must be nearest \
         to E2; got {near_e2:?} (stale index would return 'decoy')"
    );

    // Old embedding gone: near E1, nearest must be `anchor_e1`, not `target`.
    let near_e1 = server
        .query_text(
            "SELECT id FROM uj_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("anchor_e1"),
        "after UPDATE...FROM, target's OLD embedding must be gone from the HNSW; \
         nearest to E1 must be anchor_e1, not the moved 'target'; got {near_e1:?}"
    );
}
