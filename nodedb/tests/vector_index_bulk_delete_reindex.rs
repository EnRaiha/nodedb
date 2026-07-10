// SPDX-License-Identifier: BUSL-1.1

//! Live (no-restart) regression coverage for the secondary vector index on a
//! predicate (bulk) `DELETE`.
//!
//! A `DELETE ... WHERE <non-PK predicate>` routes to `execute_bulk_delete`,
//! which cascades to the inverted (FTS) index, secondary indexes, and graph
//! edges — but historically NOT the HNSW vector index. The deleted row's vector
//! node therefore stayed live under its (now-orphaned) surrogate: a KNN search
//! kept scoring the leaked vector, and because the catalog surrogate→PK mapping
//! survives, the phantom hit still surfaced the deleted row's id — all in the
//! same process with no restart.
//!
//! The assertion below fails on the pre-fix code: near the deleted row's point
//! the leaked vector wins and the search returns the deleted `target` instead of
//! the surviving `anchor`.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_delete_removes_vector_same_process() {
    let server = TestServer::start().await;
    let name = "vec_bulk_del";
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
    // `target` sits EXACTLY at E1 and carries tag='del' (the predicate DELETE, a
    // non-PK filter → bulk path, selects it). `anchor` sits at E2, orthogonal to
    // E1, and is never deleted.
    for (id, tag, v) in [
        ("target", "del", [1.0f32, 0.0, 0.0, 0.0]),
        ("anchor", "keep", [0.0, 0.0, 0.0, 1.0]),
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

    // Precondition: near E1, the exact-match `target` is the nearest row.
    let pre = server
        .query_text(
            "SELECT id FROM vec_bulk_del \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        pre.first().map(String::as_str),
        Some("target"),
        "precondition: target (exact E1) must be nearest to E1; got {pre:?}"
    );

    // Predicate DELETE on a non-PK field → `execute_bulk_delete`.
    server
        .exec("DELETE FROM vec_bulk_del WHERE tag = 'del'")
        .await
        .unwrap();

    // Same process, no restart: a search near E1 must now return `anchor` — the
    // only surviving row. Pre-fix the deleted target's [1,0,0,0] vector is still
    // indexed (distance 0 to E1) and its surrogate still resolves to 'target' at
    // the response boundary, so the leaked phantom wins — this fails.
    let near_e1 = server
        .query_text(
            "SELECT id FROM vec_bulk_del \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("anchor"),
        "after bulk DELETE, the deleted target's vector must be gone from the \
         HNSW; nearest to E1 must be the surviving 'anchor', not 'target'; got \
         {near_e1:?}"
    );
    assert!(
        !near_e1.iter().any(|id| id == "target"),
        "deleted row's leaked vector still searchable after bulk DELETE: {near_e1:?}"
    );
}
