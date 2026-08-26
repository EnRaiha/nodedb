// SPDX-License-Identifier: BUSL-1.1

//! Live regression coverage for the secondary vector index on a predicate
//! (bulk) `DELETE`. `execute_bulk_delete` cascades to FTS, secondary
//! indexes, and graph edges, but must also drop the HNSW vector node, or a
//! KNN search keeps scoring the leaked vector and surfaces the deleted row.

use crate::harness::TestServer;

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
    // `target` sits at E1 and carries tag='del'; `anchor` sits at E2 and is
    // never deleted.
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

    // A search near E1 must now return `anchor`, the only surviving row.
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
