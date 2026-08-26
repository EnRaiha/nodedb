// SPDX-License-Identifier: BUSL-1.1

//! Live regression coverage for stable, collision-free identity of rows
//! inserted by a `MERGE ... WHEN NOT MATCHED THEN INSERT` arm. Each
//! inserted row must be assigned its own fresh, catalog-registered
//! surrogate (never the source row's), giving a distinct, deterministic
//! storage key and full cross-engine index maintenance.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_insert_two_rows_get_distinct_stable_identity() {
    let server = TestServer::start().await;

    // Target: an initially-empty vector-indexed collection.
    server.exec("CREATE COLLECTION mss_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_mss_target_emb ON mss_target METRIC cosine DIM 4")
        .await
        .unwrap();

    // Source: two rows with distinct join ids and embeddings. The INSERT arm
    // deliberately projects no id column.
    server.exec("CREATE COLLECTION mss_source").await.unwrap();
    for (id, val, v) in [
        ("p", 10i64, [1.0f32, 0.0, 0.0, 0.0]),
        ("q", 20, [0.0, 0.0, 0.0, 1.0]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO mss_source (id, val, embedding) VALUES \
                 ('{id}', {val}, ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }

    // MERGE: both source rows are NOT MATCHED → INSERT (val, embedding) with no
    // id, exercising the surrogate-inheriting insert path twice back-to-back.
    server
        .exec(
            "MERGE INTO mss_target t \
             USING mss_source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (val, embedding) \
             VALUES (s.val, s.embedding)",
        )
        .await
        .unwrap();

    // Both rows survive with distinct `val`s — no collision overwrote either.
    let vals = server
        .query_text("SELECT val FROM mss_target ORDER BY val")
        .await
        .unwrap();
    assert_eq!(
        vals,
        vec!["10".to_string(), "20".to_string()],
        "both merge-inserted rows must survive with distinct vals; got {vals:?} \
         (pre-fix: colliding merge-{{nanos}} keys could drop one)"
    );

    // Distinct stable identity: both rows are independently present in the
    // HNSW under distinct surrogates, so a top-2 vector search returns both.
    let hits = server
        .query_text(
            "SELECT val FROM mss_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        hits.len(),
        2,
        "both merge-inserted rows must be independently vector-indexed under \
         distinct surrogates; got {hits:?} (pre-fix: no surrogate → not in HNSW)"
    );
}
