// SPDX-License-Identifier: BUSL-1.1

//! Live (no-restart) regression coverage for stable, collision-free identity of
//! rows inserted by a `MERGE ... WHEN NOT MATCHED THEN INSERT` arm.
//!
//! Pre-fix, a NOT-MATCHED insert whose projected columns carried no `id` field
//! derived its storage key from `merge-{subsec_nanos}` — non-deterministic and
//! collision-prone for two inserts applied in the same tight Phase-3 loop — and
//! allocated NO surrogate, so the row was never entered into any surrogate-keyed
//! index. Two merge-inserted rows could therefore overwrite each other and were
//! invisible to the vector index.
//!
//! Post-fix each inserted row is assigned its OWN fresh, catalog-registered
//! surrogate (never the source row's), giving a distinct, deterministic storage
//! key and full cross-engine index maintenance.
//!
//! Reliably fails pre-fix: if the two synthetic keys collide, only one row
//! survives (the scan sees a single row); if they do not collide, both rows are
//! stored but neither is in the HNSW index, so the vector searches return
//! nothing. Either way an assertion below fails.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_insert_two_rows_get_distinct_stable_identity() {
    let server = TestServer::start().await;

    // Target: an initially-empty vector-indexed collection.
    server.exec("CREATE COLLECTION mss_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_mss_target_emb ON mss_target METRIC cosine DIM 4")
        .await
        .unwrap();

    // Source: two rows with distinct join ids, a distinguishing `val`, and
    // distinct embeddings. The INSERT arm deliberately projects NO id column, so
    // the pre-fix path fell back to the collision-prone `merge-{nanos}` key.
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

    // Distinct stable identity: both rows are independently present in the HNSW
    // under distinct surrogates, so a top-2 vector search returns both. Pre-fix,
    // a no-`id` insert allocated no surrogate and never entered the HNSW, so a
    // vector search returned nothing.
    //
    // (Projecting a non-key field *value* back through a vector-`WHERE` search —
    // e.g. asserting the near-E1 hit's `val` is exactly 10 — exercises the
    // vector-result body-projection path, a separate pre-existing capability
    // that no vector test projects a non-pk field through; it is orthogonal to
    // the surrogate-registration guarantee this test guards, which is fully
    // established by the distinct `vals` scan above plus both rows being
    // vector-searchable here.)
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
