// SPDX-License-Identifier: BUSL-1.1

//! Live (no-restart) regression coverage for cross-engine visibility of rows
//! inserted by a `MERGE ... WHEN NOT MATCHED THEN INSERT` arm.
//!
//! `MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED
//! THEN INSERT (...) VALUES (source....)` routes to `execute_merge`, whose
//! NOT-MATCHED insert arm historically wrote the row via a raw
//! `sparse.put` under an ad-hoc doc_id, allocating NO surrogate and running
//! NONE of the cross-engine index maintenance. The merge-inserted row was thus
//! invisible to the secondary vector index, the FTS inverted index, and any
//! other surrogate-keyed engine — same process, no restart.
//!
//! All three assertions fail on the pre-fix code: the vector search and the
//! text search both return nothing (the row was never indexed), so neither
//! sees the merge-inserted document.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_insert_visible_to_vector_fts_and_scan() {
    let server = TestServer::start().await;

    // Target: an initially-empty document collection carrying BOTH a secondary
    // vector index and an FTS search index.
    server.exec("CREATE COLLECTION mcv_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_mcv_target_emb ON mcv_target METRIC cosine DIM 4")
        .await
        .unwrap();
    server
        .exec(
            "CREATE SEARCH INDEX idx_mcv_target_fts ON mcv_target FIELDS body ANALYZER 'standard'",
        )
        .await
        .unwrap();

    // Source: two rows, each with text + an embedding. Neither id exists in the
    // (empty) target, so both are NOT-MATCHED and get INSERTed by the MERGE.
    server.exec("CREATE COLLECTION mcv_source").await.unwrap();
    for (id, body, v) in [
        (
            "alpha",
            "quantum computing breakthrough",
            [1.0f32, 0.0, 0.0, 0.0],
        ),
        ("beta", "photosynthesis in plants", [0.0, 0.0, 0.0, 1.0]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO mcv_source (id, body, embedding) VALUES \
                 ('{id}', '{body}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }

    // MERGE: every source row is NOT MATCHED → INSERT into the target.
    server
        .exec(
            "MERGE INTO mcv_target t \
             USING mcv_source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, body, embedding) \
             VALUES (s.id, s.body, s.embedding)",
        )
        .await
        .unwrap();

    // (c) Normal scan sees both merge-inserted rows.
    let scanned = server
        .query_text("SELECT id FROM mcv_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        scanned,
        vec!["alpha".to_string(), "beta".to_string()],
        "scan must return both merge-inserted rows; got {scanned:?}"
    );

    // (a) Vector search near E1 finds the merge-inserted `alpha`. Pre-fix the
    // row was never inserted into the HNSW index, so the search returns nothing.
    let near_e1 = server
        .query_text(
            "SELECT id FROM mcv_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("alpha"),
        "vector search near E1 must return the merge-inserted 'alpha'; got \
         {near_e1:?} (pre-fix: no surrogate → not in HNSW → empty result)"
    );

    // Sanity: the other embedding is independently searchable too.
    let near_e2 = server
        .query_text(
            "SELECT id FROM mcv_target \
             WHERE embedding <-> ARRAY[0.0, 0.0, 0.0, 1.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e2.first().map(String::as_str),
        Some("beta"),
        "vector search near E2 must return the merge-inserted 'beta'; got {near_e2:?}"
    );

    // (b) FTS text search finds the merge-inserted `alpha` by its body text.
    // Pre-fix the row's text was never fed to the inverted index.
    let fts = server
        .query_text("SELECT id FROM mcv_target WHERE text_match(body, 'quantum')")
        .await
        .unwrap();
    assert_eq!(
        fts.iter().map(String::as_str).collect::<Vec<_>>(),
        vec!["alpha"],
        "FTS search for 'quantum' must return the merge-inserted 'alpha'; got \
         {fts:?} (pre-fix: no surrogate → not in the inverted index → empty)"
    );
}
