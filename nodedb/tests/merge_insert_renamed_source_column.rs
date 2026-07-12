// SPDX-License-Identifier: BUSL-1.1

//! Live (no-restart) regression coverage for a `MERGE ... WHEN NOT MATCHED
//! THEN INSERT (cols) VALUES (s.exprs)` arm whose source column is named
//! DIFFERENTLY from the target column it feeds.
//!
//! Historically the planner discarded the `VALUES` expressions and the merge
//! handler re-derived each inserted value by name-matching the *target* column
//! against the source document. That only works when the source column happens
//! to share the target column's name; a renamed source column (target
//! `embedding` ← `s.new_embedding`) matched nothing and inserted an empty /
//! NULL value — silent data loss.
//!
//! The fix carries the real `VALUES` expressions across the bridge and evaluates
//! them against the source row, so `s.new_embedding` lands in the target's
//! `embedding` column. Both assertions fail on the pre-fix code: the scanned
//! `embedding` comes back empty (not equal to the source array), and the vector
//! search returns nothing (the row was never indexed with a real vector).

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_insert_renamed_source_column_carries_values() {
    let server = TestServer::start().await;

    // Target: initially-empty document collection with a secondary vector index
    // on the `embedding` column.
    server.exec("CREATE COLLECTION mrs_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_mrs_target_emb ON mrs_target METRIC cosine DIM 4")
        .await
        .unwrap();

    // Source: rows whose vector lives in a DIFFERENTLY-named column,
    // `new_embedding`. Neither id exists in the (empty) target, so both are
    // NOT-MATCHED and get INSERTed by the MERGE.
    server.exec("CREATE COLLECTION mrs_source").await.unwrap();
    for (id, v) in [
        ("alpha", [1.0f32, 0.0, 0.0, 0.0]),
        ("beta", [0.0, 1.0, 0.0, 0.0]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO mrs_source (id, new_embedding) VALUES \
                 ('{id}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }

    // MERGE: the TARGET column `embedding` is fed by the SOURCE column
    // `new_embedding` — a rename that pre-fix produced an empty embedding.
    server
        .exec(
            "MERGE INTO mrs_target t \
             USING mrs_source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, embedding) \
             VALUES (s.id, s.new_embedding)",
        )
        .await
        .unwrap();

    // Both rows are present.
    let scanned = server
        .query_text("SELECT id FROM mrs_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        scanned,
        vec!["alpha".to_string(), "beta".to_string()],
        "scan must return both merge-inserted rows; got {scanned:?}"
    );

    // (1) Scan: the inserted `embedding` must equal the source `new_embedding`.
    // Both render through the same pgwire array formatter, so a correct copy is
    // byte-identical. Pre-fix the target embedding is empty → not equal.
    let src_emb = server
        .query_text("SELECT new_embedding FROM mrs_source WHERE id = 'alpha'")
        .await
        .unwrap();
    let tgt_emb = server
        .query_text("SELECT embedding FROM mrs_target WHERE id = 'alpha'")
        .await
        .unwrap();
    let src_val = src_emb.first().cloned().unwrap_or_default();
    let tgt_val = tgt_emb.first().cloned().unwrap_or_default();
    assert!(
        !tgt_val.is_empty(),
        "merge-inserted embedding must be non-empty (pre-fix: renamed source \
         column dropped → empty embedding)"
    );
    assert_eq!(
        tgt_val, src_val,
        "merge-inserted embedding must equal the source new_embedding; \
         target={tgt_val:?} source={src_val:?}"
    );

    // (2) Vector search near alpha's exact vector returns the merge-inserted
    // `alpha`. This can only match if the real vector was carried into the
    // target's `embedding` column and indexed. Pre-fix: empty vector → not in
    // the HNSW index → no result.
    let near_e1 = server
        .query_text(
            "SELECT id FROM mrs_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("alpha"),
        "vector search near alpha's vector must return the merge-inserted \
         'alpha'; got {near_e1:?} (pre-fix: renamed source column → empty \
         vector → not indexed → empty result)"
    );

    // Sanity: beta's vector is independently searchable, proving each row got
    // its own source vector (not a shared/empty placeholder).
    let near_e2 = server
        .query_text(
            "SELECT id FROM mrs_target \
             WHERE embedding <-> ARRAY[0.0, 1.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e2.first().map(String::as_str),
        Some("beta"),
        "vector search near beta's vector must return the merge-inserted \
         'beta'; got {near_e2:?}"
    );
}
