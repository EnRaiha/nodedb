// SPDX-License-Identifier: BUSL-1.1

//! In-transaction HYBRID (vector + FTS, RRF-fused) search must observe the
//! transaction's own staged document writes (read-your-own-writes for the
//! hybrid fusion path).
//!
//! Single-source RYOW already works: an FTS-only query
//! (`sql_transactions_fts_overlay.rs`) and a vector-only query
//! (`sql_transactions_vector_overlay.rs`) both see a document staged earlier in
//! the same transaction. This suite covers the fusion handler: a
//! `rrf_score(vector_distance(...), bm25_score(...))` query in a transaction
//! must fold the transaction's staged writes into BOTH the vector leg and the
//! text leg before RRF, so a same-transaction INSERT is visible, a staged
//! UPDATE is re-scored, and a staged DELETE / ROLLBACK is excluded — all before
//! COMMIT. Pre-fix the hybrid handler never consulted the overlay and the
//! staged write was invisible to the fused result.
//!
//! Observable note: the hybrid/RRF response projects a per-row RRF `score`
//! plus `id`, resolved from the internal surrogate `doc_id` via the same
//! catalog lookup the Vector plan path uses (see
//! `response_translate::text_hybrid::translate_hybrid_search_payload`). These
//! tests still observe RYOW through what is robust to staged-write timing
//! rather than through `id` directly: the COUNT of fused rows (an INSERT of a
//! uniquely-matching doc adds one fused row; a DELETE removes one) and the
//! multiset of RRF SCORES (a staged UPDATE re-scores the fusion, changing the
//! scores; pre-fix the staged write is ignored and the scores are identical
//! to the committed-only baseline). Each assertion is a delta against a
//! baseline captured in the same test, so it is robust to how many committed
//! rows the fusion returns.

mod common;

use common::pgwire_harness::TestServer;

async fn create_hybrid_collection(server: &TestServer, name: &str) {
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
    server
        .exec(&format!(
            "CREATE SEARCH INDEX idx_{name}_fts ON {name} FIELDS content ANALYZER 'standard'"
        ))
        .await
        .unwrap();
}

async fn insert_doc(server: &TestServer, coll: &str, id: &str, content: &str, emb: &[f32; 4]) {
    let arr = emb
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",");
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, content, embedding) \
             VALUES ('{id}', '{content}', ARRAY[{arr}])"
        ))
        .await
        .unwrap();
}

/// Run the canonical hybrid read (`rrf_score(vector_distance(...),
/// bm25_score(...))` projected as a score column — the SELECT-projection hybrid
/// form exercised by `sql_hybrid_search.rs`) and return the per-row RRF score
/// cells as text. The `id` column is projected too but comes back empty on the
/// hybrid path (see module doc), so identity is never read; the returned Vec's
/// LENGTH is the fused row count and its CONTENTS are the fused RRF scores.
async fn hybrid_scores(
    server: &TestServer,
    coll: &str,
    qvec: &[f32; 4],
    term: &str,
) -> Vec<String> {
    let arr = qvec
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let rows = server
        .query_rows(&format!(
            "SELECT id, \
                    rrf_score(\
                      vector_distance(embedding, ARRAY[{arr}]), \
                      bm25_score(content, '{term}')\
                    ) AS score \
             FROM {coll} LIMIT 10"
        ))
        .await
        .unwrap();
    let scores: Vec<String> = rows.into_iter().map(|r| r[1].clone()).collect();
    // Every fused row must carry a non-empty RRF score — the score column is
    // the reliable observable on the hybrid path.
    for s in &scores {
        assert!(
            !s.trim().is_empty(),
            "fused row must carry a non-null RRF score: {scores:?}"
        );
    }
    scores
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// BEGIN; INSERT a doc that uniquely matches a query term (present in no
/// committed doc); the in-transaction hybrid query must gain exactly one fused
/// row for it; COMMIT keeps it. Pre-fix: the staged row was invisible, so the
/// in-txn fused-row count equalled the committed-only baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_insert_adds_fused_row_in_txn_then_commit() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hyb_ov_ins").await;
    insert_doc(
        &server,
        "hyb_ov_ins",
        "a",
        "consensus algorithm",
        &[0.1, 0.2, 0.3, 0.4],
    )
    .await;
    insert_doc(
        &server,
        "hyb_ov_ins",
        "b",
        "distributed consensus",
        &[0.2, 0.3, 0.4, 0.5],
    )
    .await;

    let qvec = [0.15, 0.25, 0.35, 0.45];
    // "elephant" matches no committed doc's text.
    let baseline = hybrid_scores(&server, "hyb_ov_ins", &qvec, "elephant").await;

    server.exec("BEGIN").await.unwrap();
    insert_doc(&server, "hyb_ov_ins", "new1", "elephant consensus", &qvec).await;

    let in_txn = hybrid_scores(&server, "hyb_ov_ins", &qvec, "elephant").await;
    assert_eq!(
        in_txn.len(),
        baseline.len() + 1,
        "staged INSERT of a uniquely-matching doc must add exactly one fused row in-txn \
         (baseline {}, in-txn {}); pre-fix the staged row is invisible and the counts are equal",
        baseline.len(),
        in_txn.len()
    );

    server.client.simple_query("COMMIT").await.unwrap();
    let after = hybrid_scores(&server, "hyb_ov_ins", &qvec, "elephant").await;
    assert_eq!(
        after.len(),
        baseline.len() + 1,
        "committed INSERT stays visible to the fused hybrid result: {after:?}"
    );
}

/// BEGIN; UPDATE an existing committed doc so its text newly matches a term
/// present in no committed doc; the in-transaction hybrid query must re-score
/// the staged body into the fusion — changing the fused-row count and/or the
/// RRF score multiset relative to the committed-only baseline. Pre-fix the
/// staged UPDATE is ignored and the fused result is identical to baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_update_rescored_in_hybrid_txn() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hyb_ov_upd").await;
    insert_doc(
        &server,
        "hyb_ov_upd",
        "a",
        "alpha material",
        &[0.1, 0.2, 0.3, 0.4],
    )
    .await;
    insert_doc(
        &server,
        "hyb_ov_upd",
        "b",
        "beta material",
        &[0.2, 0.3, 0.4, 0.5],
    )
    .await;

    let qvec = [0.1, 0.2, 0.3, 0.4];
    // "gamma" matches nothing committed.
    let baseline = sorted(hybrid_scores(&server, "hyb_ov_upd", &qvec, "gamma").await);

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE hyb_ov_upd SET content = 'gamma gamma gamma' WHERE id = 'b'")
        .await
        .unwrap();

    let in_txn = sorted(hybrid_scores(&server, "hyb_ov_upd", &qvec, "gamma").await);
    assert_ne!(
        in_txn, baseline,
        "staged UPDATE must be re-scored into the fusion (fused rows/scores must change vs the \
         committed-only baseline); pre-fix the staged write is ignored and the results are identical"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after = sorted(hybrid_scores(&server, "hyb_ov_upd", &qvec, "gamma").await);
    assert_eq!(
        after, baseline,
        "ROLLBACK restores the committed-only fused result: {after:?} vs {baseline:?}"
    );
}

/// BEGIN; DELETE a committed doc that uniquely matches a term; the
/// in-transaction hybrid query must drop exactly one fused row (the staged
/// tombstone removes it from both legs); ROLLBACK restores it. Pre-fix the
/// committed row leaked through the fusion despite the staged delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_delete_drops_fused_row_in_txn_then_rollback_restores() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hyb_ov_del").await;
    insert_doc(
        &server,
        "hyb_ov_del",
        "a",
        "unicorn algorithm",
        &[0.1, 0.2, 0.3, 0.4],
    )
    .await;
    insert_doc(
        &server,
        "hyb_ov_del",
        "b",
        "distributed consensus",
        &[0.2, 0.3, 0.4, 0.5],
    )
    .await;

    let qvec = [0.1, 0.2, 0.3, 0.4];
    // "unicorn" uniquely matches committed doc 'a'.
    let baseline = hybrid_scores(&server, "hyb_ov_del", &qvec, "unicorn").await;
    assert!(
        !baseline.is_empty(),
        "baseline hybrid must match committed doc 'a' on its unique term"
    );

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DELETE FROM hyb_ov_del WHERE id = 'a'")
        .await
        .unwrap();

    let in_txn = hybrid_scores(&server, "hyb_ov_del", &qvec, "unicorn").await;
    assert_eq!(
        in_txn.len(),
        baseline.len() - 1,
        "staged DELETE must drop exactly one fused row in-txn (baseline {}, in-txn {}); \
         pre-fix the committed row leaks through the fusion",
        baseline.len(),
        in_txn.len()
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after = hybrid_scores(&server, "hyb_ov_del", &qvec, "unicorn").await;
    assert_eq!(
        after.len(),
        baseline.len(),
        "ROLLBACK restores the deleted doc's fused row: {after:?}"
    );
}

/// Autocommit (no transaction) hybrid query behaviour must be unchanged: a
/// committed doc matching a unique term produces a fused row with a non-null
/// score, exactly as before the fix. This must hold independent of the staging
/// path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_hybrid_unchanged() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hyb_ov_auto").await;
    insert_doc(
        &server,
        "hyb_ov_auto",
        "a",
        "consensus algorithm",
        &[0.1, 0.2, 0.3, 0.4],
    )
    .await;
    insert_doc(
        &server,
        "hyb_ov_auto",
        "b",
        "distributed consensus",
        &[0.2, 0.3, 0.4, 0.5],
    )
    .await;

    // "algorithm" uniquely matches committed doc 'a'.
    let scores = hybrid_scores(&server, "hyb_ov_auto", &[0.1, 0.2, 0.3, 0.4], "algorithm").await;
    assert!(
        !scores.is_empty(),
        "autocommit hybrid must return a fused row for the committed doc matching its term: {scores:?}"
    );
}
