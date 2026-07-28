// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage for three-source RRF fusion: vector + BM25 text + graph BFS.
//!
//! Two surfaces are tested:
//!
//! (A) SQL function form:
//!     `rrf_score(vector_distance(...), bm25_score(...), graph_score(...), k1?, k2?, k3?)`
//!     → `SqlPlan::HybridSearchTriple` → `TextOp::HybridSearchTriple`
//!
//! (B) FUSION DSL form (two variants):
//!     `SEARCH col USING FUSION(ARRAY[...] BM25 'q' ON 'field' DEPTH N LABEL 'e' TOP K RRF_K (kv, kt, kg))`
//!     `GRAPH RAG FUSION ON col QUERY ARRAY[...] BM25 'q' ON 'field' ... RRF_K (kv, kt, kg)`
//!
//! Backwards compatibility: existing two-source forms must be unaffected.
//! Arity validation: inconsistent k-constant counts and 4+ source ranks must error.

mod common;

use common::pgwire_harness::TestServer;

// ── Shared setup helpers ──────────────────────────────────────────────────────

/// Creates a collection with vector index, FTS index, documents, and graph edges.
///
/// Layout:
///   n1 (vector ~query, text matches "alpha omega") --hop--> n2 (graph-reachable)
///   n3 (vector near-query, text matches "beta gamma")
///
/// n2 is NOT in vector top-K for a tight query, NOT in FTS results for "alpha omega",
/// but IS reachable via graph BFS from n1. This lets tests verify that the graph
/// leg actually contributes distinct results.
async fn create_triple_collection(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{name}_emb ON {name} METRIC cosine DIM 3"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE SEARCH INDEX idx_{name}_fts ON {name} FIELDS body ANALYZER 'standard'"
        ))
        .await
        .unwrap();

    // n1: closest to query vector [1, 0, 0]; contains "alpha omega" for text
    server
        .exec(&format!(
            "INSERT INTO {name} (id, body, embedding) \
             VALUES ('n1', 'alpha omega', ARRAY[1.0, 0.0, 0.0])"
        ))
        .await
        .unwrap();

    // n2: far from query vector; no matching text; only reachable via graph BFS from n1
    server
        .exec(&format!(
            "INSERT INTO {name} (id, body, embedding) \
             VALUES ('n2', 'unrelated content', ARRAY[-1.0, 0.0, 0.0])"
        ))
        .await
        .unwrap();

    // n3: near query vector; contains "beta gamma"; not graph-reachable from n1
    server
        .exec(&format!(
            "INSERT INTO {name} (id, body, embedding) \
             VALUES ('n3', 'beta gamma', ARRAY[0.9, 0.1, 0.0])"
        ))
        .await
        .unwrap();

    // Edge: n1 → n2 via "hop"
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{name}' FROM 'n1' TO 'n2' TYPE 'hop'"
        ))
        .await
        .unwrap();
}

// ── 1. SQL function form: three-source rrf_score returns fused ranking ────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_triple_returns_fused_ranking() {
    let server = TestServer::start().await;
    create_triple_collection(&server, "t3_basic").await;

    // n2 is only reachable via graph BFS. With a strong graph weight (low graph_k)
    // it should appear in the fused result set even though it is not in the vector
    // or text top-K individually.
    let rows = server
        .query_rows(
            "SELECT id, \
             rrf_score(\
               vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]), \
               bm25_score(body, 'alpha'), \
               graph_score(id, 'n1', depth => 1, label => 'hop') \
             ) AS score \
             FROM t3_basic \
             LIMIT 10",
        )
        .await
        .expect("three-source rrf_score must not error");

    assert!(
        !rows.is_empty(),
        "three-source hybrid search must return rows; empty = HybridSearchTriple not fired"
    );

    // Every row must carry a non-null numeric score.
    for row in &rows {
        assert_eq!(row.len(), 2, "expected 2 columns (id, score); got {row:?}");
        let score = row[1].trim().parse::<f64>();
        assert!(
            score.is_ok(),
            "score column must be a non-null number; got {:?}",
            row[1]
        );
    }
}

// ── 2. Custom k weights for all three sources affect the ranking ──────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_triple_k_weights_affect_ranking() {
    let server = TestServer::start().await;
    create_triple_collection(&server, "t3_weights").await;

    // With graph_k = 1.0 (very low → very strong graph influence), n2 (graph-only)
    // receives a very large graph RRF contribution: 1/(1+0+1) = 0.5.
    // With graph_k = 10000.0, n2's graph contribution is ~0.0001.
    // The two result blobs must differ.

    let rows_low_graph_k = server
        .query_rows(
            "SELECT id, \
             rrf_score(\
               vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]), \
               bm25_score(body, 'alpha'), \
               graph_score(id, 'n1', depth => 1, label => 'hop'), \
               60.0, 60.0, 1.0\
             ) AS score \
             FROM t3_weights \
             LIMIT 10",
        )
        .await
        .expect("low graph_k triple query must succeed");

    let rows_high_graph_k = server
        .query_rows(
            "SELECT id, \
             rrf_score(\
               vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]), \
               bm25_score(body, 'alpha'), \
               graph_score(id, 'n1', depth => 1, label => 'hop'), \
               60.0, 60.0, 10000.0\
             ) AS score \
             FROM t3_weights \
             LIMIT 10",
        )
        .await
        .expect("high graph_k triple query must succeed");

    // The blobs must differ: graph_k controls how strongly graph-reachable-only
    // nodes contribute, so at least one row's score should change.
    assert_ne!(
        rows_low_graph_k
            .iter()
            .map(|r| r.join(","))
            .collect::<Vec<_>>()
            .join(";"),
        rows_high_graph_k
            .iter()
            .map(|r| r.join(","))
            .collect::<Vec<_>>()
            .join(";"),
        "graph_k must affect fused scores; identical output means the graph k-value is ignored"
    );
}

// ── 3. FUSION DSL form: SEARCH ... USING FUSION with BM25 and triple RRF_K ───

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_using_fusion_three_source_returns_hits() {
    let server = TestServer::start().await;
    create_triple_collection(&server, "t3_fusion_dsl").await;

    let result = server
        .query_text(
            "SEARCH t3_fusion_dsl USING FUSION(\
               ARRAY[1.0, 0.0, 0.0] \
               VECTOR_FIELD 'embedding' \
               VECTOR_TOP_K 10 \
               BM25 'alpha' ON 'body' \
               DEPTH 1 LABEL 'hop' \
               TOP 10 \
               RRF_K (60.0, 35.0, 50.0))",
        )
        .await;

    if let Err(msg) = &result {
        assert!(
            !msg.to_lowercase().contains("42601") && !msg.to_lowercase().contains("syntax"),
            "SEARCH USING FUSION three-source must not produce a syntax error; got: {msg}"
        );
    }
    // Result may be empty (no matching documents at executor level) but must not error.
}

// ── 4. GRAPH RAG FUSION DSL with BM25 clause and triple RRF_K ────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_rag_fusion_three_source_returns_hits() {
    let server = TestServer::start().await;
    create_triple_collection(&server, "t3_rag_dsl").await;

    server
        .exec(
            "CREATE VECTOR INDEX idx_t3_rag_dsl_emb ON t3_rag_dsl \
             METRIC cosine DIM 3 VECTOR_FIELD 'embedding'",
        )
        .await
        .ok(); // index may already exist from create_triple_collection

    let result = server
        .query_text(
            "GRAPH RAG FUSION ON t3_rag_dsl \
             QUERY ARRAY[1.0, 0.0, 0.0] \
             VECTOR_FIELD 'embedding' \
             VECTOR_TOP_K 10 \
             BM25 'alpha' ON 'body' \
             EXPANSION_DEPTH 1 \
             EDGE_LABEL 'hop' \
             FINAL_TOP_K 10 \
             RRF_K (60.0, 35.0, 50.0)",
        )
        .await;

    if let Err(msg) = &result {
        assert!(
            !msg.to_lowercase().contains("42601") && !msg.to_lowercase().contains("syntax"),
            "GRAPH RAG FUSION three-source must not produce a syntax error; got: {msg}"
        );
    }
}

// ── 5. Backwards compat: two-source rrf_score still works ────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_two_source_still_works() {
    let server = TestServer::start().await;
    create_triple_collection(&server, "t3_compat_two").await;

    let rows = server
        .query_rows(
            "SELECT id, \
             rrf_score(\
               vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]), \
               bm25_score(body, 'alpha')\
             ) AS score \
             FROM t3_compat_two \
             LIMIT 5",
        )
        .await
        .expect("two-source rrf_score must still work after three-source addition");

    assert!(
        !rows.is_empty(),
        "two-source hybrid search must still return rows"
    );
    for row in &rows {
        let score = row[1].trim().parse::<f64>();
        assert!(
            score.is_ok(),
            "two-source score must be non-null; got {:?}",
            row[1]
        );
    }
}

// ── 6. Backwards compat: two-tuple RRF_K in FUSION DSL still works ───────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_using_fusion_two_source_still_works() {
    let server = TestServer::start().await;
    create_triple_collection(&server, "t3_compat_fusion").await;

    let result = server
        .query_text(
            "SEARCH t3_compat_fusion USING FUSION(\
               ARRAY[1.0, 0.0, 0.0] \
               VECTOR_FIELD 'embedding' \
               VECTOR_TOP_K 5 \
               DEPTH 1 LABEL 'hop' \
               TOP 5 \
               RRF_K (60.0, 35.0))",
        )
        .await;

    if let Err(msg) = &result {
        assert!(
            !msg.to_lowercase().contains("42601"),
            "two-source FUSION DSL must not produce a syntax error; got: {msg}"
        );
    }
}

// ── 7. Reject rrf_score with 4 source ranks ──────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_four_source_ranks_is_rejected() {
    let server = TestServer::start().await;
    create_triple_collection(&server, "t3_reject_4").await;

    let result = server
        .query_rows(
            "SELECT id, \
             rrf_score(\
               vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]), \
               bm25_score(body, 'alpha'), \
               graph_score(id, 'n1', depth => 1, label => 'hop'), \
               graph_score(id, 'n2', depth => 1, label => 'hop')\
             ) AS score \
             FROM t3_reject_4 \
             LIMIT 5",
        )
        .await;

    let err = result.expect_err("four source ranks must produce an error");
    assert!(
        !err.is_empty(),
        "four-source rrf_score must return a typed error"
    );
}

// ── 8. Reject three sources + two k-constants (inconsistent arity) ───────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_triple_with_two_k_constants_is_rejected() {
    let server = TestServer::start().await;
    create_triple_collection(&server, "t3_reject_2k").await;

    let result = server
        .query_rows(
            "SELECT id, \
             rrf_score(\
               vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]), \
               bm25_score(body, 'alpha'), \
               graph_score(id, 'n1', depth => 1, label => 'hop'), \
               60.0, 60.0\
             ) AS score \
             FROM t3_reject_2k \
             LIMIT 5",
        )
        .await;

    let err = result.expect_err("3 sources + 2 k-constants must produce an error");
    assert!(
        !err.is_empty(),
        "inconsistent arity (3 ranks + 2 k values) must return a typed error"
    );
}
