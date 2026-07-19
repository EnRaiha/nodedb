// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for sparse-vector search.
//!
//! A strict-schema collection (`CREATE TABLE`, document_strict) with a
//! `SPARSEVECTOR` column maintains an inverted index on every INSERT. The
//! `ORDER BY sparse_score(field, '{dim: weight, ...}') DESC LIMIT k` surface
//! routes to `VectorOp::SparseSearch`, returning the `k` documents with the
//! highest dot-product score against the query vector.

mod common;
use common::pgwire_harness::TestServer;

#[tokio::test]
async fn order_by_sparse_score_ranks_by_dot_product() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE sparse_docs (id TEXT PRIMARY KEY, terms SPARSEVECTOR)")
        .await
        .unwrap();

    // Sparse vectors. Dot product against the query `{3: 1.0, 7: 0.5}`:
    //   a {3:1.0, 7:1.0} -> 1.0*1.0 + 1.0*0.5 = 1.5   (rank 1)
    //   b {3:1.0}        -> 1.0*1.0            = 1.0   (rank 2)
    //   c {7:1.0}        -> 1.0*0.5            = 0.5   (rank 3)
    //   d {1:1.0}        -> 0.0 (no shared dimension — excluded from the index scan)
    let rows_in: &[(&str, &str)] = &[
        ("a", "{3: 1.0, 7: 1.0}"),
        ("b", "{3: 1.0}"),
        ("c", "{7: 1.0}"),
        ("d", "{1: 1.0}"),
    ];
    for (id, terms) in rows_in {
        srv.exec(&format!(
            "INSERT INTO sparse_docs (id, terms) VALUES ('{id}', '{terms}')"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows(
            "SELECT id FROM sparse_docs \
             ORDER BY sparse_score(terms, '{3: 1.0, 7: 0.5}') DESC LIMIT 3",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();

    assert_eq!(
        ids,
        vec!["a", "b", "c"],
        "sparse search must return documents ranked by descending dot product; got {ids:?}"
    );
    assert!(
        !ids.contains(&"d"),
        "document with no shared dimension must not appear: {ids:?}"
    );
}
