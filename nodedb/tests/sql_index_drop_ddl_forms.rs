// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the kind-qualified drop statements documented in
//! `docs/query-language.md`: `DROP VECTOR INDEX`, `DROP FULLTEXT INDEX`,
//! `DROP SPATIAL INDEX`.
//!
//! Each kind has a `CREATE <KIND> INDEX` statement of its own, and the docs
//! pair every one with a matching drop, so the create surface and the drop
//! surface must cover the same set of kinds. Each test runs the documented
//! form end to end: the index is created, the kind-qualified drop is accepted,
//! and `SHOW INDEXES` no longer lists it. A kind-qualified drop naming an
//! index of another kind is rejected rather than honoured — see
//! `sql_index_lifecycle.rs`.

mod common;

use common::pgwire_harness::TestServer;

/// Index names listed by `SHOW INDEXES` for the current tenant.
async fn listed_indexes(server: &TestServer) -> Vec<String> {
    server
        .query_text("SHOW INDEXES")
        .await
        .expect("SHOW INDEXES must succeed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_vector_index_statement_drops_the_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION dvi_docs (id TEXT PRIMARY KEY, embedding FLOAT[])")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX dvi_docs_idx ON dvi_docs (embedding) METRIC cosine DIM 3")
        .await
        .unwrap();

    server
        .exec("DROP VECTOR INDEX dvi_docs_idx")
        .await
        .expect("documented DROP VECTOR INDEX form must be accepted");

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n == "dvi_docs_idx"),
        "DROP VECTOR INDEX must remove the index it names: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_vector_index_if_exists_tolerates_an_unknown_name() {
    let server = TestServer::start().await;
    server
        .exec("DROP VECTOR INDEX IF EXISTS dvi_never_created")
        .await
        .expect("documented DROP VECTOR INDEX IF EXISTS form must be a no-op for an absent index");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_fulltext_index_statement_drops_the_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION dfi_docs (id TEXT PRIMARY KEY, body TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE FULLTEXT INDEX idx_dfi_docs_body ON dfi_docs (body)")
        .await
        .unwrap();
    let name = listed_indexes(&server)
        .await
        .into_iter()
        .find(|n| n.contains("dfi_docs"))
        .expect("the created full-text index must be listed");

    server
        .exec(&format!("DROP FULLTEXT INDEX {name}"))
        .await
        .expect("documented DROP FULLTEXT INDEX form must be accepted");

    let after = listed_indexes(&server).await;
    assert!(
        !after.contains(&name),
        "DROP FULLTEXT INDEX must remove the index it names: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_spatial_index_statement_drops_the_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION dsi_places (id TEXT PRIMARY KEY, location TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE SPATIAL INDEX dsi_places_idx ON dsi_places (location) USING RTREE")
        .await
        .unwrap();

    server
        .exec("DROP SPATIAL INDEX dsi_places_idx")
        .await
        .expect("documented DROP SPATIAL INDEX form must be accepted");

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n == "dsi_places_idx"),
        "DROP SPATIAL INDEX must remove the index it names: {after:?}"
    );
}
