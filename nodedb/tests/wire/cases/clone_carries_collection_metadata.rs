// SPDX-License-Identifier: BUSL-1.1

//! `CLONE DATABASE` must carry the source's per-collection catalog rows into
//! the clone, not only the shadow collection descriptors.
//!
//! Vector model rows, vector index parameters, and ANALYZE column statistics
//! are each keyed by `database_id`. A clone that copies only the descriptors
//! resolves its collection names but holds none of these rows. No vector
//! index builds, and the planner costs every scan with no statistics.
//!
//! Both probes read database-scoped catalog rows, so each answers empty in a
//! clone that copied nothing. `SHOW VECTOR MODELS` reads
//! `list_vector_models(database_id, tenant_id)`. `SHOW INDEXES` reads the
//! index registry under the clone's own id.

use crate::harness::TestServer;

/// The collections `SHOW VECTOR MODELS` reports for the session's current
/// database. The list is database-scoped, so a clone that copied nothing
/// answers with none.
async fn listed_models(server: &TestServer) -> Vec<String> {
    server
        .query_text("SHOW VECTOR MODELS")
        .await
        .expect("SHOW VECTOR MODELS must succeed")
}

/// One column's stored model metadata, as the JSON `VECTOR_METADATA` returns.
async fn model_json(server: &TestServer) -> String {
    server
        .query_text("SELECT VECTOR_METADATA('chunks', 'embedding')")
        .await
        .expect("VECTOR_METADATA must succeed")
        .join("")
}

/// A clone answers `SHOW VECTOR MODELS` with the source's rows, under its own
/// database id, and the source keeps its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clone_carries_the_source_vector_model_rows() {
    let server = TestServer::start().await;

    server.exec("CREATE DATABASE meta_src").await.unwrap();
    server.exec("USE DATABASE meta_src").await.unwrap();
    server
        .exec("CREATE COLLECTION chunks (id TEXT PRIMARY KEY, body TEXT, embedding FLOAT[])")
        .await
        .unwrap();
    server
        .exec(
            "ALTER COLLECTION chunks SET VECTOR METADATA ON embedding \
             (model = 'all-MiniLM-L6-v2', dimensions = 384)",
        )
        .await
        .expect("SET VECTOR METADATA must succeed");

    // The source holds the row it just declared.
    let declared = model_json(&server).await;
    assert!(
        declared.contains("all-MiniLM-L6-v2") && declared.contains("384"),
        "the source must report the model it declared: {declared}"
    );

    server.exec("USE DATABASE default").await.unwrap();
    server
        .exec("CLONE DATABASE meta_clone FROM meta_src")
        .await
        .unwrap();

    // The clone's own rows carry the source's model, under the clone's id.
    server.exec("USE DATABASE meta_clone").await.unwrap();
    let in_clone = listed_models(&server).await;
    assert!(
        in_clone.iter().any(|c| c == "chunks"),
        "the clone must carry the source's vector model row: {in_clone:?}"
    );
    let cloned = model_json(&server).await;
    assert!(
        cloned.contains("all-MiniLM-L6-v2") && cloned.contains("384"),
        "the clone's row must keep the model and its dimensions: {cloned}"
    );

    // The copy writes new rows; it never moves the source's.
    server.exec("USE DATABASE meta_src").await.unwrap();
    let kept = model_json(&server).await;
    assert!(
        kept.contains("all-MiniLM-L6-v2"),
        "the source must keep its own vector model row: {kept}"
    );
}

/// The clone lists the source's vector index, from its own copied index
/// registry rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clone_lists_the_source_vector_index() {
    let server = TestServer::start().await;

    server.exec("CREATE DATABASE idx_src").await.unwrap();
    server.exec("USE DATABASE idx_src").await.unwrap();
    server
        .exec("CREATE COLLECTION chunks (id TEXT PRIMARY KEY, body TEXT, embedding FLOAT[])")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX chunks_embedding_idx ON chunks (embedding) METRIC cosine DIM 3")
        .await
        .unwrap();

    server.exec("USE DATABASE default").await.unwrap();
    server
        .exec("CLONE DATABASE idx_clone FROM idx_src")
        .await
        .unwrap();

    server.exec("USE DATABASE idx_clone").await.unwrap();
    let in_clone = server
        .query_text("SHOW INDEXES")
        .await
        .expect("SHOW INDEXES must succeed");
    assert!(
        in_clone.iter().any(|n| n == "chunks_embedding_idx"),
        "the clone must list the source's vector index: {in_clone:?}"
    );
}
