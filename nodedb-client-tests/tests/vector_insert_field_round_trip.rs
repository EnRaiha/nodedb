// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDb::vector_insert_field` honors the
//! `field_name` argument across the full pgwire round-trip.
//!
//! A trait default that delegates to `vector_insert` and discards
//! `field_name` lands the vector in the wrong column — the silent-
//! wrong pattern this test guards against. The remote override emits
//! `INSERT INTO <coll> (id, <field>) VALUES ...` so the named column
//! receives the vector.

use nodedb_client::{NodeDb, NodeDbRemote};
use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test]
async fn vector_insert_field_must_not_silently_delegate_to_unfielded() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");

    // Provision a vector-primary collection where the indexed vector
    // column is named `body_embedding` (not the implicit `embedding`).
    // `primary='vector'` is the server contract that routes per-row
    // INSERT through `VectorOp::DirectUpsert`, which binds the vector
    // under the named-field HNSW key — exactly the path
    // `vector_insert_field` must target. Plain `engine='vector'`
    // without `primary='vector'` is a schemaless collection that
    // happens to have a VECTOR column: the data lands in the document
    // body, not in a per-field HNSW index, so a field-aware insert is
    // unverifiable in that mode.
    remote
        .execute_sql(
            "CREATE COLLECTION embeddings_multi \
             FIELDS (id TEXT, body_embedding VECTOR(3)) \
             WITH (engine='vector', primary='vector', \
                   vector_field='body_embedding', \
                   dim=3, m=8, ef_construction=50)",
            &[],
        )
        .await
        .expect("CREATE COLLECTION embeddings_multi with named vector_field");

    // Spec: `vector_insert_field("body_embedding", ...)` lands the
    // vector in the named HNSW index on the collection — not in the
    // default unfielded vector index. An `Err("not implemented")`
    // default is correct negative behavior but not the spec — do not
    // soften the assertion to accept `Err`; that locks the broken
    // default in as the contract.
    remote
        .vector_insert_field(
            "embeddings_multi",
            "body_embedding",
            "v1",
            &[0.1, 0.2, 0.3],
            None,
        )
        .await
        .expect("vector_insert_field must land the vector in the named field");

    server.graceful_shutdown().await;
}
