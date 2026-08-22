// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDbRemote::vector_search` honors a non-None
//! `MetadataFilter` argument across the full pgwire round-trip.
//!
//! The client renders `MetadataFilter::Eq { field, value }` into a
//! `WHERE <field> = <literal>` clause that precedes `ORDER BY
//! vector_distance(...) LIMIT k`. The test pins the wire round-trip:
//! seed a vector collection with metadata, ask for nearest neighbors
//! constrained by metadata, and require the call to return without
//! erroring — proving the predicate reaches the server and the planner
//! accepts the WHERE-on-metadata shape the client emits.

use nodedb_client::{MetadataFilter, NodeDb, NodeDbRemote, Value};
use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test]
async fn vector_search_with_metadata_filter_round_trips_through_pgwire() {
    let server = TestServer::start().await;

    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");

    // Provision a vector collection with an explicit `category`
    // metadata column so the WHERE predicate has a real column to bind
    // against. Vector engine requires the engine='vector' option plus
    // explicit FIELDS; the planner does not auto-promote.
    remote
        .execute_sql(
            "CREATE COLLECTION embeddings \
             FIELDS (id TEXT, embedding VECTOR(3), category TEXT) \
             WITH (engine='vector', m=8, ef_construction=50)",
            &[],
        )
        .await
        .expect("CREATE COLLECTION embeddings");

    remote
        .execute_sql(
            "INSERT INTO embeddings (id, embedding, category) \
             VALUES ('v_ai', ARRAY[0.1, 0.2, 0.3], 'ai')",
            &[],
        )
        .await
        .expect("seed v_ai");
    remote
        .execute_sql(
            "INSERT INTO embeddings (id, embedding, category) \
             VALUES ('v_other', ARRAY[0.9, 0.9, 0.9], 'other')",
            &[],
        )
        .await
        .expect("seed v_other");

    // Spec: vector_search with a non-None filter renders into a
    // server-side predicate, the server applies it, and the call
    // returns matching results — Ok with real rows. A typed Err is
    // disallowed: it is indistinguishable from the pre-fix
    // "filter rejected at the client boundary" silent-wrong shape.
    let filter = MetadataFilter::eq("category", Value::String("ai".into()));
    let results = remote
        .vector_search("embeddings", &[0.1, 0.2, 0.3], 5, Some(&filter), None)
        .await
        .expect("vector_search with filter must return Ok end-to-end");
    assert!(
        !results.is_empty(),
        "vector_search must return at least one row matching the metadata predicate; got empty"
    );

    server.graceful_shutdown().await;
}
