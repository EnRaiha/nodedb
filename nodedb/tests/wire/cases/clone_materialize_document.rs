// SPDX-License-Identifier: BUSL-1.1

//! `ALTER DATABASE clone MATERIALIZE` for Document engine collections.
//!
//! Verifies that a CLONE of a default-engine (Document schemaless) collection
//! can be fully materialized: all 20 source rows are readable from the clone
//! after `MATERIALIZE`, and a second `MATERIALIZE` is idempotent.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread")]
async fn document_clone_materialize_copies_all_source_rows() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Create the source database and a Document-engine collection (no WITH clause
    // → default engine, which is Document schemaless).
    client
        .simple_query("CREATE DATABASE doc_mat_src")
        .await
        .expect("CREATE DATABASE doc_mat_src");
    client
        .simple_query("USE DATABASE doc_mat_src")
        .await
        .expect("USE doc_mat_src");
    client
        .simple_query(
            "CREATE COLLECTION events \
             (id STRING PRIMARY KEY, payload STRING)",
        )
        .await
        .expect("CREATE COLLECTION events");

    for i in 0..20u32 {
        client
            .simple_query(&format!(
                "INSERT INTO events (id, payload) VALUES ('e{i}', 'data{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT e{i}: {e}"));
    }

    // Clone the source database (CoW shadow; no data copy yet).
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE doc_mat_clone FROM doc_mat_src")
        .await
        .expect("CLONE DATABASE");

    // Materialize: bulk-copy all source rows into the clone.
    client
        .simple_query("ALTER DATABASE doc_mat_clone MATERIALIZE")
        .await
        .expect("ALTER DATABASE doc_mat_clone MATERIALIZE");

    // All 20 rows must be readable from the materialized clone.
    client
        .simple_query("USE DATABASE doc_mat_clone")
        .await
        .expect("USE doc_mat_clone");

    let rows = client
        .simple_query("SELECT id FROM events")
        .await
        .expect("SELECT id FROM events in clone");

    let count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        count, 20,
        "all 20 source rows must be readable in the materialized document clone"
    );

    // Idempotency: a second MATERIALIZE on an already-Materialized database is
    // a no-op and must not return an error.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("ALTER DATABASE doc_mat_clone MATERIALIZE")
        .await
        .expect("second MATERIALIZE must be idempotent");
}
