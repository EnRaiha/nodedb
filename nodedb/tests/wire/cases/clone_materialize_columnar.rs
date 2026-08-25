// SPDX-License-Identifier: BUSL-1.1

//! `ALTER DATABASE clone MATERIALIZE` for the Plain Columnar engine.
//!
//! Verifies that a CLONE of a columnar collection can be fully materialized:
//! all 20 source rows are readable from the clone after `MATERIALIZE`, and a
//! second `MATERIALIZE` is idempotent.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread")]
async fn columnar_clone_materialize_copies_all_source_rows() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Create source database with a plain columnar collection.
    client
        .simple_query("CREATE DATABASE col_mat_src")
        .await
        .expect("CREATE DATABASE col_mat_src");
    client
        .simple_query("USE DATABASE col_mat_src")
        .await
        .expect("USE col_mat_src");
    client
        .simple_query(
            "CREATE COLLECTION rows \
             COLUMNS (id TEXT, payload TEXT) \
             WITH (engine='columnar')",
        )
        .await
        .expect("CREATE COLLECTION rows");

    for i in 0..20u32 {
        client
            .simple_query(&format!(
                "INSERT INTO rows (id, payload) VALUES ('r{i}', 'data{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT r{i}: {e}"));
    }

    // Clone the source database (CoW shadow — no data copied yet).
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE col_mat_clone FROM col_mat_src")
        .await
        .expect("CLONE DATABASE");

    // Materialize: bulk-copy all source rows into the clone.
    client
        .simple_query("ALTER DATABASE col_mat_clone MATERIALIZE")
        .await
        .expect("ALTER DATABASE col_mat_clone MATERIALIZE");

    // All 20 rows must be readable from the materialized clone.
    client
        .simple_query("USE DATABASE col_mat_clone")
        .await
        .expect("USE col_mat_clone");

    let rows = client
        .simple_query("SELECT id FROM rows")
        .await
        .expect("SELECT id FROM rows in clone");

    let count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        count, 20,
        "all 20 source rows must be readable in the materialized columnar clone"
    );

    // Idempotency: a second MATERIALIZE on an already-Materialized database is
    // a no-op and must not return an error.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("ALTER DATABASE col_mat_clone MATERIALIZE")
        .await
        .expect("second MATERIALIZE must be idempotent");
}
