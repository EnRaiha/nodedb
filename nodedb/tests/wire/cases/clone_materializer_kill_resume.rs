// SPDX-License-Identifier: BUSL-1.1

//! Materializer success-and-idempotency test.
//!
//! Seeds 100 rows into a source KV collection, clones it, materializes,
//! and verifies every source row is readable in the clone. A second
//! MATERIALIZE must be a no-op (status already `Materialized`).

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread")]
async fn materializer_completes_and_rows_are_readable() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE mat_src")
        .await
        .expect("CREATE DATABASE mat_src");
    client
        .simple_query("USE DATABASE mat_src")
        .await
        .expect("USE mat_src");
    client
        .simple_query(
            "CREATE COLLECTION items \
             (key STRING PRIMARY KEY, value STRING) \
             WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION items");

    for i in 0..100u32 {
        client
            .simple_query(&format!(
                "INSERT INTO items (key, value) VALUES ('k{i}', 'v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT k{i}: {e}"));
    }

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE mat_clone FROM mat_src")
        .await
        .expect("CLONE DATABASE");

    client
        .simple_query("ALTER DATABASE mat_clone MATERIALIZE")
        .await
        .expect("ALTER DATABASE mat_clone MATERIALIZE");

    client
        .simple_query("USE DATABASE mat_clone")
        .await
        .expect("USE mat_clone");

    let rows = client
        .simple_query("SELECT key FROM items")
        .await
        .expect("SELECT from mat_clone");

    let count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        count, 100,
        "all 100 source rows must be readable post-materialize"
    );

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("ALTER DATABASE mat_clone MATERIALIZE")
        .await
        .expect("second MATERIALIZE must be idempotent");
}
