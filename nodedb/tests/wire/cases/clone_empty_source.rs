// SPDX-License-Identifier: BUSL-1.1

//! Cloning an empty (zero-row, zero-collection) source database must succeed
//! and produce an equally empty clone.

use crate::harness::TestServer;

fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// Clone an empty source database and confirm the clone has no rows.
#[tokio::test]
async fn clone_empty_source_produces_empty_clone() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source: a database with a collection but no rows.
    client
        .simple_query("CREATE DATABASE empty_src")
        .await
        .expect("CREATE DATABASE empty_src");
    client
        .simple_query("USE DATABASE empty_src")
        .await
        .expect("USE empty_src");
    client
        .simple_query(
            "CREATE COLLECTION things (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION things");

    // Clone the empty source.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE empty_clone FROM empty_src")
        .await
        .expect("CLONE empty_src");

    // Clone must be reachable and return zero rows.
    client
        .simple_query("USE DATABASE empty_clone")
        .await
        .expect("USE empty_clone");
    let msgs = client
        .simple_query("SELECT k FROM things")
        .await
        .expect("SELECT from empty clone");
    assert_eq!(
        row_count(&msgs),
        0,
        "clone of empty source must have zero rows"
    );
}

/// Clone a database that has no collections at all (bare database, no schema).
#[tokio::test]
async fn clone_bare_database_succeeds() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Bare source: just a database, no collections.
    client
        .simple_query("CREATE DATABASE bare_src")
        .await
        .expect("CREATE DATABASE bare_src");

    // Clone should succeed.
    client
        .simple_query("CLONE DATABASE bare_clone FROM bare_src")
        .await
        .expect("CLONE bare_src should succeed even with no collections");

    // The clone database must exist and be usable.
    client
        .simple_query("USE DATABASE bare_clone")
        .await
        .expect("USE bare_clone must succeed");
}
