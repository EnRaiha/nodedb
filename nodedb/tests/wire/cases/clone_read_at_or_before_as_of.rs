// SPDX-License-Identifier: BUSL-1.1

//! Reading from a clone at `T ≤ as_of` must delegate to the source and see the
//! source state.  The clone's target storage is empty for rows that were never
//! written into the clone directly; they come entirely from source delegation.

use crate::harness::TestServer;

/// Insert two rows into source.  Clone it.  Immediately scan the clone — all
/// rows must come from source delegation (target has no own rows yet).
#[tokio::test]
async fn clone_read_delegates_to_source_when_target_empty() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Build source with one row.
    client
        .simple_query("CREATE DATABASE src_aof")
        .await
        .expect("CREATE DATABASE src_aof");
    client
        .simple_query("USE DATABASE src_aof")
        .await
        .expect("USE src_aof");
    client
        .simple_query(
            "CREATE COLLECTION items (key STRING PRIMARY KEY, val STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION items");
    client
        .simple_query("INSERT INTO items (key, val) VALUES ('row1', 'from-source')")
        .await
        .expect("INSERT row1");

    // Clone source.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE clone_aof FROM src_aof LATEST")
        .await
        .expect("CLONE DATABASE");

    // Switch to clone and read — must see source row.
    client
        .simple_query("USE DATABASE clone_aof")
        .await
        .expect("USE clone_aof");

    let msgs = client
        .simple_query("SELECT key, val FROM items WHERE key = 'row1'")
        .await
        .expect("SELECT from clone");

    let mut found = false;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg
            && row.get(0) == Some("row1")
            && row.get(1) == Some("from-source")
        {
            found = true;
        }
    }
    assert!(
        found,
        "clone should delegate read to source and return row1"
    );
}
