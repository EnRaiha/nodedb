// SPDX-License-Identifier: BUSL-1.1

//! After cloning a database and writing new rows into the clone, reads against
//! the clone at `T > as_of` must see the post-clone writes in target storage.

use crate::harness::TestServer;

/// Clone a source DB that has two rows.  Write a third row into the clone only.
/// A full-scan on the clone must return all three rows (two from source via CoW
/// delegation, one from target's own storage).
#[tokio::test]
async fn clone_read_sees_post_clone_writes() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Build source database with two rows.
    client
        .simple_query("CREATE DATABASE src_post")
        .await
        .expect("CREATE DATABASE src_post");
    client
        .simple_query("USE DATABASE src_post")
        .await
        .expect("USE DATABASE src_post");
    client
        .simple_query(
            "CREATE COLLECTION notes (key STRING PRIMARY KEY, body STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION notes");
    client
        .simple_query("INSERT INTO notes (key, body) VALUES ('n1', 'source-row-1')")
        .await
        .expect("INSERT n1");
    client
        .simple_query("INSERT INTO notes (key, body) VALUES ('n2', 'source-row-2')")
        .await
        .expect("INSERT n2");

    // Clone the source at the current LSN.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE DATABASE default");
    client
        .simple_query("CLONE DATABASE clone_post FROM src_post LATEST")
        .await
        .expect("CLONE DATABASE clone_post");

    // Switch to clone and insert a new row that only exists in target.
    client
        .simple_query("USE DATABASE clone_post")
        .await
        .expect("USE DATABASE clone_post");
    client
        .simple_query("INSERT INTO notes (key, body) VALUES ('n3', 'clone-only-row')")
        .await
        .expect("INSERT n3 into clone");

    // Read all rows from the clone — must see source rows + new clone row.
    let msgs = client
        .simple_query("SELECT key FROM notes")
        .await
        .expect("SELECT id FROM notes on clone");

    let mut ids: Vec<String> = Vec::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg
            && let Some(id) = row.get(0)
        {
            ids.push(id.to_string());
        }
    }
    ids.sort();

    assert!(
        ids.contains(&"n1".to_string()),
        "clone should see source row n1 via delegation; got: {ids:?}"
    );
    assert!(
        ids.contains(&"n2".to_string()),
        "clone should see source row n2 via delegation; got: {ids:?}"
    );
    assert!(
        ids.contains(&"n3".to_string()),
        "clone should see post-clone write n3 from target; got: {ids:?}"
    );
}
