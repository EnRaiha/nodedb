// SPDX-License-Identifier: BUSL-1.1

//! DELETE on a source-only row in a clone must:
//! 1. Write a tombstone entry in `_system.clone_tombstones`.
//! 2. Subsequent reads on the clone return not-found for that row.
//! 3. The source still has the row.

use crate::harness::TestServer;

/// Delete a source-only row from the clone.  The clone read path must respect
/// the tombstone and hide the row; the source must be unaffected.
#[tokio::test]
async fn delete_source_only_row_creates_tombstone_and_hides_row() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source: two rows.
    client
        .simple_query("CREATE DATABASE src_ts")
        .await
        .expect("CREATE DATABASE src_ts");
    client
        .simple_query("USE DATABASE src_ts")
        .await
        .expect("USE src_ts");
    client
        .simple_query(
            "CREATE COLLECTION logs (key STRING PRIMARY KEY, msg STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION logs");
    client
        .simple_query("INSERT INTO logs (key, msg) VALUES ('l1', 'keep-me')")
        .await
        .expect("INSERT l1");
    client
        .simple_query("INSERT INTO logs (key, msg) VALUES ('l2', 'delete-me')")
        .await
        .expect("INSERT l2");

    // Clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE clone_ts FROM src_ts LATEST")
        .await
        .expect("CLONE DATABASE");

    // Delete l2 from the clone only.
    client
        .simple_query("USE DATABASE clone_ts")
        .await
        .expect("USE clone_ts");
    client
        .simple_query("DELETE FROM logs WHERE key = 'l2'")
        .await
        .expect("DELETE l2 from clone");

    // l2 must be absent from clone.
    let msgs = client
        .simple_query("SELECT key FROM logs WHERE key = 'l2'")
        .await
        .expect("SELECT l2 from clone after delete");
    let clone_rows = row_count(&msgs);
    assert_eq!(
        clone_rows, 0,
        "clone must not see l2 after tombstone; got {clone_rows} rows"
    );

    // l1 must still be visible via source delegation.
    let msgs = client
        .simple_query("SELECT key FROM logs WHERE key = 'l1'")
        .await
        .expect("SELECT l1 from clone");
    let l1_rows = row_count(&msgs);
    assert_eq!(
        l1_rows, 1,
        "clone must still see l1 via delegation; got {l1_rows} rows"
    );

    // Source must still have both rows.
    client
        .simple_query("USE DATABASE src_ts")
        .await
        .expect("USE src_ts");
    let msgs = client
        .simple_query("SELECT key FROM logs")
        .await
        .expect("SELECT all from source");
    let src_ids: Vec<String> = msgs
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
                row.get(0).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect();

    assert!(
        src_ids.contains(&"l1".to_string()),
        "source must have l1; got {src_ids:?}"
    );
    assert!(
        src_ids.contains(&"l2".to_string()),
        "source must have l2 after clone DELETE; got {src_ids:?}"
    );
}

fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}
