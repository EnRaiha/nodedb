// SPDX-License-Identifier: BUSL-1.1

//! Acceptance gate: re-reading a clone must NOT re-scan the full source on
//! every query.
//!
//! After a clone is created, repeated reads on the same key in the clone must
//! all return consistent results (no regression from materialisation bugs that
//! could return different values across invocations).

use crate::harness::TestServer;

fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

/// The same point-read on a clone must return an identical value across
/// multiple successive queries.
#[tokio::test]
async fn repeated_clone_read_returns_consistent_value() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE sc_src")
        .await
        .expect("CREATE DATABASE sc_src");
    client
        .simple_query("USE DATABASE sc_src")
        .await
        .expect("USE sc_src");
    client
        .simple_query(
            "CREATE COLLECTION stable (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION stable");
    client
        .simple_query("INSERT INTO stable (k, v) VALUES ('anchor', 'constant')")
        .await
        .expect("INSERT anchor");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE sc_clone FROM sc_src")
        .await
        .expect("CLONE sc_src");

    client
        .simple_query("USE DATABASE sc_clone")
        .await
        .expect("USE sc_clone");

    // Read the same key 10 times; must always return 'constant'.
    for round in 0..10u32 {
        let msgs = client
            .simple_query("SELECT v FROM stable WHERE k = 'anchor'")
            .await
            .expect("SELECT anchor");
        assert_eq!(
            first_value(&msgs).as_deref(),
            Some("constant"),
            "round {round}: clone read must return consistent value"
        );
    }
}

/// After writing to the clone, the same point-read must return the new value
/// consistently (not sometimes old, sometimes new).
#[tokio::test]
async fn clone_write_then_repeated_read_is_stable() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE sc2_src")
        .await
        .expect("CREATE DATABASE sc2_src");
    client
        .simple_query("USE DATABASE sc2_src")
        .await
        .expect("USE sc2_src");
    client
        .simple_query("CREATE COLLECTION items (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION items");
    client
        .simple_query("INSERT INTO items (k, v) VALUES ('key1', 'original')")
        .await
        .expect("INSERT key1");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE sc2_clone FROM sc2_src")
        .await
        .expect("CLONE sc2_src");

    // Overwrite in clone.
    client
        .simple_query("USE DATABASE sc2_clone")
        .await
        .expect("USE sc2_clone");
    client
        .simple_query("UPDATE items SET v = 'updated' WHERE k = 'key1'")
        .await
        .expect("UPDATE key1 in clone");

    // Repeated reads must all return 'updated'.
    for round in 0..5u32 {
        let msgs = client
            .simple_query("SELECT v FROM items WHERE k = 'key1'")
            .await
            .expect("SELECT key1");
        assert_eq!(
            first_value(&msgs).as_deref(),
            Some("updated"),
            "round {round}: clone read after write must be stable"
        );
    }
}
