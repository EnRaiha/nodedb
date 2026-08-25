// SPDX-License-Identifier: BUSL-1.1

//! `DROP DATABASE source FORCE` orphan-protection test.
//!
//! With dependent clones present, FORCE must materialize them first and
//! then drop the source. The clone must remain readable with all source
//! rows visible after the source is gone.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread")]
async fn drop_source_force_materializes_clone_and_drops_source() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE force_src")
        .await
        .expect("CREATE DATABASE force_src");
    client
        .simple_query("USE DATABASE force_src")
        .await
        .expect("USE force_src");
    client
        .simple_query(
            "CREATE COLLECTION records \
             (key STRING PRIMARY KEY, data STRING) \
             WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION records");
    client
        .simple_query("INSERT INTO records (key, data) VALUES ('r1', 'hello')")
        .await
        .expect("INSERT r1");
    client
        .simple_query("INSERT INTO records (key, data) VALUES ('r2', 'world')")
        .await
        .expect("INSERT r2");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE force_clone FROM force_src")
        .await
        .expect("CLONE DATABASE");

    client
        .simple_query("DROP DATABASE force_src FORCE")
        .await
        .expect("DROP DATABASE force_src FORCE should succeed");

    // Source is gone.
    let res = client.simple_query("USE DATABASE force_src").await;
    assert!(
        res.is_err(),
        "source database must be gone after FORCE drop"
    );

    // Clone is intact, every source row is readable.
    client
        .simple_query("USE DATABASE force_clone")
        .await
        .expect("force_clone must be accessible after source drop");
    let rows = client
        .simple_query("SELECT key FROM records")
        .await
        .expect("SELECT on force_clone.records");
    let count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(count, 2, "both rows must survive in the materialized clone");
}
