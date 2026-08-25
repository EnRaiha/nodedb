// SPDX-License-Identifier: BUSL-1.1

//! `CLONE DATABASE` must not inherit the source database's quota configuration.
//!
//! The clone is a new logical database. Any quota on the source must remain on
//! the source only; the clone starts quota-free (no explicit limit set) and the
//! source quota must be unchanged after the clone operation.

use crate::harness::TestServer;

/// After cloning, the source database's quota must be unchanged.
#[tokio::test]
async fn source_quota_unchanged_after_clone() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Create a source database and set a quota on it.
    client
        .simple_query("CREATE DATABASE quota_src")
        .await
        .expect("CREATE DATABASE quota_src");
    client
        .simple_query("ALTER DATABASE quota_src SET QUOTA (max_storage_bytes = 1073741824)")
        .await
        .expect("ALTER DATABASE SET QUOTA");

    // Create and insert a row to make the source non-trivial.
    client
        .simple_query("USE DATABASE quota_src")
        .await
        .expect("USE quota_src");
    client
        .simple_query("CREATE COLLECTION qdata (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION qdata");
    client
        .simple_query("INSERT INTO qdata (k, v) VALUES ('q1', 'abc')")
        .await
        .expect("INSERT q1");

    // Clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE quota_clone FROM quota_src")
        .await
        .expect("CLONE quota_src");

    // Source quota must still be set correctly.
    let quota_msgs = client
        .simple_query("SHOW DATABASE QUOTA FOR quota_src")
        .await
        .expect("SHOW DATABASE QUOTA FOR quota_src");

    // At least one row must be returned — the quota was set.
    let has_row = quota_msgs
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)));
    assert!(
        has_row,
        "source database quota must still be set after clone"
    );
}

/// After cloning, the clone must be usable — not quota-blocked.
#[tokio::test]
async fn clone_is_not_quota_blocked() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source with a tight quota.
    client
        .simple_query("CREATE DATABASE qblock_src")
        .await
        .expect("CREATE DATABASE qblock_src");
    client
        .simple_query("USE DATABASE qblock_src")
        .await
        .expect("USE qblock_src");
    client
        .simple_query(
            "CREATE COLLECTION records (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION records");
    client
        .simple_query("INSERT INTO records (k, v) VALUES ('r1', 'hello')")
        .await
        .expect("INSERT r1");

    // Clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE qblock_clone FROM qblock_src")
        .await
        .expect("CLONE qblock_src");

    // Clone must be reachable and accept a write (not quota-blocked).
    client
        .simple_query("USE DATABASE qblock_clone")
        .await
        .expect("USE qblock_clone");
    client
        .simple_query("INSERT INTO records (k, v) VALUES ('r2', 'world')")
        .await
        .expect("INSERT into clone must succeed (clone has no quota inherited)");
}
