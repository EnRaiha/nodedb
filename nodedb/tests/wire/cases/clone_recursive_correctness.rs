// SPDX-License-Identifier: BUSL-1.1

//! Recursive clone correctness: `CLONE DATABASE clone_of_clone FROM clone`.
//!
//! A clone-of-a-clone must read rows that originate in the original source,
//! and writes in the grandchild clone must not affect either its parent clone
//! or the original source.

use crate::harness::TestServer;

fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

/// Grandchild clone must read through two indirection levels and reach the
/// original source row.
#[tokio::test]
async fn grandchild_clone_reads_original_row() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Original source.
    client
        .simple_query("CREATE DATABASE rec_origin")
        .await
        .expect("CREATE DATABASE rec_origin");
    client
        .simple_query("USE DATABASE rec_origin")
        .await
        .expect("USE rec_origin");
    client
        .simple_query("CREATE COLLECTION items (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION items");
    client
        .simple_query("INSERT INTO items (k, v) VALUES ('root', 'origin-value')")
        .await
        .expect("INSERT root");

    // First-level clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE rec_clone1 FROM rec_origin")
        .await
        .expect("CLONE rec_clone1");

    // Second-level clone (clone of clone).
    client
        .simple_query("CLONE DATABASE rec_clone2 FROM rec_clone1")
        .await
        .expect("CLONE rec_clone2");

    // Grandchild must read the origin row.
    client
        .simple_query("USE DATABASE rec_clone2")
        .await
        .expect("USE rec_clone2");
    let msgs = client
        .simple_query("SELECT v FROM items WHERE k = 'root'")
        .await
        .expect("SELECT from grandchild");
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("origin-value"),
        "grandchild clone must read the original source row"
    );
}

/// A write to the grandchild must NOT be visible in the parent clone or the
/// origin.
#[tokio::test]
async fn write_in_grandchild_does_not_affect_parent_or_origin() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Origin.
    client
        .simple_query("CREATE DATABASE rec2_origin")
        .await
        .expect("CREATE DATABASE rec2_origin");
    client
        .simple_query("USE DATABASE rec2_origin")
        .await
        .expect("USE rec2_origin");
    client
        .simple_query("CREATE COLLECTION data (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION data");
    client
        .simple_query("INSERT INTO data (k, v) VALUES ('shared', 'unchanged')")
        .await
        .expect("INSERT shared");

    // Clone chain.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE rec2_clone1 FROM rec2_origin")
        .await
        .expect("CLONE rec2_clone1");
    client
        .simple_query("CLONE DATABASE rec2_clone2 FROM rec2_clone1")
        .await
        .expect("CLONE rec2_clone2");

    // Overwrite in grandchild.
    client
        .simple_query("USE DATABASE rec2_clone2")
        .await
        .expect("USE rec2_clone2");
    client
        .simple_query("UPDATE data SET v = 'modified' WHERE k = 'shared'")
        .await
        .expect("UPDATE in grandchild");

    // Parent clone still sees original value.
    client
        .simple_query("USE DATABASE rec2_clone1")
        .await
        .expect("USE rec2_clone1");
    let parent_msgs = client
        .simple_query("SELECT v FROM data WHERE k = 'shared'")
        .await
        .expect("SELECT from parent clone");
    assert_eq!(
        first_value(&parent_msgs).as_deref(),
        Some("unchanged"),
        "parent clone must not see grandchild write"
    );

    // Origin still sees original value.
    client
        .simple_query("USE DATABASE rec2_origin")
        .await
        .expect("USE rec2_origin");
    let origin_msgs = client
        .simple_query("SELECT v FROM data WHERE k = 'shared'")
        .await
        .expect("SELECT from origin");
    assert_eq!(
        first_value(&origin_msgs).as_deref(),
        Some("unchanged"),
        "origin must not see grandchild write"
    );
}
