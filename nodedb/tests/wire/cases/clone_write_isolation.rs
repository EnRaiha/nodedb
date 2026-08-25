// SPDX-License-Identifier: BUSL-1.1

//! Writes to a clone must never affect the source database.
//!
//! - `UPDATE` in the clone → source row unchanged.
//! - `DELETE` in the clone → source row still present.

use crate::harness::TestServer;

/// Update a row in the clone; the corresponding row in the source must remain
/// at its original value.
#[tokio::test]
async fn update_in_clone_does_not_modify_source() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source: one row.
    client
        .simple_query("CREATE DATABASE src_iso_upd")
        .await
        .expect("CREATE DATABASE src_iso_upd");
    client
        .simple_query("USE DATABASE src_iso_upd")
        .await
        .expect("USE src_iso_upd");
    client
        .simple_query(
            "CREATE COLLECTION emp (key STRING PRIMARY KEY, name STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION emp");
    client
        .simple_query("INSERT INTO emp (key, name) VALUES ('e1', 'alice')")
        .await
        .expect("INSERT e1");

    // Clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE clone_iso_upd FROM src_iso_upd LATEST")
        .await
        .expect("CLONE DATABASE");

    // Update in clone.
    client
        .simple_query("USE DATABASE clone_iso_upd")
        .await
        .expect("USE clone_iso_upd");
    client
        .simple_query("UPDATE emp SET name = 'bob' WHERE key = 'e1'")
        .await
        .expect("UPDATE in clone");

    // Clone should see 'bob'.
    let msgs = client
        .simple_query("SELECT name FROM emp WHERE key = 'e1'")
        .await
        .expect("SELECT in clone");
    let clone_name = first_column(&msgs);
    assert_eq!(
        clone_name.as_deref(),
        Some("bob"),
        "clone must see updated value 'bob'; got {clone_name:?}"
    );

    // Source must still see 'alice'.
    client
        .simple_query("USE DATABASE src_iso_upd")
        .await
        .expect("USE src_iso_upd");
    let msgs = client
        .simple_query("SELECT name FROM emp WHERE key = 'e1'")
        .await
        .expect("SELECT in source");
    let src_name = first_column(&msgs);
    assert_eq!(
        src_name.as_deref(),
        Some("alice"),
        "source must remain 'alice' after clone UPDATE; got {src_name:?}"
    );
}

/// Delete a row from the clone; the row must still exist in the source.
#[tokio::test]
async fn delete_in_clone_does_not_modify_source() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source: one row.
    client
        .simple_query("CREATE DATABASE src_iso_del")
        .await
        .expect("CREATE DATABASE src_iso_del");
    client
        .simple_query("USE DATABASE src_iso_del")
        .await
        .expect("USE src_iso_del");
    client
        .simple_query(
            "CREATE COLLECTION docs (key STRING PRIMARY KEY, txt STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION docs");
    client
        .simple_query("INSERT INTO docs (key, txt) VALUES ('d1', 'important')")
        .await
        .expect("INSERT d1");

    // Clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE clone_iso_del FROM src_iso_del LATEST")
        .await
        .expect("CLONE DATABASE");

    // Delete in clone.
    client
        .simple_query("USE DATABASE clone_iso_del")
        .await
        .expect("USE clone_iso_del");
    client
        .simple_query("DELETE FROM docs WHERE key = 'd1'")
        .await
        .expect("DELETE in clone");

    // Clone should not see the row.
    let msgs = client
        .simple_query("SELECT key FROM docs WHERE key = 'd1'")
        .await
        .expect("SELECT in clone after delete");
    let clone_rows = row_count(&msgs);
    assert_eq!(
        clone_rows, 0,
        "clone must not see d1 after delete; got {clone_rows} rows"
    );

    // Source must still see the row.
    client
        .simple_query("USE DATABASE src_iso_del")
        .await
        .expect("USE src_iso_del");
    let msgs = client
        .simple_query("SELECT key FROM docs WHERE key = 'd1'")
        .await
        .expect("SELECT in source after clone delete");
    let src_rows = row_count(&msgs);
    assert_eq!(
        src_rows, 1,
        "source must still have d1 after clone DELETE; got {src_rows} rows"
    );
}

fn first_column(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_string());
        }
    }
    None
}

fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}
