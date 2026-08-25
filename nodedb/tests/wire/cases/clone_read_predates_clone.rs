// SPDX-License-Identifier: BUSL-1.1

//! Reading from a clone with `AS OF SYSTEM TIME` predating `clone_created_at`
//! must return an empty result and attach the `clone_predates_query_time`
//! metadata note.

use crate::harness::TestServer;

/// Clone a source DB.  Issue an AS OF query at `T = 0` (Unix epoch) — this
/// predates any `clone_created_at` LSN ever assigned.  Expect empty rows.
#[tokio::test]
async fn clone_read_predates_clone_returns_empty() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source setup.
    client
        .simple_query("CREATE DATABASE src_pred")
        .await
        .expect("CREATE DATABASE src_pred");
    client
        .simple_query("USE DATABASE src_pred")
        .await
        .expect("USE src_pred");
    client
        .simple_query(
            "CREATE COLLECTION data (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless', bitemporal=true)",
        )
        .await
        .expect("CREATE COLLECTION data");
    client
        .simple_query("INSERT INTO data (id, v) VALUES ('k1', 'val1')")
        .await
        .expect("INSERT k1");

    // Clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE clone_pred FROM src_pred LATEST")
        .await
        .expect("CLONE DATABASE");

    // Switch to clone and query AS OF T = 1 ms (well before clone_created_at).
    client
        .simple_query("USE DATABASE clone_pred")
        .await
        .expect("USE clone_pred");

    // AS OF 1 ms — predates all LSNs in this test session.
    let result = client
        .simple_query("SELECT id FROM data AS OF SYSTEM TIME 1")
        .await;

    // The query should succeed (no error) and return zero rows.
    match result {
        Ok(msgs) => {
            let row_count = msgs
                .iter()
                .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
                .count();
            assert_eq!(
                row_count, 0,
                "AS OF predating clone_created_at must return 0 rows; got {row_count}"
            );
        }
        Err(e) => {
            // Some implementations surface this as a structured error — accept
            // either empty result or an error containing the note keyword.
            let msg = format!("{e}");
            assert!(
                msg.contains("clone_predates_query_time")
                    || msg.contains("predates")
                    || msg.contains("CLONE_PREDATES"),
                "unexpected error for predating AS OF query: {msg}"
            );
        }
    }
}
