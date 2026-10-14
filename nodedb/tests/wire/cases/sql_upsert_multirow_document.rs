// SPDX-License-Identifier: BUSL-1.1

//! A multi-row `UPSERT` / `INSERT ... ON CONFLICT DO UPDATE` into a document
//! collection is one write.
//!
//! The statement lowers to one upsert page, so its rows commit together. A
//! key the statement names twice merges into the body its earlier row
//! produced, as two statements in a row would. A row the collection refuses
//! fails the statement, and no row of it lands.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn strict_collection(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION d (id TEXT PRIMARY KEY, n INT, email TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
}

/// The row count `sql` reports in its command tag.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql} reported no command tag"))
}

/// `id` and `n` of every row joined by a tab, ordered by `id`.
async fn rows(server: &TestServer) -> Vec<String> {
    server
        .query_text_joined("SELECT id, n FROM d ORDER BY id")
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_upsert_writes_new_and_stored_keys() {
    let server = TestServer::start().await;
    strict_collection(&server).await;
    server
        .exec("INSERT INTO d (id, n, email) VALUES ('b', 1, 'b@x')")
        .await
        .unwrap();

    let n = affected(
        &server,
        "UPSERT INTO d (id, n, email) VALUES ('a', 2, 'a@x'), ('b', 3, 'b@x')",
    )
    .await;
    assert_eq!(n, 2);
    assert_eq!(
        rows(&server).await,
        vec!["a\t2".to_string(), "b\t3".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_on_conflict_do_update_merges_stored_and_repeated_keys() {
    let server = TestServer::start().await;
    strict_collection(&server).await;
    server
        .exec("INSERT INTO d (id, n, email) VALUES ('b', 1, 'b@x')")
        .await
        .unwrap();

    let n = affected(
        &server,
        "INSERT INTO d (id, n, email) VALUES ('a', 2, 'a@x'), ('b', 3, 'b@x'), ('a', 4, 'a@x') \
         ON CONFLICT (id) DO UPDATE SET n = n + EXCLUDED.n",
    )
    .await;
    assert_eq!(n, 3);
    assert_eq!(
        rows(&server).await,
        vec!["a\t6".to_string(), "b\t4".to_string()]
    );
}

/// The second row takes an email a stored row holds, so the page fails its
/// UNIQUE check and the first row does not land either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_unique_violation_in_a_multi_row_upsert_writes_no_row() {
    let server = TestServer::start().await;
    strict_collection(&server).await;
    server
        .exec("CREATE UNIQUE INDEX d_email ON d (email)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO d (id, n, email) VALUES ('z', 9, 'taken@x')")
        .await
        .unwrap();

    let result = server
        .client
        .simple_query("UPSERT INTO d (id, n, email) VALUES ('a', 1, 'a@x'), ('b', 2, 'taken@x')")
        .await;
    let error = result.expect_err("a UNIQUE violation must fail the statement");
    assert_eq!(
        error.as_db_error().map(|e| e.code().code()),
        Some("23505"),
        "{error}"
    );
    assert_eq!(rows(&server).await, vec!["z\t9".to_string()]);
}

/// Each row of a schemaless page merges its fields over the stored row, as
/// a single-row `UPSERT` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_schemaless_upsert_merges_fields() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION docs").await.unwrap();
    server
        .exec("INSERT INTO docs (id, n, tag) VALUES ('b', 1, 'keep')")
        .await
        .unwrap();

    let n = affected(
        &server,
        "UPSERT INTO docs (id, n) VALUES ('a', 2), ('b', 3)",
    )
    .await;
    assert_eq!(n, 2);
    let stored = server
        .query_text_joined("SELECT id, n, tag FROM docs WHERE id = 'b'")
        .await
        .unwrap();
    assert_eq!(stored, vec!["b\t3\tkeep".to_string()]);
}
