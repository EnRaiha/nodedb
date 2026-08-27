// SPDX-License-Identifier: BUSL-1.1

//! Intra-transaction visibility of buffered DDL over pgwire.
//!
//! DDL inside an explicit transaction is buffered until COMMIT, but the
//! transaction that issued it must still resolve names against its own
//! uncommitted CREATE / ALTER / DROP — PostgreSQL does, and so must this.
//! Other sessions keep seeing committed state only, and ROLLBACK still leaves
//! nothing behind (covered by `transactional_ddl_atomicity`).

use crate::harness::TestServer;

/// Force the connection task to be repolled so the runtime can migrate it to
/// another worker between the statements of one transaction.
async fn hop_workers() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// A collection created inside a transaction must accept writes and reads from
/// the statements that follow it in that same transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_then_write_and_read_in_one_transaction() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION txn_vis_rw (id TEXT PRIMARY KEY, n INT) \
             WITH (engine='document_schemaless')",
        )
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec("INSERT INTO txn_vis_rw (id, n) VALUES ('a', 1)")
        .await
        .expect("INSERT must resolve the collection this transaction just created");
    hop_workers().await;
    let staged = server
        .query_rows("SELECT n FROM txn_vis_rw WHERE id = 'a'")
        .await
        .expect("in-transaction SELECT must resolve the buffered collection");
    assert_eq!(
        staged
            .first()
            .and_then(|row| row.first())
            .map(String::as_str),
        Some("1"),
        "the transaction must read its own staged write, got: {staged:?}"
    );
    server.exec("COMMIT").await.unwrap();

    let committed = server
        .query_rows("SELECT n FROM txn_vis_rw WHERE id = 'a'")
        .await
        .expect("post-commit SELECT must succeed");
    assert_eq!(
        committed
            .first()
            .and_then(|row| row.first())
            .map(String::as_str),
        Some("1"),
        "COMMIT must persist both the collection and its row, got: {committed:?}"
    );
}

/// A collection created and dropped inside one transaction resolves for the
/// DROP, and COMMIT leaves nothing usable behind. `DROP` is soft here, so the
/// check is on the substantive end state — unlisted, unreadable, name free —
/// not on which of the two 42P01 messages the read happens to return.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_then_drop_in_one_transaction_commits_to_nothing() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION txn_vis_cd (id TEXT PRIMARY KEY) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec("DROP COLLECTION txn_vis_cd")
        .await
        .expect("DROP must resolve the collection this transaction just created");
    server.exec("COMMIT").await.unwrap();

    let listed = server.query_rows("SHOW COLLECTIONS").await.unwrap();
    assert!(
        !listed.iter().any(|r| r[0] == "txn_vis_cd"),
        "a collection created and dropped in one transaction must not be listed, got: {listed:?}"
    );
    assert!(
        server
            .query_rows("SELECT id FROM txn_vis_cd")
            .await
            .is_err(),
        "the dropped collection must not be readable after COMMIT"
    );
    server
        .exec(
            "CREATE COLLECTION txn_vis_cd (id TEXT PRIMARY KEY) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("the name must be free to reuse after the transaction committed");
}

/// An ALTER inside a transaction must be visible to the reads and writes that
/// follow it in that transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_is_visible_to_a_later_statement_in_the_same_transaction() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION txn_vis_alter (id TEXT PRIMARY KEY, n INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("ALTER COLLECTION txn_vis_alter ADD COLUMN note TEXT DEFAULT 'n/a'")
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec("INSERT INTO txn_vis_alter (id, n, note) VALUES ('a', 1, 'hello')")
        .await
        .expect("the column this transaction just added must be writable in it");
    let staged = server
        .query_rows("SELECT note FROM txn_vis_alter WHERE id = 'a'")
        .await
        .expect("in-transaction SELECT must see the altered shape");
    assert_eq!(
        staged
            .first()
            .and_then(|row| row.first())
            .map(String::as_str),
        Some("hello"),
        "the altered shape must be readable inside its own transaction, got: {staged:?}"
    );
    server.exec("COMMIT").await.unwrap();
}

/// A DROP inside a transaction hides the collection from the statements that
/// follow it, and ROLLBACK brings it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_buffered_drop_hides_the_collection_then_rollback_restores_it() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION txn_vis_drop (id TEXT PRIMARY KEY) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO txn_vis_drop (id) VALUES ('a')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server.exec("DROP COLLECTION txn_vis_drop").await.unwrap();
    hop_workers().await;
    server
        .expect_error("SELECT id FROM txn_vis_drop", "txn_vis_drop")
        .await;
    server.exec("ROLLBACK").await.unwrap();

    let rows = server
        .query_rows("SELECT id FROM txn_vis_drop WHERE id = 'a'")
        .await
        .expect("ROLLBACK must leave the collection exactly as it was");
    assert_eq!(
        rows.first().and_then(|row| row.first()).map(String::as_str),
        Some("a"),
        "a rolled-back DROP must not remove anything, got: {rows:?}"
    );
}

/// Buffered DDL is visible to its own transaction only. A concurrent session
/// must still see committed state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffered_ddl_stays_invisible_to_another_session() {
    let server = TestServer::start().await;
    let (other, other_task) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("second pgwire session");

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION txn_vis_isolated (id TEXT PRIMARY KEY) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    hop_workers().await;

    let seen = other
        .simple_query("SELECT id FROM txn_vis_isolated")
        .await
        .err()
        .map(|error| error.to_string());
    assert!(
        seen.is_some(),
        "another session must not resolve a collection this transaction has only buffered"
    );

    server.exec("ROLLBACK").await.unwrap();
    other_task.abort();
}
