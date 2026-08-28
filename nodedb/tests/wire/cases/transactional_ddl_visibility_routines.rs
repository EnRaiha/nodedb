// SPDX-License-Identifier: BUSL-1.1

//! Intra-transaction visibility for catalog kinds beyond collections:
//! index records and functions.
//!
//! Every non-collection `CREATE`/`DROP` shares the collections' buffered-DDL
//! mechanism, but until the overlay covered them too, only collections
//! resolved their own uncommitted DDL. `DROP INDEX` and `DROP FUNCTION` both
//! validate the target's existence through the same single choke point a
//! `CREATE` inside the same transaction only buffers
//! (`get_index_record` / `get_function_in_database`), so a
//! create-then-drop in one transaction is a direct proof the overlay covers
//! them: it fails with "not found" without the fix and succeeds with it.

use crate::harness::TestServer;

/// Force the connection task to be repolled so the runtime can migrate it to
/// another worker between the statements of one transaction.
async fn hop_workers() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// An index created inside a transaction must resolve for a `DROP INDEX` in
/// that same transaction, before COMMIT ever writes the row to redb.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_then_drop_index_in_one_transaction() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION txn_vis_idx_coll (id TEXT PRIMARY KEY, region TEXT) \
             WITH (engine='document_schemaless')",
        )
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE INDEX txn_vis_idx ON txn_vis_idx_coll(region)")
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec("DROP INDEX txn_vis_idx")
        .await
        .expect("DROP INDEX must resolve the index this transaction just created");
    server.exec("COMMIT").await.unwrap();

    let listed = server.query_text("SHOW INDEXES").await.unwrap();
    assert!(
        !listed.iter().any(|n| n == "txn_vis_idx"),
        "an index created and dropped in one transaction must not be listed, got: {listed:?}"
    );
}

/// A function created inside a transaction must resolve for a `DROP
/// FUNCTION` in that same transaction, before COMMIT ever writes the row to
/// redb.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_then_drop_function_in_one_transaction() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE FUNCTION txn_vis_fn(x INT) RETURNS INT AS SELECT x + 1")
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec("DROP FUNCTION txn_vis_fn")
        .await
        .expect("DROP FUNCTION must resolve the function this transaction just created");
    server.exec("COMMIT").await.unwrap();

    server
        .expect_error("DROP FUNCTION txn_vis_fn", "does not exist")
        .await;
}

/// A function created inside a transaction, then dropped in that same
/// transaction, must not be resurrected by COMMIT — the buffered
/// create-then-drop must net to nothing, mirroring the collection case in
/// `transactional_ddl_visibility::create_then_drop_in_one_transaction_commits_to_nothing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_then_drop_function_commits_to_nothing() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE FUNCTION txn_vis_fn_cd(x INT) RETURNS INT AS SELECT x + 1")
        .await
        .unwrap();
    server.exec("DROP FUNCTION txn_vis_fn_cd").await.unwrap();
    server.exec("COMMIT").await.unwrap();

    server
        .exec("CREATE FUNCTION txn_vis_fn_cd(x INT) RETURNS INT AS SELECT x + 2")
        .await
        .expect("the name must be free to reuse after the transaction committed");
}
