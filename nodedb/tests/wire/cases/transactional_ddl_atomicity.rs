// SPDX-License-Identifier: BUSL-1.1

//! Transactional DDL atomicity over pgwire.
//!
//! DDL inside an explicit transaction must be buffered until COMMIT and
//! discarded on ROLLBACK. The buffer is connection-scoped, so it has to
//! survive a tokio worker-thread hop between the statements of one
//! transaction — a thread-scoped buffer leaked the collection instead.

use crate::harness::TestServer;

/// Names of every collection visible to the harness connection.
async fn collection_names(server: &TestServer) -> Vec<String> {
    server
        .query_rows("SHOW COLLECTIONS")
        .await
        .expect("SHOW COLLECTIONS must succeed")
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect()
}

async fn exists(server: &TestServer, name: &str) -> bool {
    collection_names(server).await.iter().any(|n| n == name)
}

/// Force the connection task to be repolled, giving the runtime the chance to
/// migrate it to another worker between the statements of one transaction.
async fn hop_workers() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_discards_created_collection() {
    let server = TestServer::start().await;

    // Repeat: the leak was a thread-affinity race, so one attempt can pass by
    // luck. Every iteration must roll back cleanly.
    for i in 0..10 {
        let name = format!("txn_ddl_rollback_{i}");
        server.exec("BEGIN").await.unwrap();
        hop_workers().await;
        server
            .exec(&format!(
                "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, val INT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap();
        hop_workers().await;
        server.exec("ROLLBACK").await.unwrap();

        assert!(
            !exists(&server, &name).await,
            "{name} must not survive ROLLBACK"
        );
        server
            .expect_error(&format!("SELECT id FROM {name}"), "does not exist")
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_persists_created_collection() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    hop_workers().await;
    server
        .exec(
            "CREATE COLLECTION txn_ddl_commit (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    hop_workers().await;
    server.exec("COMMIT").await.unwrap();

    assert!(
        exists(&server, "txn_ddl_commit").await,
        "txn_ddl_commit must survive COMMIT"
    );
    server
        .exec("INSERT INTO txn_ddl_commit (id, val) VALUES ('a', 1)")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT id FROM txn_ddl_commit WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "committed collection must be writable");
}

/// Several DDL statements in one transaction commit or abort as one batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_discards_every_statement_of_a_multi_ddl_transaction() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    for i in 0..3 {
        server
            .exec(&format!(
                "CREATE COLLECTION txn_ddl_multi_{i} (id TEXT PRIMARY KEY) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap();
        hop_workers().await;
    }
    server.exec("ROLLBACK").await.unwrap();

    let names = collection_names(&server).await;
    for i in 0..3 {
        let name = format!("txn_ddl_multi_{i}");
        assert!(
            !names.contains(&name),
            "{name} must not survive ROLLBACK, saw: {names:?}"
        );
    }
}

/// A collection created and written to in the SAME transaction survives
/// COMMIT with both the catalog row and the data row intact — the atomic
/// cutover this exercises: the DDL finalizes to the catalog before the
/// buffered INSERT dispatches, so a crash between them still leaves a real,
/// cataloged, empty collection rather than an undispatched or orphaned row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_persists_collection_created_and_written_in_the_same_transaction() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    hop_workers().await;
    server
        .exec(
            "CREATE COLLECTION txn_ddl_create_then_insert (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec("INSERT INTO txn_ddl_create_then_insert (id, val) VALUES ('a', 1)")
        .await
        .unwrap();
    hop_workers().await;
    server.exec("COMMIT").await.unwrap();

    assert!(
        exists(&server, "txn_ddl_create_then_insert").await,
        "the collection created inside the transaction must survive COMMIT"
    );
    let rows = server
        .query_text("SELECT id, val FROM txn_ddl_create_then_insert WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the row inserted in the same transaction must survive COMMIT alongside the collection"
    );
}

/// DDL and DML in one transaction roll back together: neither the new
/// collection nor the row written into a pre-existing one survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_discards_mixed_ddl_and_dml() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION txn_ddl_base (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO txn_ddl_base (id, val) VALUES ('mixed', 7)")
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec(
            "CREATE COLLECTION txn_ddl_mixed (id TEXT PRIMARY KEY) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    hop_workers().await;
    server.exec("ROLLBACK").await.unwrap();

    assert!(
        !exists(&server, "txn_ddl_mixed").await,
        "txn_ddl_mixed must not survive ROLLBACK"
    );
    let rows = server
        .query_text("SELECT id FROM txn_ddl_base WHERE id = 'mixed'")
        .await
        .unwrap();
    assert!(rows.is_empty(), "rolled-back row must not be visible");
}

/// Regression pin for the descriptor-lease self-drain fix: a transaction
/// that ALTERs a collection and, in the SAME transaction, writes to that
/// SAME collection must not wait on its own statement-time lease hold.
///
/// `finalize_pending` (which runs the ALTER's descriptor-version drain)
/// executes before the buffered INSERT dispatches, so the drain would see
/// this session's own INSERT-time lease as an unreleased hold on the exact
/// descriptor it is draining — without excluding the requester's own hold,
/// this wedges for `DEFAULT_DRAIN_TIMEOUT` (35s) and then aborts the COMMIT
/// with a drain-timeout error. The bounded `timeout` below turns that wedge
/// into a fast, unambiguous test failure instead of a 35s hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_survives_altering_and_writing_the_same_collection_in_one_transaction() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION txn_ddl_self_drain (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("ALTER COLLECTION txn_ddl_self_drain ADD COLUMN note TEXT DEFAULT 'n/a'")
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec("INSERT INTO txn_ddl_self_drain (id, val, note) VALUES ('a', 1, 'hi')")
        .await
        .unwrap();
    hop_workers().await;

    tokio::time::timeout(std::time::Duration::from_secs(10), server.exec("COMMIT"))
        .await
        .expect(
            "COMMIT must not wedge waiting on this transaction's own buffered-write \
             lease hold on the descriptor it just ALTERed",
        )
        .unwrap();

    let rows = server
        .query_text("SELECT id, note FROM txn_ddl_self_drain WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the row written in the altering transaction must survive COMMIT"
    );
}
