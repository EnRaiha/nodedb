// SPDX-License-Identifier: BUSL-1.1

//! `ROLLBACK TO SAVEPOINT` reverts the transaction's Data-Plane staging
//! overlay (value + TTL) to the savepoint, not just the Control-Plane buffered
//! plan list. Before this, staged writes made AFTER a savepoint stayed visible
//! to subsequent in-transaction reads (read-your-own-writes corruption). Each
//! test asserts the VISIBLE in-transaction state after `ROLLBACK TO`, then the
//! durable state after COMMIT.
//!
//! Scope: value + TTL overlay on Document (schemaless/strict) and KV. Graph
//! overlay and the native protocol are separate units and not exercised here.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// The `n` column of the first row returned by `sql`, run on the transaction's
/// own connection (so it observes the staging overlay). `None` if no row.
async fn read_n(server: &TestServer, sql: &str) -> Option<String> {
    let msgs = server
        .client
        .simple_query(sql)
        .await
        .expect("in-tx read should succeed");
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get("n").map(str::to_string),
        _ => None,
    })
}

async fn setup_doc(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION t \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, n) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, n) VALUES ('b', 2)")
        .await
        .unwrap();
}

/// THE bug this fixes: an insert staged AFTER a savepoint must vanish from
/// in-transaction reads once rolled back, while an insert staged BEFORE it
/// survives; a fresh insert after the rollback still commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_reverts_post_savepoint_insert_keeps_pre_savepoint() {
    let server = TestServer::start().await;
    setup_doc(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO t (id, n) VALUES ('p', 10)")
        .await
        .unwrap();
    server.exec("SAVEPOINT s1").await.unwrap();
    server
        .exec("INSERT INTO t (id, n) VALUES ('q', 20)")
        .await
        .unwrap();

    // Before rollback, both p and q are visible in-transaction.
    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'p'").await,
        Some("10".to_string())
    );
    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'q'").await,
        Some("20".to_string())
    );

    server.exec("ROLLBACK TO SAVEPOINT s1").await.unwrap();

    // After rollback: p (pre-savepoint) survives, q (post-savepoint) is gone.
    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'p'").await,
        Some("10".to_string()),
        "pre-savepoint insert must survive ROLLBACK TO"
    );
    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'q'").await,
        None,
        "post-savepoint insert must be invisible after ROLLBACK TO (the bug)"
    );

    // Transaction stays open: a fresh write after the rollback commits.
    server
        .exec("INSERT INTO t (id, n) VALUES ('r', 30)")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        server
            .query_text("SELECT n FROM t WHERE id = 'p'")
            .await
            .unwrap(),
        vec!["10"]
    );
    assert_eq!(
        server
            .query_text("SELECT n FROM t WHERE id = 'r'")
            .await
            .unwrap(),
        vec!["30"]
    );
    assert!(
        server
            .query_text("SELECT n FROM t WHERE id = 'q'")
            .await
            .unwrap()
            .is_empty(),
        "rolled-back insert must not persist"
    );
}

/// A ROLLBACK TO an outer savepoint discards every inner savepoint's writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_rollback_to_outer_discards_inner_savepoint() {
    let server = TestServer::start().await;
    setup_doc(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO t (id, n) VALUES ('p', 10)")
        .await
        .unwrap();
    server.exec("SAVEPOINT s1").await.unwrap();
    server
        .exec("INSERT INTO t (id, n) VALUES ('q', 20)")
        .await
        .unwrap();
    server.exec("SAVEPOINT s2").await.unwrap();
    server
        .exec("INSERT INTO t (id, n) VALUES ('r', 30)")
        .await
        .unwrap();

    // Roll all the way back to s1: both q (at s1..s2) and r (after s2) vanish.
    server.exec("ROLLBACK TO SAVEPOINT s1").await.unwrap();

    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'p'").await,
        Some("10".to_string())
    );
    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'q'").await,
        None,
        "inner savepoint write must be discarded"
    );
    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'r'").await,
        None,
        "innermost write must be discarded"
    );

    server.exec("COMMIT").await.unwrap();
    assert_eq!(
        server
            .query_text("SELECT n FROM t WHERE id = 'p'")
            .await
            .unwrap(),
        vec!["10"]
    );
    assert!(
        server
            .query_text("SELECT n FROM t WHERE id = 'q'")
            .await
            .unwrap()
            .is_empty()
    );
}

/// Overwriting a row after a savepoint then rolling back restores the PRIOR
/// value (proves prior-value undo, not a naive drop of post-savepoint entries).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_reverts_update_to_prior_value() {
    let server = TestServer::start().await;
    setup_doc(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE t SET n = 10 WHERE id = 'a'")
        .await
        .unwrap();
    server.exec("SAVEPOINT s").await.unwrap();
    server
        .exec("UPDATE t SET n = 20 WHERE id = 'a'")
        .await
        .unwrap();

    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'a'").await,
        Some("20".to_string())
    );

    server.exec("ROLLBACK TO SAVEPOINT s").await.unwrap();

    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'a'").await,
        Some("10".to_string()),
        "ROLLBACK TO must restore the value staged before the savepoint, not the base"
    );

    server.exec("COMMIT").await.unwrap();
    assert_eq!(
        server
            .query_text("SELECT n FROM t WHERE id = 'a'")
            .await
            .unwrap(),
        vec!["10"]
    );
}

/// A delete staged after a savepoint is undone by ROLLBACK TO — the row
/// reappears in-transaction and commits with its original value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_reverts_delete() {
    let server = TestServer::start().await;
    setup_doc(&server).await;

    server.exec("BEGIN").await.unwrap();
    server.exec("SAVEPOINT s").await.unwrap();
    server.exec("DELETE FROM t WHERE id = 'b'").await.unwrap();

    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'b'").await,
        None,
        "delete must be visible in-transaction before rollback"
    );

    server.exec("ROLLBACK TO SAVEPOINT s").await.unwrap();

    assert_eq!(
        read_n(&server, "SELECT n FROM t WHERE id = 'b'").await,
        Some("2".to_string()),
        "ROLLBACK TO must restore the deleted row"
    );

    server.exec("COMMIT").await.unwrap();
    assert_eq!(
        server
            .query_text("SELECT n FROM t WHERE id = 'b'")
            .await
            .unwrap(),
        vec!["2"]
    );
}

/// RELEASE destroys the savepoint; a subsequent ROLLBACK TO that name is
/// SQLSTATE 3B001 (invalid_savepoint_specification).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_then_rollback_to_errors_3b001() {
    let server = TestServer::start().await;
    setup_doc(&server).await;

    server.exec("BEGIN").await.unwrap();
    server.exec("SAVEPOINT s1").await.unwrap();
    server.exec("RELEASE SAVEPOINT s1").await.unwrap();

    let err = server
        .client
        .simple_query("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect_err("ROLLBACK TO a released savepoint must error");
    let db_err = err.as_db_error().expect("expected a DbError");
    assert_eq!(
        db_err.code().code(),
        "3B001",
        "expected 3B001 for an unknown savepoint, got {}",
        db_err.code().code()
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// RELEASE of an unknown savepoint name is 3B001 (previously a silent no-op).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_unknown_savepoint_errors_3b001() {
    let server = TestServer::start().await;
    setup_doc(&server).await;

    server.exec("BEGIN").await.unwrap();
    let err = server
        .client
        .simple_query("RELEASE SAVEPOINT nope")
        .await
        .expect_err("RELEASE of an unknown savepoint must error");
    let db_err = err.as_db_error().expect("expected a DbError");
    assert_eq!(db_err.code().code(), "3B001");
    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// SAVEPOINT / ROLLBACK TO / RELEASE outside a transaction block is SQLSTATE
/// 25P01 (no_active_sql_transaction).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn savepoint_outside_transaction_errors_25p01() {
    let server = TestServer::start().await;
    setup_doc(&server).await;

    for sql in [
        "SAVEPOINT s1",
        "ROLLBACK TO SAVEPOINT s1",
        "RELEASE SAVEPOINT s1",
    ] {
        let err = match server.client.simple_query(sql).await {
            Ok(_) => panic!("`{sql}` outside a transaction should have errored"),
            Err(e) => e,
        };
        let db_err = err.as_db_error().expect("expected a DbError");
        assert_eq!(
            db_err.code().code(),
            "25P01",
            "`{sql}` outside a transaction must be 25P01, got {}",
            db_err.code().code()
        );
    }
}

/// KV value overwrite after a savepoint is reverted by ROLLBACK TO.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_value_change_reverted_by_rollback_to() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION c (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO c (key, n) VALUES ('a', 1)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server.exec("SAVEPOINT s").await.unwrap();
    server
        .exec("UPDATE c SET n = 99 WHERE key = 'a'")
        .await
        .unwrap();

    assert_eq!(
        read_n(&server, "SELECT n FROM c WHERE key = 'a'").await,
        Some("99".to_string())
    );

    server.exec("ROLLBACK TO SAVEPOINT s").await.unwrap();

    assert_eq!(
        read_n(&server, "SELECT n FROM c WHERE key = 'a'").await,
        Some("1".to_string()),
        "ROLLBACK TO must revert the KV value overlay to its pre-savepoint state"
    );

    server.exec("COMMIT").await.unwrap();
    assert_eq!(
        server
            .query_text("SELECT n FROM c WHERE key = 'a'")
            .await
            .unwrap(),
        vec!["1"]
    );
}
