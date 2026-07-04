// SPDX-License-Identifier: BUSL-1.1

//! In-transaction point writes execute at STATEMENT time: they return their
//! real command tag (INSERT 0 1 / UPDATE 1 / DELETE 1) and raise constraint
//! violations at the offending statement, while COMMIT remains the sole
//! durable apply (a post-commit SELECT sees the writes; ROLLBACK sees none).

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in a simple-query
/// response (PostgreSQL's `INSERT 0 N` / `UPDATE N` / `DELETE N` count).
fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

async fn setup(server: &TestServer) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_point_writes_return_real_tags_and_persist_on_commit() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    // INSERT of a NEW row returns INSERT 0 1 at the statement (not OK/0).
    let msgs = server
        .client
        .simple_query("INSERT INTO t (id, n) VALUES ('c', 3)")
        .await
        .expect("in-tx insert should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "in-tx INSERT must report 1 row at statement time, not a bare OK"
    );

    // UPDATE by primary key returns UPDATE 1.
    let msgs = server
        .client
        .simple_query("UPDATE t SET n = 20 WHERE id = 'b'")
        .await
        .expect("in-tx update should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "in-tx UPDATE must report 1 row"
    );

    // DELETE by primary key returns DELETE 1.
    let msgs = server
        .client
        .simple_query("DELETE FROM t WHERE id = 'a'")
        .await
        .expect("in-tx delete should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "in-tx DELETE must report 1 row"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    // Durable path unchanged: the staged writes are persisted at COMMIT.
    let c = server
        .query_text("SELECT n FROM t WHERE id = 'c'")
        .await
        .unwrap();
    assert_eq!(c, vec!["3"], "committed insert must be visible");
    let b = server
        .query_text("SELECT n FROM t WHERE id = 'b'")
        .await
        .unwrap();
    assert_eq!(b, vec!["20"], "committed update must be visible");
    let a = server
        .query_text("SELECT n FROM t WHERE id = 'a'")
        .await
        .unwrap();
    assert!(
        a.is_empty(),
        "committed delete must remove the row, got {a:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_duplicate_pk_raises_23505_at_the_statement() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    // Duplicate primary key must be rejected AT THE STATEMENT (not deferred to
    // COMMIT) with SQLSTATE 23505.
    match server
        .client
        .simple_query("INSERT INTO t (id, n) VALUES ('a', 99)")
        .await
    {
        Ok(_) => panic!("duplicate-PK insert must raise 23505 at the statement"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError at the statement");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505 at the statement, got {}",
                db_err.code().code()
            );
        }
    }

    server.client.simple_query("ROLLBACK").await.unwrap();

    // The original row is untouched — the duplicate was never staged or applied.
    let a = server
        .query_text("SELECT n FROM t WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(a, vec!["1"], "original row must be unchanged, got {a:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_rollback_discards_staged_writes() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("INSERT INTO t (id, n) VALUES ('z', 9)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(1));
    server.client.simple_query("ROLLBACK").await.unwrap();

    let z = server
        .query_text("SELECT n FROM t WHERE id = 'z'")
        .await
        .unwrap();
    assert!(
        z.is_empty(),
        "rolled-back insert must not persist, got {z:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_insert_on_conflict_do_nothing_reports_zero_rows() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    // ON CONFLICT DO NOTHING on a duplicate PK: no error, 0 rows affected.
    let msgs = server
        .client
        .simple_query("INSERT INTO t (id, n) VALUES ('a', 99) ON CONFLICT DO NOTHING")
        .await
        .expect("ON CONFLICT DO NOTHING must not error on a duplicate");
    assert_eq!(
        command_count(&msgs),
        Some(0),
        "ON CONFLICT DO NOTHING must report 0 rows on a conflict"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let a = server
        .query_text("SELECT n FROM t WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(a, vec!["1"], "no-op insert must not overwrite, got {a:?}");
}
