// SPDX-License-Identifier: BUSL-1.1

//! KV engine point writes execute at STATEMENT time inside a transaction,
//! the same way Document point writes already do: real command tags
//! (`INSERT 0 1` / `UPDATE 1` / `DELETE 1`), statement-time constraint
//! errors, and read-your-own-writes on an in-transaction `SELECT`. COMMIT's
//! durable replay is unchanged -- the plan is still buffered and applied at
//! COMMIT; this suite only exercises the statement-time overlay behavior.
//!
//! KV is the first non-Document engine wired into the staging overlay.

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
        .exec("CREATE COLLECTION c (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO c (key, n) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO c (key, n) VALUES ('b', 2)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_insert_returns_real_tag_and_is_visible_in_tx() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    // INSERT of a new key returns INSERT 0 1 at the statement, not a bare OK.
    let msgs = server
        .client
        .simple_query("INSERT INTO c (key, n) VALUES ('c', 3)")
        .await
        .expect("in-tx KV insert should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "in-tx KV INSERT must report 1 row at statement time"
    );

    // Read-your-own-writes: visible to a SELECT inside the same transaction.
    let rows = server
        .client
        .simple_query("SELECT n FROM c WHERE key = 'c'")
        .await
        .unwrap();
    let n = rows
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get("n").map(str::to_string),
            _ => None,
        })
        .expect("staged insert must be visible in the same transaction");
    assert_eq!(n, "3");

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = server
        .query_text("SELECT n FROM c WHERE key = 'c'")
        .await
        .unwrap();
    assert_eq!(committed, vec!["3"], "committed KV insert must persist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_insert_rollback_discards_staged_write() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("INSERT INTO c (key, n) VALUES ('z', 9)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(1));
    server.client.simple_query("ROLLBACK").await.unwrap();

    let z = server
        .query_text("SELECT n FROM c WHERE key = 'z'")
        .await
        .unwrap();
    assert!(
        z.is_empty(),
        "rolled-back KV insert must not persist, got {z:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_duplicate_key_insert_raises_23505_at_the_statement() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    match server
        .client
        .simple_query("INSERT INTO c (key, n) VALUES ('a', 99)")
        .await
    {
        Ok(_) => panic!("duplicate-key KV insert must raise 23505 at the statement"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError at the statement");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}",
                db_err.code().code()
            );
        }
    }

    server.client.simple_query("ROLLBACK").await.unwrap();

    let a = server
        .query_text("SELECT n FROM c WHERE key = 'a'")
        .await
        .unwrap();
    assert_eq!(a, vec!["1"], "original row must be unchanged, got {a:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_insert_on_conflict_do_nothing_reports_zero_rows() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query("INSERT INTO c (key, n) VALUES ('a', 99) ON CONFLICT DO NOTHING")
        .await
        .expect("ON CONFLICT DO NOTHING must not error on a duplicate key");
    assert_eq!(
        command_count(&msgs),
        Some(0),
        "ON CONFLICT DO NOTHING must report 0 rows on a conflict"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let a = server
        .query_text("SELECT n FROM c WHERE key = 'a'")
        .await
        .unwrap();
    assert_eq!(a, vec!["1"], "no-op insert must not overwrite, got {a:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_insert_if_absent_inserts_when_absent() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query("INSERT INTO c (key, n) VALUES ('c', 3) ON CONFLICT DO NOTHING")
        .await
        .expect("ON CONFLICT DO NOTHING on an absent key should insert");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "ON CONFLICT DO NOTHING must insert (count 1) when the key is absent"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let c = server
        .query_text("SELECT n FROM c WHERE key = 'c'")
        .await
        .unwrap();
    assert_eq!(c, vec!["3"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_delete_returns_real_tag_and_hides_row_in_tx() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query("DELETE FROM c WHERE key = 'a'")
        .await
        .expect("in-tx KV delete should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "in-tx KV DELETE must report 1 row"
    );

    // Read-your-own-writes: the deleted key is no longer visible.
    let rows = server
        .client
        .simple_query("SELECT n FROM c WHERE key = 'a'")
        .await
        .unwrap();
    let has_row = rows.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_)));
    assert!(
        !has_row,
        "staged-deleted key must not be visible in the same transaction"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    // Rollback restores the row.
    let a = server
        .query_text("SELECT n FROM c WHERE key = 'a'")
        .await
        .unwrap();
    assert_eq!(
        a,
        vec!["1"],
        "rolled-back KV delete must restore the row, got {a:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_delete_then_commit_persists() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("DELETE FROM c WHERE key = 'a'")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(1));
    server.client.simple_query("COMMIT").await.unwrap();

    let a = server
        .query_text("SELECT n FROM c WHERE key = 'a'")
        .await
        .unwrap();
    assert!(
        a.is_empty(),
        "committed KV delete must remove the row, got {a:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_insert_on_conflict_do_update_on_existing_key_reports_update_tag() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query(
            "INSERT INTO c (key, n) VALUES ('a', 2) \
             ON CONFLICT (key) DO UPDATE SET n = EXCLUDED.n",
        )
        .await
        .expect("in-tx KV ON CONFLICT DO UPDATE should succeed at the statement");
    let tag_str = msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(_) => Some(()),
        _ => None,
    });
    assert!(tag_str.is_some());
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "ON CONFLICT DO UPDATE on an existing key must report 1 row"
    );

    // Visible in-tx.
    let rows = server
        .client
        .simple_query("SELECT n FROM c WHERE key = 'a'")
        .await
        .unwrap();
    let n = rows
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get("n").map(str::to_string),
            _ => None,
        })
        .expect("staged ON CONFLICT DO UPDATE result must be visible in-tx");
    assert_eq!(n, "2");

    server.client.simple_query("COMMIT").await.unwrap();

    let a = server
        .query_text("SELECT n FROM c WHERE key = 'a'")
        .await
        .unwrap();
    assert_eq!(a, vec!["2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_insert_on_conflict_do_update_on_absent_key_inserts() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query(
            "INSERT INTO c (key, n) VALUES ('c', 3) \
             ON CONFLICT (key) DO UPDATE SET n = EXCLUDED.n",
        )
        .await
        .expect("in-tx KV ON CONFLICT DO UPDATE on an absent key should insert");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "ON CONFLICT DO UPDATE on an absent key must insert (count 1)"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let c = server
        .query_text("SELECT n FROM c WHERE key = 'c'")
        .await
        .unwrap();
    assert_eq!(c, vec!["3"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_insert_then_delete_same_txn_is_not_visible() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let ins = server
        .client
        .simple_query("INSERT INTO c (key, n) VALUES ('c', 3)")
        .await
        .unwrap();
    assert_eq!(command_count(&ins), Some(1));

    let del = server
        .client
        .simple_query("DELETE FROM c WHERE key = 'c'")
        .await
        .unwrap();
    assert_eq!(
        command_count(&del),
        Some(1),
        "deleting a staged-only insert in the same transaction must report 1 row"
    );

    let rows = server
        .client
        .simple_query("SELECT n FROM c WHERE key = 'c'")
        .await
        .unwrap();
    let has_row = rows.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_)));
    assert!(
        !has_row,
        "a staged-then-deleted-in-same-txn key must not be visible"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let c = server
        .query_text("SELECT n FROM c WHERE key = 'c'")
        .await
        .unwrap();
    assert!(
        c.is_empty(),
        "a staged-then-deleted-in-same-txn key must never become durable, got {c:?}"
    );
}
