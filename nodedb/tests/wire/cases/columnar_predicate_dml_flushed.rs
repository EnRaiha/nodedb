// SPDX-License-Identifier: BUSL-1.1

//! Predicate `UPDATE` and `DELETE` on a columnar collection match rows in
//! flushed segments and rows in the memtable alike.
//!
//! The server flushes the memtable once it holds two rows. The seed writes
//! four rows in one statement, which flush to a segment, then one row that
//! stays in the memtable. Each predicate matches rows in both places.
//!
//! A computed `UPDATE` (`SET x = x + 10`) evaluates against each matched
//! row's pre-image. A result past a declared width refuses the statement,
//! and no row changes.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// The flush threshold every test here starts the server with.
const FLUSH_THRESHOLD: usize = 2;

/// Rows `1..=4` are flushed. Row `5` is in the memtable.
async fn seed(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id INT PRIMARY KEY, x INT) WITH (engine='columnar')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {name} (id, x) VALUES (1, 1), (2, 6), (3, 7), (4, 2)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed flushed rows of {name}: {e}"));
    server
        .exec(&format!("INSERT INTO {name} (id, x) VALUES (5, 8)"))
        .await
        .unwrap_or_else(|e| panic!("seed memtable row of {name}: {e}"));
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

/// `id` and `x` of every row joined by a tab, ordered by `id`.
async fn rows(server: &TestServer, name: &str) -> Vec<String> {
    server
        .query_text_joined(&format!("SELECT id, x FROM {name} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("select {name}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_delete_removes_flushed_and_memtable_rows() {
    let server = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    seed(&server, "cpd_delete").await;

    let count = affected(&server, "DELETE FROM cpd_delete WHERE x > 5").await;

    assert_eq!(
        count, 3,
        "rows 2 and 3 are flushed, row 5 is in the memtable"
    );
    assert_eq!(rows(&server, "cpd_delete").await, vec!["1\t1", "4\t2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_update_changes_flushed_and_memtable_rows() {
    let server = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    seed(&server, "cpd_update").await;

    let count = affected(&server, "UPDATE cpd_update SET x = 0 WHERE x > 5").await;

    assert_eq!(
        count, 3,
        "rows 2 and 3 are flushed, row 5 is in the memtable"
    );
    assert_eq!(
        rows(&server, "cpd_update").await,
        vec!["1\t1", "2\t0", "3\t0", "4\t2", "5\t0"]
    );
    // The updated flushed rows read once each: the originals are tombstoned.
    assert_eq!(
        affected(&server, "DELETE FROM cpd_update WHERE x = 0").await,
        3
    );
    assert_eq!(rows(&server, "cpd_update").await, vec!["1\t1", "4\t2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_delete_in_a_transaction_removes_flushed_and_memtable_rows() {
    let server = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    seed(&server, "cpd_txn").await;

    server.exec("BEGIN").await.expect("begin");
    let count = affected(&server, "DELETE FROM cpd_txn WHERE x > 5").await;
    assert_eq!(
        count, 3,
        "rows 2 and 3 are flushed, row 5 is in the memtable"
    );
    server.exec("COMMIT").await.expect("commit");

    assert_eq!(rows(&server, "cpd_txn").await, vec!["1\t1", "4\t2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_computed_update_evaluates_each_flushed_and_memtable_row() {
    let server = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    seed(&server, "cpd_computed").await;

    let count = affected(&server, "UPDATE cpd_computed SET x = x + 10 WHERE x > 5").await;

    assert_eq!(
        count, 3,
        "rows 2 and 3 are flushed, row 5 is in the memtable"
    );
    assert_eq!(
        rows(&server, "cpd_computed").await,
        vec!["1\t1", "2\t16", "3\t17", "4\t2", "5\t18"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_computed_update_in_a_transaction_reads_its_own_writes_and_commits() {
    let server = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    seed(&server, "cpd_computed_txn").await;

    server.exec("BEGIN").await.expect("begin");
    let count = affected(&server, "UPDATE cpd_computed_txn SET x = x * 2 WHERE x > 5").await;
    assert_eq!(count, 3);
    assert_eq!(
        rows(&server, "cpd_computed_txn").await,
        vec!["1\t1", "2\t12", "3\t14", "4\t2", "5\t16"],
        "the transaction reads its own computed post-images"
    );
    server.exec("COMMIT").await.expect("commit");

    assert_eq!(
        rows(&server, "cpd_computed_txn").await,
        vec!["1\t1", "2\t12", "3\t14", "4\t2", "5\t16"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolled_back_computed_update_changes_no_row() {
    let server = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    seed(&server, "cpd_computed_rb").await;

    server.exec("BEGIN").await.expect("begin");
    affected(&server, "UPDATE cpd_computed_rb SET x = x - 1").await;
    server.exec("ROLLBACK").await.expect("rollback");

    assert_eq!(
        rows(&server, "cpd_computed_rb").await,
        vec!["1\t1", "2\t6", "3\t7", "4\t2", "5\t8"]
    );
}

/// `x * 4096` fits `SMALLINT` for every row but row 5, the last row the
/// update visits. The statement is refused whole: no flushed row changes.
async fn assert_overflowing_computed_update_is_refused(server: &TestServer, name: &str) {
    server
        .expect_error(&format!("UPDATE {name} SET x = x * 4096"), "SQLSTATE 22003")
        .await;
}

/// Rows `1..=4` are flushed. Row `5` is in the memtable. `x` is `SMALLINT`.
async fn seed_smallint(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id INT PRIMARY KEY, x SMALLINT) WITH (engine='columnar')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {name} (id, x) VALUES (1, 1), (2, 6), (3, 7), (4, 2)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed flushed rows of {name}: {e}"));
    server
        .exec(&format!("INSERT INTO {name} (id, x) VALUES (5, 8)"))
        .await
        .unwrap_or_else(|e| panic!("seed memtable row of {name}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_computed_update_past_a_declared_width_changes_no_row() {
    let server = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    seed_smallint(&server, "cpd_width").await;

    assert_overflowing_computed_update_is_refused(&server, "cpd_width").await;

    assert_eq!(
        rows(&server, "cpd_width").await,
        vec!["1\t1", "2\t6", "3\t7", "4\t2", "5\t8"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_computed_update_past_a_declared_width_in_a_transaction_changes_no_row() {
    let server = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    seed_smallint(&server, "cpd_width_txn").await;

    server.exec("BEGIN").await.expect("begin");
    assert_overflowing_computed_update_is_refused(&server, "cpd_width_txn").await;
    server.exec("ROLLBACK").await.expect("rollback");

    assert_eq!(
        rows(&server, "cpd_width_txn").await,
        vec!["1\t1", "2\t6", "3\t7", "4\t2", "5\t8"]
    );
}
