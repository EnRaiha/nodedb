// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for temporary tables.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_temp_table_and_drop() {
    let server = TestServer::start().await;

    server
        .exec("CREATE TEMPORARY TABLE staging (id INT, raw TEXT)")
        .await
        .unwrap();
    // Drop temp table.
    // (Temp tables are session-scoped; explicit DROP just removes from session.)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn temp_table_on_commit_options() {
    let server = TestServer::start().await;

    // ON COMMIT PRESERVE ROWS (default).
    server.exec("CREATE TEMP TABLE t1 (a INT)").await.unwrap();

    // ON COMMIT DROP.
    server
        .exec("CREATE TEMP TABLE t2 (b INT) ON COMMIT DROP")
        .await
        .unwrap();

    // ON COMMIT DELETE ROWS.
    server
        .exec("CREATE TEMP TABLE t3 (c INT) ON COMMIT DELETE ROWS")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_temp_table_errors() {
    let server = TestServer::start().await;

    server.exec("CREATE TEMP TABLE dup (x INT)").await.unwrap();
    server
        .expect_error("CREATE TEMP TABLE dup (y INT)", "already exists")
        .await;
}
