// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for scheduled jobs.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_alter_drop_schedule() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SCHEDULE cleanup CRON '0 3 * * *' AS BEGIN RETURN 1; END")
        .await
        .unwrap();

    // SHOW SCHEDULES should list it.
    let rows = server.query_text("SHOW SCHEDULES").await.unwrap();
    assert!(!rows.is_empty());

    // ALTER SCHEDULE DISABLE.
    server.exec("ALTER SCHEDULE cleanup DISABLE").await.unwrap();

    // ALTER SCHEDULE SET CRON.
    server
        .exec("ALTER SCHEDULE cleanup SET CRON '0 0 * * *'")
        .await
        .unwrap();

    // ALTER SCHEDULE ENABLE.
    server.exec("ALTER SCHEDULE cleanup ENABLE").await.unwrap();

    // SHOW SCHEDULE HISTORY — should not error.
    let _rows = server
        .query_text("SHOW SCHEDULE HISTORY cleanup")
        .await
        .unwrap();

    // DROP SCHEDULE.
    server.exec("DROP SCHEDULE cleanup").await.unwrap();
    server
        .expect_error("DROP SCHEDULE cleanup", "does not exist")
        .await;
}
