// SPDX-License-Identifier: BUSL-1.1

//! A role-guarded state transition is judged against the writer's roles on
//! every write path.
//!
//! Each case declares `status TRANSITIONS ('draft' -> 'active' BY ROLE
//! 'approver')` on a row in `draft`. A user without `approver` drives
//! `draft -> active` and is refused, leaving the row unchanged. A user with
//! `approver` drives the same statement and is admitted.

use crate::harness::TestServer;

const CLERK: &str = "rt_clerk";
const APPROVER: &str = "rt_approver";

/// Create the role and the two users. Both users can write. Only
/// `APPROVER` holds the `approver` role.
async fn users(server: &TestServer) {
    server.exec("CREATE ROLE approver").await.unwrap();
    server
        .exec(&format!(
            "CREATE USER {CLERK} WITH PASSWORD 'x' ROLE readwrite"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE USER {APPROVER} WITH PASSWORD 'x' ROLE approver"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("GRANT ROLE readwrite TO {APPROVER}"))
        .await
        .unwrap();
}

/// Create `name` with one row `a` in `draft`, guarded by the role rule.
async fn guarded_collection(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, status TEXT)"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {name} (id, status) VALUES ('a', 'draft')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "ALTER COLLECTION {name} ADD CONSTRAINT approval \
             ON COLUMN status TRANSITIONS ('draft' -> 'active' BY ROLE 'approver')"
        ))
        .await
        .unwrap();
}

async fn status(server: &TestServer, name: &str) -> String {
    let rows = server
        .query_text(&format!("SELECT status FROM {name} WHERE id = 'a'"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "row a must exist: {rows:?}");
    rows[0].clone()
}

/// Run `statement` as `user` on a fresh connection.
async fn run_as(server: &TestServer, user: &str, statement: &str) -> Result<(), String> {
    let (client, _connection) = server.connect_as(user, "x").await?;
    client
        .simple_query(statement)
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// Run `statement` as `user` inside a transaction. The refusal of a staged
/// write can land on the statement or on COMMIT, so both count.
async fn run_in_transaction_as(
    server: &TestServer,
    user: &str,
    statement: &str,
) -> Result<(), String> {
    let (client, _connection) = server.connect_as(user, "x").await?;
    client
        .simple_query("BEGIN")
        .await
        .map_err(|e| format!("{e:?}"))?;
    if let Err(e) = client.simple_query(statement).await {
        client
            .simple_query("ROLLBACK")
            .await
            .map_err(|e| format!("{e:?}"))?;
        return Err(format!("{e:?}"));
    }
    client
        .simple_query("COMMIT")
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// The clerk's `draft -> active` is refused and leaves `draft`. The
/// approver's is admitted and stores `active`.
async fn assert_role_gate(server: &TestServer, name: &str, statement: &str, staged: bool) {
    let refused = if staged {
        run_in_transaction_as(server, CLERK, statement).await
    } else {
        run_as(server, CLERK, statement).await
    };
    assert!(
        refused.is_err(),
        "a writer without the role must be refused: {statement}"
    );
    assert_eq!(status(server, name).await, "draft", "{statement}");

    let admitted = if staged {
        run_in_transaction_as(server, APPROVER, statement).await
    } else {
        run_as(server, APPROVER, statement).await
    };
    admitted
        .unwrap_or_else(|e| panic!("a writer with the role must be admitted: {statement}: {e}"));
    assert_eq!(status(server, name).await, "active", "{statement}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_update_judges_the_writer_role() {
    let server = TestServer::start().await;
    users(&server).await;
    guarded_collection(&server, "rt_point").await;
    assert_role_gate(
        &server,
        "rt_point",
        "UPDATE rt_point SET status = 'active' WHERE id = 'a'",
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_update_judges_the_writer_role() {
    let server = TestServer::start().await;
    users(&server).await;
    guarded_collection(&server, "rt_bulk").await;
    assert_role_gate(
        &server,
        "rt_bulk",
        "UPDATE rt_bulk SET status = 'active' WHERE status = 'draft'",
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_update_judges_the_committing_role() {
    let server = TestServer::start().await;
    users(&server).await;
    guarded_collection(&server, "rt_txn").await;
    assert_role_gate(
        &server,
        "rt_txn",
        "UPDATE rt_txn SET status = 'active' WHERE id = 'a'",
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_update_arm_judges_the_writer_role() {
    let server = TestServer::start().await;
    users(&server).await;
    guarded_collection(&server, "rt_merge").await;
    server
        .exec("CREATE COLLECTION rt_merge_src (id TEXT PRIMARY KEY, next TEXT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rt_merge_src (id, next) VALUES ('a', 'active')")
        .await
        .unwrap();
    assert_role_gate(
        &server,
        "rt_merge",
        "MERGE INTO rt_merge t USING rt_merge_src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET status = s.next",
        false,
    )
    .await;
}
