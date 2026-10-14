// SPDX-License-Identifier: BUSL-1.1

//! A state constraint binds every statement form that rewrites a row.
//!
//! Each case declares `status TRANSITIONS ('draft' -> 'active')` on a row in
//! `draft`, then drives one update form twice: `draft -> closed` is refused
//! and leaves the row unchanged, `draft -> active` is admitted.

use crate::harness::TestServer;

/// Create `name` with one row `a` in `draft`, constrained by the `flow`
/// transition rule. `options` is appended to the CREATE statement.
async fn flow_collection(server: &TestServer, name: &str, options: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, status TEXT){options}"
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
            "ALTER COLLECTION {name} ADD CONSTRAINT flow \
             ON COLUMN status TRANSITIONS ('draft' -> 'active')"
        ))
        .await
        .unwrap();
}

/// A source collection holding the status each target row moves to.
async fn next_status_source(server: &TestServer, name: &str, next: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, next TEXT)"
        ))
        .await
        .unwrap();
    set_next(server, name, next).await;
}

async fn set_next(server: &TestServer, name: &str, next: &str) {
    server
        .exec(&format!(
            "UPSERT INTO {name} (id, next) VALUES ('a', '{next}')"
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

/// `refused` must fail and leave `draft`. `admitted` must succeed and store
/// `active`.
async fn assert_form(server: &TestServer, name: &str, refused: &str, admitted: &str) {
    let outcome = server.exec(refused).await;
    assert!(
        outcome.is_err(),
        "draft -> closed must be refused: {refused}"
    );
    assert_eq!(status(server, name).await, "draft", "{refused}");
    server
        .exec(admitted)
        .await
        .unwrap_or_else(|e| panic!("draft -> active must be admitted: {admitted}: {e:?}"));
    assert_eq!(status(server, name).await, "active", "{admitted}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_update_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    flow_collection(&server, "stf_point", "").await;
    assert_form(
        &server,
        "stf_point",
        "UPDATE stf_point SET status = 'closed' WHERE id = 'a'",
        "UPDATE stf_point SET status = 'active' WHERE id = 'a'",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_update_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    flow_collection(&server, "stf_bulk", "").await;
    assert_form(
        &server,
        "stf_bulk",
        "UPDATE stf_bulk SET status = 'closed' WHERE status = 'draft'",
        "UPDATE stf_bulk SET status = 'active' WHERE status = 'draft'",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_from_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    flow_collection(&server, "stf_from", "").await;
    next_status_source(&server, "stf_from_src", "closed").await;
    let statement = "UPDATE stf_from SET status = stf_from_src.next FROM stf_from_src \
                     WHERE stf_from.id = stf_from_src.id";
    let outcome = server.exec(statement).await;
    assert!(outcome.is_err(), "draft -> closed must be refused");
    assert_eq!(status(&server, "stf_from").await, "draft");

    set_next(&server, "stf_from_src", "active").await;
    server.exec(statement).await.unwrap();
    assert_eq!(status(&server, "stf_from").await, "active");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_update_arm_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    flow_collection(&server, "stf_merge", "").await;
    next_status_source(&server, "stf_merge_src", "closed").await;
    let statement = "MERGE INTO stf_merge t USING stf_merge_src s ON t.id = s.id \
                     WHEN MATCHED THEN UPDATE SET status = s.next";
    let outcome = server.exec(statement).await;
    assert!(outcome.is_err(), "draft -> closed must be refused");
    assert_eq!(status(&server, "stf_merge").await, "draft");

    set_next(&server, "stf_merge_src", "active").await;
    server.exec(statement).await.unwrap();
    assert_eq!(status(&server, "stf_merge").await, "active");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_overwrite_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    flow_collection(&server, "stf_upsert", "").await;
    assert_form(
        &server,
        "stf_upsert",
        "UPSERT INTO stf_upsert (id, status) VALUES ('a', 'closed')",
        "UPSERT INTO stf_upsert (id, status) VALUES ('a', 'active')",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_do_update_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    flow_collection(&server, "stf_conflict", "").await;
    assert_form(
        &server,
        "stf_conflict",
        "INSERT INTO stf_conflict (id, status) VALUES ('a', 'x') \
         ON CONFLICT (id) DO UPDATE SET status = 'closed'",
        "INSERT INTO stf_conflict (id, status) VALUES ('a', 'x') \
         ON CONFLICT (id) DO UPDATE SET status = 'active'",
    )
    .await;
}

/// Run `statement` in a transaction. The refusal of a staged write can land
/// on the statement or on COMMIT, so both count.
async fn in_transaction(server: &TestServer, statement: &str) -> Result<(), String> {
    server.exec("BEGIN").await.unwrap();
    if let Err(e) = server.exec(statement).await {
        server.exec("ROLLBACK").await.unwrap();
        return Err(format!("{e:?}"));
    }
    server.exec("COMMIT").await.map_err(|e| format!("{e:?}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_point_update_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    flow_collection(&server, "stf_txn_point", "").await;
    let refused = in_transaction(
        &server,
        "UPDATE stf_txn_point SET status = 'closed' WHERE id = 'a'",
    )
    .await;
    assert!(refused.is_err(), "draft -> closed must be refused");
    assert_eq!(status(&server, "stf_txn_point").await, "draft");

    in_transaction(
        &server,
        "UPDATE stf_txn_point SET status = 'active' WHERE id = 'a'",
    )
    .await
    .unwrap();
    assert_eq!(status(&server, "stf_txn_point").await, "active");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_bulk_update_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    flow_collection(&server, "stf_txn_bulk", "").await;
    let refused = in_transaction(
        &server,
        "UPDATE stf_txn_bulk SET status = 'closed' WHERE status = 'draft'",
    )
    .await;
    assert!(refused.is_err(), "draft -> closed must be refused");
    assert_eq!(status(&server, "stf_txn_bulk").await, "draft");

    in_transaction(
        &server,
        "UPDATE stf_txn_bulk SET status = 'active' WHERE status = 'draft'",
    )
    .await
    .unwrap();
    assert_eq!(status(&server, "stf_txn_bulk").await, "active");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_update_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    flow_collection(&server, "stf_crdt", " WITH (crdt='true')").await;
    assert_form(
        &server,
        "stf_crdt",
        "UPDATE stf_crdt SET status = 'closed' WHERE id = 'a'",
        "UPDATE stf_crdt SET status = 'active' WHERE id = 'a'",
    )
    .await;
}
