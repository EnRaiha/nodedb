// SPDX-License-Identifier: BUSL-1.1

//! Every write form enforces a collection's typeguards.
//!
//! The guards run on the Data Plane, on the body each write stores:
//! `INSERT … VALUES`, `INSERT … SELECT`, UPDATE, UPSERT and transaction
//! staging all reach the same hook as the `{ }` form.

use crate::harness::TestServer;

/// A typeguard violation's SQLSTATE.
const TYPE_GUARD_VIOLATION: &str = "23608";

async fn guarded(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE TYPEGUARD ON {name} (\
                 email STRING REQUIRED CHECK (email LIKE '%@%'), \
                 role STRING DEFAULT 'user', \
                 slug STRING VALUE LOWER(title)\
             )"
        ))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn values_insert_enforces_guards() {
    let server = TestServer::start().await;
    guarded(&server, "wf_values").await;

    server
        .expect_error(
            "INSERT INTO wf_values (id, title) VALUES ('a', 'Hi')",
            TYPE_GUARD_VIOLATION,
        )
        .await;
    server
        .exec("INSERT INTO wf_values (id, title, email) VALUES ('b', 'Hi', 'x@y.z')")
        .await
        .unwrap();
    let rows = server
        .query_text_joined("SELECT role, slug FROM wf_values WHERE id = 'b'")
        .await
        .unwrap();
    assert!(
        rows.iter()
            .any(|row| row.contains("user") && row.contains("hi")),
        "DEFAULT and VALUE must inject: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn values_insert_returning_shows_injected_fields() {
    let server = TestServer::start().await;
    guarded(&server, "wf_returning").await;

    let rows = server
        .query_named_rows(
            "INSERT INTO wf_returning (id, title, email) VALUES ('r1', 'Hi', 'x@y.z') \
             RETURNING role, slug",
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get("role").map(String::as_str), Some("user"));
    assert_eq!(rows[0].get("slug").map(String::as_str), Some("hi"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_enforces_guards() {
    let server = TestServer::start().await;
    guarded(&server, "wf_target").await;
    server.exec("CREATE COLLECTION wf_source").await.unwrap();
    server
        .exec("INSERT INTO wf_source { id: 's1', title: 'Hi' }")
        .await
        .unwrap();

    server
        .expect_error(
            "INSERT INTO wf_target (id, title) SELECT id, title FROM wf_source",
            TYPE_GUARD_VIOLATION,
        )
        .await;

    server
        .exec("INSERT INTO wf_source { id: 's2', title: 'Ok Now', email: 'a@b.c' }")
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO wf_target (id, title, email) \
             SELECT id, title, email FROM wf_source WHERE id = 's2'",
        )
        .await
        .unwrap();
    let rows = server
        .query_text_joined("SELECT slug FROM wf_target WHERE id = 's2'")
        .await
        .unwrap();
    assert!(rows.iter().any(|row| row.contains("ok now")), "{rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_recomputes_value_and_refills_default() {
    let server = TestServer::start().await;
    guarded(&server, "wf_update").await;
    server
        .exec("INSERT INTO wf_update { id: 'u1', title: 'Hi', email: 'x@y.z' }")
        .await
        .unwrap();

    server
        .exec("UPDATE wf_update SET title = 'New X' WHERE id = 'u1'")
        .await
        .unwrap();
    let slug = server
        .query_text("SELECT slug FROM wf_update WHERE id = 'u1'")
        .await
        .unwrap();
    assert!(slug[0].contains("new x"), "VALUE must recompute: {slug:?}");

    server
        .exec("UPDATE wf_update SET role = NULL WHERE id = 'u1'")
        .await
        .unwrap();
    let role = server
        .query_text("SELECT role FROM wf_update WHERE id = 'u1'")
        .await
        .unwrap();
    assert!(role[0].contains("user"), "DEFAULT must refill: {role:?}");

    server
        .expect_error(
            "UPDATE wf_update SET email = 'bad' WHERE id = 'u1'",
            TYPE_GUARD_VIOLATION,
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_of_a_row_predating_the_guard() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION wf_legacy").await.unwrap();
    server
        .exec("INSERT INTO wf_legacy { id: 'l1', title: 'Old' }")
        .await
        .unwrap();
    server
        .exec("CREATE TYPEGUARD ON wf_legacy (email STRING REQUIRED)")
        .await
        .unwrap();

    // The UPDATE assigns `title` only, so the missing `email` is not judged.
    server
        .exec("UPDATE wf_legacy SET title = 'z' WHERE id = 'l1'")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_merge_enforces_guards() {
    let server = TestServer::start().await;
    guarded(&server, "wf_upsert").await;
    server
        .exec("INSERT INTO wf_upsert { id: 'm1', title: 'Hi', email: 'x@y.z' }")
        .await
        .unwrap();

    server
        .exec("UPSERT INTO wf_upsert { id: 'm1', title: 'Merged Row' }")
        .await
        .unwrap();
    let slug = server
        .query_text("SELECT slug FROM wf_upsert WHERE id = 'm1'")
        .await
        .unwrap();
    assert!(
        slug[0].contains("merged row"),
        "VALUE must recompute: {slug:?}"
    );

    server
        .expect_error(
            "UPSERT INTO wf_upsert { id: 'm1', email: 'bad' }",
            TYPE_GUARD_VIOLATION,
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_transaction_enforces_guards() {
    let server = TestServer::start().await;
    guarded(&server, "wf_txn").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO wf_txn (id, title, email) VALUES ('t1', 'In Txn', 'x@y.z')")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();
    let rows = server
        .query_text_joined("SELECT slug, role FROM wf_txn WHERE id = 't1'")
        .await
        .unwrap();
    assert!(
        rows.iter()
            .any(|row| row.contains("in txn") && row.contains("user")),
        "{rows:?}"
    );

    server.exec("BEGIN").await.unwrap();
    server
        .expect_error(
            "INSERT INTO wf_txn (id, title) VALUES ('t2', 'No Email')",
            TYPE_GUARD_VIOLATION,
        )
        .await;
    server.exec("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_writes_enforce_guards_on_the_merged_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION wf_crdt (id TEXT PRIMARY KEY, v INT) WITH (crdt='true')")
        .await
        .unwrap();
    server
        .exec("CREATE TYPEGUARD ON wf_crdt (v INT REQUIRED CHECK (v >= 0))")
        .await
        .unwrap();

    server
        .expect_error(
            "INSERT INTO wf_crdt (id) VALUES ('c0')",
            TYPE_GUARD_VIOLATION,
        )
        .await;
    server
        .exec("INSERT INTO wf_crdt (id, v) VALUES ('c1', 1)")
        .await
        .unwrap();

    // A partial UPDATE merges into the Loro row. The merged row is judged.
    server
        .expect_error(
            "UPDATE wf_crdt SET v = -5 WHERE id = 'c1'",
            TYPE_GUARD_VIOLATION,
        )
        .await;
    let rows = server
        .query_text("SELECT v FROM wf_crdt WHERE id = 'c1'")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["1".to_string()],
        "the refused UPDATE keeps the row"
    );

    server
        .exec("UPDATE wf_crdt SET v = 7 WHERE id = 'c1'")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT v FROM wf_crdt WHERE id = 'c1'")
        .await
        .unwrap();
    assert_eq!(rows, vec!["7".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_violation_names_the_collection_the_user_wrote() {
    let server = TestServer::start().await;
    server.exec("CREATE DATABASE wf_named").await.unwrap();
    server.exec("USE DATABASE wf_named").await.unwrap();
    server.exec("CREATE COLLECTION wf_people").await.unwrap();
    server
        .exec("CREATE TYPEGUARD ON wf_people (email STRING REQUIRED)")
        .await
        .unwrap();

    let message = server
        .exec("INSERT INTO wf_people (id) VALUES ('p1')")
        .await
        .expect_err("email is required");
    assert!(
        message.contains("type guard violation on wf_people:"),
        "the message names the collection the user wrote: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_typeguard_takes_effect_without_restart() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION wf_alter").await.unwrap();
    server
        .exec("INSERT INTO wf_alter (id, title) VALUES ('a1', 'Hi')")
        .await
        .unwrap();
    server
        .exec("ALTER TYPEGUARD ON wf_alter ADD email STRING REQUIRED")
        .await
        .unwrap();
    server
        .expect_error(
            "INSERT INTO wf_alter (id, title) VALUES ('a2', 'Hi')",
            TYPE_GUARD_VIOLATION,
        )
        .await;
}
