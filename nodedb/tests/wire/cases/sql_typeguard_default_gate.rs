// SPDX-License-Identifier: BUSL-1.1

//! A typeguard `DEFAULT` / `VALUE` the engine cannot evaluate is refused at
//! declaration.
//!
//! Both clauses produce a field value on every write. An unregistered function
//! name would otherwise store `NULL` on every write and report nothing, and a
//! non-deterministic call would fail every write instead of the declaration.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_unevaluable_default_is_refused_at_declaration() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION tg_gate").await.unwrap();

    // A function name no registry knows.
    server
        .expect_error(
            "CREATE TYPEGUARD ON tg_gate (status STRING DEFAULT no_such_function())",
            "42883",
        )
        .await;

    // The same refusal covers the VALUE clause.
    server
        .expect_error(
            "CREATE TYPEGUARD ON tg_gate (computed STRING VALUE no_such_function())",
            "42883",
        )
        .await;

    // A call the write path refuses as non-deterministic.
    server
        .expect_error(
            "CREATE TYPEGUARD ON tg_gate (at STRING DEFAULT now())",
            "42601",
        )
        .await;

    // ALTER carries the same refusal.
    server
        .expect_error(
            "ALTER TYPEGUARD ON tg_gate ADD status STRING DEFAULT no_such_function()",
            "42883",
        )
        .await;

    // A refused declaration reaches no storage.
    let rows = server
        .query_text("SHOW TYPEGUARD ON tg_gate")
        .await
        .unwrap();
    assert_eq!(rows.len(), 0, "refused guard must not be stored: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_evaluable_defaults_stay_accepted() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION tg_gate_ok").await.unwrap();

    server
        .exec(
            "CREATE TYPEGUARD ON tg_gate_ok (\
                 status STRING DEFAULT 'draft',\
                 version INT REQUIRED DEFAULT 1\
             )",
        )
        .await
        .unwrap();

    server
        .exec("ALTER TYPEGUARD ON tg_gate_ok ADD slug STRING VALUE LOWER(status)")
        .await
        .unwrap();

    let rows = server
        .query_text("SHOW TYPEGUARD ON tg_gate_ok")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "every accepted guard is stored: {rows:?}");

    // The accepted DEFAULT still injects at write time.
    server
        .exec("INSERT INTO tg_gate_ok { id: 'g1', name: 'Alice' }")
        .await
        .unwrap();

    let stored = server
        .query_text_joined("SELECT * FROM tg_gate_ok WHERE id = 'g1'")
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert!(
        stored[0].contains("draft"),
        "DEFAULT must still inject: {stored:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_bare_generator_keyword_is_refused() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION tg_gate_gen").await.unwrap();

    // The write path reads a bare `UUID_V7` as a field, which stores that
    // field's value instead of generating one, so the declaration is refused.
    server
        .expect_error(
            "CREATE TYPEGUARD ON tg_gate_gen (k STRING DEFAULT UUID_V7)",
            "42601",
        )
        .await;
    server
        .expect_error(
            "CREATE TYPEGUARD ON tg_gate_gen (at STRING VALUE current_timestamp)",
            "42601",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_collection_refuses_default_and_value_guards() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION tg_gate_crdt (id TEXT PRIMARY KEY, v INT) WITH (crdt='true')")
        .await
        .unwrap();

    // Every replica merges the CRDT state, so the server injects nothing.
    server
        .expect_error(
            "CREATE TYPEGUARD ON tg_gate_crdt (v INT DEFAULT 1)",
            "0A000",
        )
        .await;
    server
        .expect_error("CREATE TYPEGUARD ON tg_gate_crdt (v INT VALUE 1)", "0A000")
        .await;

    // A validating guard is accepted.
    server
        .exec("CREATE TYPEGUARD ON tg_gate_crdt (v INT CHECK (v >= 0))")
        .await
        .unwrap();
}
