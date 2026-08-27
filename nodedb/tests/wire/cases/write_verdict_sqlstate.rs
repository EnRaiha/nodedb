// SPDX-License-Identifier: BUSL-1.1

//! SQLSTATE of a write verdict reached in the Data Plane and carried back
//! through the Raft propose path. The verdict is typed all the way out: a
//! constraint refusal is 23505 and a row-level-security refusal is 42501,
//! never the XX000 a flattened error string collapses to.

use crate::harness::TestServer;

const PASSWORD: &str = "verdict-sqlstate-secret-42";

/// The SQLSTATE the server answered `sql` with, or `None` when it succeeded.
async fn sqlstate_of(server: &TestServer, sql: &str) -> Option<String> {
    match server.client.simple_query(sql).await {
        Ok(_) => None,
        Err(e) => Some(
            e.as_db_error()
                .unwrap_or_else(|| panic!("expected a DbError from {sql}, got: {e}"))
                .code()
                .code()
                .to_string(),
        ),
    }
}

/// A duplicate primary key is a constraint verdict, so it reaches the client
/// as `unique_violation` with the conflicting key named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_primary_key_raises_unique_violation() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION vs_dup (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .expect("create vs_dup");
    server
        .exec("INSERT INTO vs_dup (id, n) VALUES ('dup', 1)")
        .await
        .expect("seed vs_dup");

    let error = server
        .client
        .simple_query("INSERT INTO vs_dup (id, n) VALUES ('dup', 2)")
        .await
        .expect_err("a duplicate primary key must be refused");
    let db_err = error.as_db_error().expect("expected a DbError");

    assert_eq!(
        db_err.code().code(),
        "23505",
        "expected unique_violation, got {}: {}",
        db_err.code().code(),
        db_err.message()
    );
    assert!(
        db_err.message().to_lowercase().contains("dup"),
        "the message must name the conflicting key, got: {}",
        db_err.message()
    );
}

/// A row-level-security refusal is decided where the row is persisted, so it
/// travels the same propose path and must keep its own SQLSTATE rather than
/// borrowing the constraint one or collapsing to XX000.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rls_write_refusal_raises_insufficient_privilege() {
    let server = TestServer::start().await;
    let user = "vs_rls_user";

    server
        .exec("CREATE COLLECTION vs_rls (id TEXT PRIMARY KEY, owner TEXT, note TEXT) WITH (engine='document_strict')")
        .await
        .expect("create vs_rls");
    server
        .exec(&format!(
            "INSERT INTO vs_rls (id, owner, note) VALUES ('r_mine', '{user}', 'before')"
        ))
        .await
        .expect("seed vs_rls");
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .expect("create user");
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY vs_rls_owner ON vs_rls FOR WRITE \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create write policy");

    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .expect("connect as the probing user");
    let error = client
        .simple_query("UPDATE vs_rls SET owner = 'alice' WHERE id = 'r_mine'")
        .await
        .expect_err("handing the row to another owner must be refused");
    let db_err = error.as_db_error().expect("expected a DbError");

    assert_eq!(
        db_err.code().code(),
        "42501",
        "expected insufficient_privilege, got {}: {}",
        db_err.code().code(),
        db_err.message()
    );

    drop(client);
    handle.abort();
}

/// The verdict is not a blanket refusal: a write that breaks nothing still
/// succeeds on the same path, so neither assertion above passes by accident.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conforming_write_returns_no_sqlstate() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION vs_ok (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .expect("create vs_ok");

    assert_eq!(
        sqlstate_of(&server, "INSERT INTO vs_ok (id, n) VALUES ('a', 1)").await,
        None,
        "a conforming insert must succeed"
    );
}
