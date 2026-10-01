// SPDX-License-Identifier: BUSL-1.1

//! A UNIQUE / PK violation inside a `BEGIN;...;` transaction must be
//! rejected with SQLSTATE 23505, exactly as it is outside a transaction —
//! a transaction context must not silently accept a duplicate PK insert.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tx_duplicate_pk_insert_raises_unique_violation() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION tx_dup  \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO tx_dup (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    // In-transaction point writes execute at statement time, so a duplicate
    // primary key is rejected at the offending statement, not deferred to commit.
    match server
        .client
        .simple_query("INSERT INTO tx_dup (id, n) VALUES ('dup', 2)")
        .await
    {
        Ok(_) => panic!(
            "duplicate-PK insert must raise 23505 at the statement — UNIQUE unenforced in tx"
        ),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError at the statement");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505 at the statement, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
        }
    }

    // The transaction is now aborted; ROLLBACK returns to a clean state.
    let _ = server.client.simple_query("ROLLBACK").await;

    // The duplicate must not be present / must not have overwritten the original.
    let rows = server
        .query_text("SELECT n FROM tx_dup WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "exactly the original row must remain, got {rows:?}"
    );
    assert_eq!(
        rows[0], "1",
        "duplicate-PK INSERT must not have overwritten the original row, got: {}",
        rows[0]
    );
}

// ── UNIQUE judged on the post-state of a statement or transaction ─────────
//
// A value one row releases is free for another row of the same statement or
// transaction, in whatever order the rows are written. Two rows claiming one
// value, or a claim on a value an untouched row holds, raise 23505.

/// Create `name` with a UNIQUE index on `code` and rows `a` = 'A', `b` = 'B'.
async fn seed_codes(server: &TestServer, name: &str, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id STRING NOT NULL PRIMARY KEY, code STRING) \
             WITH (engine='{engine}')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE UNIQUE INDEX idx_{name}_code ON {name} (code)"
        ))
        .await
        .unwrap();
    for (id, code) in [("a", "A"), ("b", "B")] {
        server
            .exec(&format!(
                "INSERT INTO {name} (id, code) VALUES ('{id}', '{code}')"
            ))
            .await
            .unwrap();
    }
}

async fn code_of(server: &TestServer, name: &str, id: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT code FROM {name} WHERE id = '{id}'"))
        .await
        .unwrap()
}

async fn assert_unique_violation(server: &TestServer, sql: &str) {
    match server.client.simple_query(sql).await {
        Ok(_) => panic!("expected SQLSTATE 23505 for: {sql}"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected a DbError");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_swap_in_one_update_succeeds() {
    let server = TestServer::start().await;
    for (name, engine) in [
        ("uq_swap_schemaless", "document_schemaless"),
        ("uq_swap_strict", "document_strict"),
    ] {
        seed_codes(&server, name, engine).await;
        server
            .exec(&format!(
                "UPDATE {name} SET code = CASE WHEN id = 'a' THEN 'B' ELSE 'A' END \
                 WHERE id IN ('a', 'b')"
            ))
            .await
            .unwrap_or_else(|e| panic!("{engine}: a swap must not raise a unique violation: {e}"));
        assert_eq!(code_of(&server, name, "a").await, vec!["B"], "{engine}");
        assert_eq!(code_of(&server, name, "b").await, vec!["A"], "{engine}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_release_then_claim_in_one_transaction_succeeds() {
    let server = TestServer::start().await;
    seed_codes(&server, "uq_handover", "document_schemaless").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE uq_handover SET code = 'X' WHERE id = 'a'")
        .await
        .unwrap();
    server
        .exec("INSERT INTO uq_handover (id, code) VALUES ('c', 'A')")
        .await
        .unwrap_or_else(|e| panic!("'A' was released earlier in the transaction: {e}"));
    server
        .exec("UPDATE uq_handover SET code = 'X2' WHERE id = 'b'")
        .await
        .unwrap();
    server
        .exec("UPDATE uq_handover SET code = 'B' WHERE id = 'a'")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    assert_eq!(code_of(&server, "uq_handover", "a").await, vec!["B"]);
    assert_eq!(code_of(&server, "uq_handover", "b").await, vec!["X2"]);
    assert_eq!(code_of(&server, "uq_handover", "c").await, vec!["A"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_two_rows_claiming_one_new_value_raise_23505() {
    let server = TestServer::start().await;
    seed_codes(&server, "uq_dup_claim", "document_schemaless").await;

    assert_unique_violation(
        &server,
        "UPDATE uq_dup_claim SET code = 'Z' WHERE id IN ('a', 'b')",
    )
    .await;
    // The statement is all or nothing: neither row took 'Z'.
    assert_eq!(code_of(&server, "uq_dup_claim", "a").await, vec!["A"]);
    assert_eq!(code_of(&server, "uq_dup_claim", "b").await, vec!["B"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_claim_on_a_value_an_untouched_row_holds_raises_23505() {
    let server = TestServer::start().await;
    seed_codes(&server, "uq_held", "document_schemaless").await;

    assert_unique_violation(&server, "UPDATE uq_held SET code = 'B' WHERE id = 'a'").await;
    assert_eq!(code_of(&server, "uq_held", "a").await, vec!["A"]);
    assert_eq!(code_of(&server, "uq_held", "b").await, vec!["B"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_staged_point_update_raises_23505_at_the_statement() {
    let server = TestServer::start().await;
    seed_codes(&server, "uq_staged_point", "document_schemaless").await;

    server.exec("BEGIN").await.unwrap();
    assert_unique_violation(
        &server,
        "UPDATE uq_staged_point SET code = 'B' WHERE id = 'a'",
    )
    .await;
    let _ = server.client.simple_query("ROLLBACK").await;

    assert_eq!(code_of(&server, "uq_staged_point", "a").await, vec!["A"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_staged_bulk_update_is_judged_at_the_statement() {
    let server = TestServer::start().await;
    seed_codes(&server, "uq_staged_bulk", "document_schemaless").await;

    // Two matched rows claiming one value fail the statement itself.
    server.exec("BEGIN").await.unwrap();
    assert_unique_violation(
        &server,
        "UPDATE uq_staged_bulk SET code = 'Z' WHERE id IN ('a', 'b')",
    )
    .await;
    let _ = server.client.simple_query("ROLLBACK").await;

    // A swap inside one staged statement is legal and commits.
    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "UPDATE uq_staged_bulk SET code = CASE WHEN id = 'a' THEN 'B' ELSE 'A' END \
             WHERE id IN ('a', 'b')",
        )
        .await
        .unwrap_or_else(|e| panic!("a staged swap must not raise a unique violation: {e}"));
    server.exec("COMMIT").await.unwrap();

    assert_eq!(code_of(&server, "uq_staged_bulk", "a").await, vec!["B"]);
    assert_eq!(code_of(&server, "uq_staged_bulk", "b").await, vec!["A"]);
}

/// A UNIQUE index on a generated column is judged on the computed value, in
/// autocommit, at a staged statement, and at COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_on_a_generated_column_judges_the_computed_value() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION uq_gen (id STRING NOT NULL PRIMARY KEY, first TEXT, last TEXT, \
             full_name TEXT GENERATED ALWAYS AS (first || ' ' || last)) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX idx_uq_gen_full ON uq_gen (full_name)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO uq_gen (id, first, last) VALUES ('a', 'Ann', 'Lee')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO uq_gen (id, first, last) VALUES ('b', 'Bo', 'Kim')")
        .await
        .unwrap();

    // Autocommit: the computed 'Ann Lee' is already owned by `a`.
    assert_unique_violation(
        &server,
        "INSERT INTO uq_gen (id, first, last) VALUES ('c', 'Ann', 'Lee')",
    )
    .await;

    // Staged: the computed 'Bo Kim' is already owned by `b`.
    server.exec("BEGIN").await.unwrap();
    assert_unique_violation(
        &server,
        "UPDATE uq_gen SET first = 'Bo', last = 'Kim' WHERE id = 'a'",
    )
    .await;
    let _ = server.client.simple_query("ROLLBACK").await;

    // A swap of the computed values inside one transaction commits.
    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "UPDATE uq_gen SET \
             first = CASE WHEN id = 'a' THEN 'Bo' ELSE 'Ann' END, \
             last = CASE WHEN id = 'a' THEN 'Kim' ELSE 'Lee' END \
             WHERE id IN ('a', 'b')",
        )
        .await
        .unwrap_or_else(|e| panic!("a swap of computed values is legal: {e}"));
    server.exec("COMMIT").await.unwrap();

    let a = server
        .query_text("SELECT full_name FROM uq_gen WHERE id = 'a'")
        .await
        .unwrap();
    let b = server
        .query_text("SELECT full_name FROM uq_gen WHERE id = 'b'")
        .await
        .unwrap();
    assert_eq!(a, vec!["Bo Kim"]);
    assert_eq!(b, vec!["Ann Lee"]);
}
