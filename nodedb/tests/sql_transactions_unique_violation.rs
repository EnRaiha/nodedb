// SPDX-License-Identifier: BUSL-1.1

//! A UNIQUE / PK violation inside a `BEGIN;...;` transaction must be
//! rejected with SQLSTATE 23505, exactly as it is outside a transaction —
//! a transaction context must not silently accept a duplicate PK insert.

mod common;

use common::pgwire_harness::TestServer;

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

    // In-transaction point writes execute at STATEMENT time (staged into the
    // per-transaction overlay), so a duplicate primary key is rejected with
    // SQLSTATE 23505 AT THE OFFENDING STATEMENT — as PostgreSQL does — rather
    // than deferred to COMMIT. The correctness property is unchanged: UNIQUE is
    // enforced inside a transaction and the duplicate is never applied.
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
