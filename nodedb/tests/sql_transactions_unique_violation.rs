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

    // NOTE on WHERE the violation surfaces: NodeDB currently *defers execution*
    // of in-transaction writes until COMMIT (the buffered-write model), so the
    // duplicate INSERT returns OK at statement time and the UNIQUE violation is
    // raised when the buffered batch executes at COMMIT. PostgreSQL raises it at
    // the offending statement; making NodeDB do the same is the job of the
    // staged-write execution redesign. What this test locks in regardless of
    // that timing is the *correctness* property U6a delivered: UNIQUE IS enforced
    // inside a transaction — the duplicate is rejected, never silently accepted.
    server
        .client
        .simple_query("INSERT INTO tx_dup (id, n) VALUES ('dup', 2)")
        .await
        .expect("buffered in-tx INSERT returns OK at statement time (deferred execution)");

    // COMMIT must reject the duplicate with SQLSTATE 23505.
    match server.client.simple_query("COMMIT").await {
        Ok(_) => panic!("COMMIT must reject the duplicate-PK insert — UNIQUE unenforced in tx"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError at COMMIT");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505 at COMMIT, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
        }
    }

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
