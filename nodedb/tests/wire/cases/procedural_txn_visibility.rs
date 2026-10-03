// SPDX-License-Identifier: BUSL-1.1

//! A procedural body runs as one transaction whose statements see each
//! other's writes: a statement that reads a collection sees the rows an
//! earlier statement of the same body wrote, before the body commits.

use std::time::Duration;

use crate::harness::TestServer;

/// A procedure inserts a row, then copies the collection with
/// `INSERT ... SELECT`. The copy sees the uncommitted row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedure_statement_sees_an_earlier_statements_write() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ptv_stage").await.unwrap();
    server.exec("CREATE COLLECTION ptv_seen").await.unwrap();
    server
        .exec(
            "CREATE PROCEDURE ptv_copy() AS \
             BEGIN \
               INSERT INTO ptv_stage (id, val) VALUES ('a', 1); \
               INSERT INTO ptv_seen SELECT * FROM ptv_stage; \
             END",
        )
        .await
        .unwrap();

    server.exec("CALL ptv_copy()").await.unwrap();

    let seen = server.query_text("SELECT id FROM ptv_seen").await.unwrap();
    assert_eq!(
        seen,
        vec!["a".to_string()],
        "the copy must see the row the same body inserted"
    );
}

/// A procedure that fails after its first write leaves nothing behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_procedure_applies_nothing() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ptv_partial").await.unwrap();
    server
        .exec(
            "CREATE PROCEDURE ptv_fail() AS \
             BEGIN \
               INSERT INTO ptv_partial (id, val) VALUES ('a', 1); \
               RAISE EXCEPTION 'stop'; \
             END",
        )
        .await
        .unwrap();

    server.expect_error("CALL ptv_fail()", "stop").await;

    let rows = server
        .query_text("SELECT id FROM ptv_partial")
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "a failed body applies nothing, got {rows:?}"
    );
}

/// An AFTER trigger body inserts a row, then copies the collection. The copy
/// sees the row written earlier in the same body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trigger_statement_sees_an_earlier_statements_write() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ptv_src").await.unwrap();
    server.exec("CREATE COLLECTION ptv_tstage").await.unwrap();
    server.exec("CREATE COLLECTION ptv_tseen").await.unwrap();
    server
        .exec(
            "CREATE TRIGGER ptv_copy_trig AFTER INSERT ON ptv_src FOR EACH ROW \
             BEGIN \
               INSERT INTO ptv_tstage (id, val) VALUES (NEW.id || '-s', 1); \
               INSERT INTO ptv_tseen SELECT * FROM ptv_tstage; \
             END;",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO ptv_src (id, val) VALUES ('o1', 1)")
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let seen = server.query_text("SELECT id FROM ptv_tseen").await.unwrap();
        if !seen.is_empty() {
            assert_eq!(seen, vec!["o1-s".to_string()], "got: {seen:?}");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the trigger body's copy"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// DDL in a body buffers with its writes: a committed body creates the
/// collection, a failed body leaves none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn body_ddl_commits_and_rolls_back_with_the_body() {
    let server = TestServer::start().await;
    server
        .exec("CREATE PROCEDURE ptv_ddl_ok() AS BEGIN CREATE COLLECTION ptv_made; END")
        .await
        .unwrap();
    server
        .exec(
            "CREATE PROCEDURE ptv_ddl_fail() AS \
             BEGIN \
               CREATE COLLECTION ptv_unmade; \
               RAISE EXCEPTION 'stop'; \
             END",
        )
        .await
        .unwrap();

    server.exec("CALL ptv_ddl_ok()").await.unwrap();
    server
        .query_text("SELECT id FROM ptv_made")
        .await
        .expect("the committed body's collection exists");

    server.expect_error("CALL ptv_ddl_fail()", "stop").await;
    assert!(
        server
            .query_text("SELECT id FROM ptv_unmade")
            .await
            .is_err(),
        "the failed body's collection must not exist"
    );
}

/// A body that inserts into a collection feeding a materialized SUM credits
/// the target, whether the target shares the source's vShard or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn body_insert_credits_a_materialized_sum() {
    let server = TestServer::start().await;
    for (target, source) in [("uo_accounts", "uo_entries"), ("ptv_acct", "ptv_post")] {
        server
            .exec(&format!(
                "CREATE COLLECTION {target} (id TEXT PRIMARY KEY, owner TEXT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap();
        server
            .exec(&format!(
                "CREATE COLLECTION {source} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap();
        server
            .exec(&format!(
                "ALTER COLLECTION {target} ADD COLUMN balance TEXT \
                 MATERIALIZED_SUM SOURCE {source} \
                 ON {source}.account_id = {target}.id VALUE {source}.amount"
            ))
            .await
            .unwrap();
        server
            .exec(&format!(
                "INSERT INTO {target} (id, owner, balance) VALUES ('acc', 'alice', '0')"
            ))
            .await
            .unwrap();
        server
            .exec(&format!(
                "CREATE PROCEDURE {source}_post() AS \
                 BEGIN \
                   INSERT INTO {source} (id, account_id, amount) VALUES ('e1', 'acc', '25'); \
                 END"
            ))
            .await
            .unwrap();

        server.exec(&format!("CALL {source}_post()")).await.unwrap();

        let balance = server
            .query_text(&format!("SELECT balance FROM {target} WHERE id = 'acc'"))
            .await
            .unwrap();
        assert_eq!(balance, vec!["25".to_string()], "{target} is credited");
    }
}
