// SPDX-License-Identifier: BUSL-1.1

//! Typeguard DDL resolves the collection in the session's database.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_ddl_resolves_in_the_session_database() {
    let server = TestServer::start().await;

    server.exec("CREATE DATABASE tg_db").await.unwrap();
    server.exec("USE DATABASE tg_db").await.unwrap();
    server.exec("CREATE COLLECTION tgd").await.unwrap();

    server
        .exec("CREATE TYPEGUARD ON tgd (name STRING REQUIRED)")
        .await
        .unwrap();
    let guards = server.query_text("SHOW TYPEGUARD ON tgd").await.unwrap();
    assert_eq!(guards.len(), 1, "the guard must be stored: {guards:?}");
    let listed = server.query_text("SHOW TYPEGUARDS").await.unwrap();
    assert!(
        listed.iter().any(|row| row.contains("tgd")),
        "SHOW TYPEGUARDS must list the collection: {listed:?}"
    );

    // The guard is enforced in the session database.
    server
        .expect_error("INSERT INTO tgd { id: 'x' }", "23608")
        .await;
    server
        .exec("INSERT INTO tgd { id: 'y', name: 'ok' }")
        .await
        .unwrap();

    server
        .exec("ALTER TYPEGUARD ON tgd ADD score INT")
        .await
        .unwrap();
    server
        .exec("ALTER TYPEGUARD ON tgd DROP score")
        .await
        .unwrap();
    let violations = server
        .query_text("VALIDATE TYPEGUARD ON tgd")
        .await
        .unwrap();
    assert!(
        violations.is_empty(),
        "no stored row violates: {violations:?}"
    );
    server.exec("DROP TYPEGUARD ON tgd").await.unwrap();
}
