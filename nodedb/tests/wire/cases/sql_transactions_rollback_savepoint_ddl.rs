// SPDX-License-Identifier: BUSL-1.1

//! `ROLLBACK TO SAVEPOINT` must discard DDL issued after the savepoint, not
//! just DML. Before this fix, the task-local DDL buffer had no savepoint
//! marker, so a CREATE COLLECTION issued after SAVEPOINT still committed even
//! after ROLLBACK TO reverted the transaction to that savepoint.

use crate::harness::TestServer;

async fn collection_names(server: &TestServer) -> Vec<String> {
    server
        .query_rows("SHOW COLLECTIONS")
        .await
        .expect("SHOW COLLECTIONS must succeed")
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect()
}

async fn exists(server: &TestServer, name: &str) -> bool {
    collection_names(server).await.iter().any(|n| n == name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_savepoint_discards_ddl_issued_after_it() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION sp_ddl_first (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server.exec("SAVEPOINT s1").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION sp_ddl_second (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server.exec("ROLLBACK TO SAVEPOINT s1").await.unwrap();
    server.exec("COMMIT").await.unwrap();

    assert!(
        exists(&server, "sp_ddl_first").await,
        "sp_ddl_first was created before the savepoint and must survive"
    );
    assert!(
        !exists(&server, "sp_ddl_second").await,
        "sp_ddl_second was created after the savepoint and must be discarded by ROLLBACK TO"
    );

    // The name is reusable: the DDL entry, not just its visibility, was
    // discarded from the buffer — a leaked entry would fail this CREATE with
    // a duplicate-name error on flush.
    server
        .exec(
            "CREATE COLLECTION sp_ddl_second (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    assert!(exists(&server, "sp_ddl_second").await);
}
