// SPDX-License-Identifier: BUSL-1.1

//! A write to a clone inside BEGIN..COMMIT takes the same copy-on-write
//! steps as an autocommit one. Its copy-up and tombstones commit with the
//! transaction and roll back with it, and the source never changes.

use crate::harness::TestServer;

/// A source database with `p1` and `p2`, and a clone of it. Leaves the
/// session in the clone.
async fn clone_with_two_rows(server: &TestServer, source: &str, clone: &str) {
    for sql in [
        format!("CREATE DATABASE {source}"),
        format!("USE DATABASE {source}"),
        "CREATE COLLECTION products (key STRING PRIMARY KEY, price STRING) \
         WITH (engine='kv')"
            .to_string(),
        "INSERT INTO products (key, price) VALUES ('p1', '10')".to_string(),
        "INSERT INTO products (key, price) VALUES ('p2', '20')".to_string(),
        "USE DATABASE default".to_string(),
        format!("CLONE DATABASE {clone} FROM {source} LATEST"),
        format!("USE DATABASE {clone}"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

async fn prices(server: &TestServer, database: &str) -> Vec<Vec<String>> {
    server
        .exec(&format!("USE DATABASE {database}"))
        .await
        .unwrap_or_else(|e| panic!("USE {database}: {e}"));
    server
        .query_rows("SELECT key, price FROM products ORDER BY key")
        .await
        .unwrap_or_else(|e| panic!("read {database}: {e}"))
}

fn row(key: &str, price: &str) -> Vec<String> {
    vec![key.to_string(), price.to_string()]
}

/// A committed update and delete on source-only rows land in the clone, and
/// the source keeps its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_clone_write_lands_in_the_clone_only() {
    let server = TestServer::start().await;
    clone_with_two_rows(&server, "cwt_src", "cwt_clone").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE products SET price = '99' WHERE key = 'p1'")
        .await
        .unwrap();
    server
        .exec("DELETE FROM products WHERE key = 'p2'")
        .await
        .unwrap();
    assert_eq!(
        server
            .query_rows("SELECT key, price FROM products ORDER BY key")
            .await
            .unwrap(),
        vec![row("p1", "99")],
        "the transaction reads its own clone writes"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(prices(&server, "cwt_clone").await, vec![row("p1", "99")]);
    assert_eq!(
        prices(&server, "cwt_src").await,
        vec![row("p1", "10"), row("p2", "20")],
        "the source never changes"
    );
}

/// A rolled-back update and delete leave the clone reading the source rows:
/// no copy-up and no tombstone survives the rollback.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolled_back_clone_write_leaves_nothing() {
    let server = TestServer::start().await;
    clone_with_two_rows(&server, "cwr_src", "cwr_clone").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE products SET price = '99' WHERE key = 'p1'")
        .await
        .unwrap();
    server
        .exec("DELETE FROM products WHERE key = 'p2'")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    assert_eq!(
        prices(&server, "cwr_clone").await,
        vec![row("p1", "10"), row("p2", "20")]
    );
    assert_eq!(
        prices(&server, "cwr_src").await,
        vec![row("p1", "10"), row("p2", "20")]
    );
}
