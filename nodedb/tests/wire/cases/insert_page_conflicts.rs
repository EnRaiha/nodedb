// SPDX-License-Identifier: BUSL-1.1

//! A multi-row document INSERT is one page, and each of its rows is judged
//! as a single-row INSERT is: a key already stored, or one the statement
//! names twice, is a unique violation (23505) and the statement writes
//! nothing. `ON CONFLICT DO NOTHING` skips those rows and counts the rest.

use crate::harness::TestServer;

async fn setup(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, v TEXT)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {name} (id, v) VALUES ('a', 'stored')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {name}: {e}"));
}

async fn rows(server: &TestServer, name: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!("SELECT id, v FROM {name} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {name}: {e}"))
}

fn expect(pairs: &[(&str, &str)]) -> Vec<Vec<String>> {
    pairs
        .iter()
        .map(|(id, v)| vec![(*id).to_string(), (*v).to_string()])
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stored_key_refuses_the_whole_statement() {
    let server = TestServer::start().await;
    setup(&server, "ipc_stored").await;
    server
        .expect_error(
            "INSERT INTO ipc_stored (id, v) VALUES ('b', 'new'), ('a', 'overwrite')",
            "duplicate key",
        )
        .await;
    assert_eq!(
        rows(&server, "ipc_stored").await,
        expect(&[("a", "stored")]),
        "the stored row stays and no row of the statement lands"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_named_twice_refuses_the_statement() {
    let server = TestServer::start().await;
    setup(&server, "ipc_twice").await;
    server
        .expect_error(
            "INSERT INTO ipc_twice (id, v) VALUES ('c', 'one'), ('c', 'two')",
            "duplicate key",
        )
        .await;
    assert_eq!(rows(&server, "ipc_twice").await, expect(&[("a", "stored")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_do_nothing_skips_and_counts_the_rest() {
    let server = TestServer::start().await;
    setup(&server, "ipc_skip").await;
    let inserted = server
        .client
        .execute(
            "INSERT INTO ipc_skip (id, v) VALUES ('a', 'skipped'), ('b', 'new'), \
             ('b', 'repeat') ON CONFLICT DO NOTHING",
            &[],
        )
        .await
        .expect("insert with skip");
    assert_eq!(inserted, 1, "only the new key lands");
    assert_eq!(
        rows(&server, "ipc_skip").await,
        expect(&[("a", "stored"), ("b", "new")])
    );
}
