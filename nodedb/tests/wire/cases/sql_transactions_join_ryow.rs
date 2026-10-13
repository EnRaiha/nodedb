// SPDX-License-Identifier: BUSL-1.1

//! A join inside a transaction reads the transaction's staged rows on the
//! left side, the right side, and both, for every join shape SQL plans: an
//! equi-join, a non-equi join, and a cross join. A row join and an
//! aggregate over the join agree.

use crate::harness::TestServer;

/// Which sides take a staged row.
#[derive(Clone, Copy, Debug)]
enum Staged {
    Left,
    Right,
    Both,
}

/// The join shapes SQL plans, as `FROM` clauses over `l` and `r`.
fn shapes(prefix: &str) -> [(&'static str, String); 3] {
    [
        (
            "equi",
            format!("FROM {prefix}_l l JOIN {prefix}_r r ON l.k = r.k"),
        ),
        (
            "non-equi",
            format!("FROM {prefix}_l l JOIN {prefix}_r r ON l.k <= r.k"),
        ),
        (
            "cross",
            format!("FROM {prefix}_l l CROSS JOIN {prefix}_r r"),
        ),
    ]
}

async fn setup(server: &TestServer, prefix: &str) {
    for side in ["l", "r"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {prefix}_{side} TYPE DOCUMENT SCHEMALESS"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {prefix}_{side}: {e}"));
        server
            .exec(&format!(
                "INSERT INTO {prefix}_{side} (id, k) VALUES ('{side}1', 1)"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {prefix}_{side}: {e}"));
    }
}

async fn join_sees_staged_rows(staged: Staged) {
    let server = TestServer::start().await;
    let prefix = match staged {
        Staged::Left => "jryow_left",
        Staged::Right => "jryow_right",
        Staged::Both => "jryow_both",
    };
    setup(&server, prefix).await;

    server.exec("BEGIN").await.unwrap();
    let sides: &[&str] = match staged {
        Staged::Left => &["l"],
        Staged::Right => &["r"],
        Staged::Both => &["l", "r"],
    };
    for side in sides {
        server
            .exec(&format!(
                "INSERT INTO {prefix}_{side} (id, k) VALUES ('{side}2', 1)"
            ))
            .await
            .unwrap_or_else(|e| panic!("{staged:?}: staged insert into {side}: {e}"));
    }
    // Every row has k = 1, so every shape pairs every left row with every
    // right row.
    let want: usize = match staged {
        Staged::Left | Staged::Right => 2,
        Staged::Both => 4,
    };

    for (shape, from) in shapes(prefix) {
        let rows = server
            .query_rows(&format!("SELECT l.id, r.id {from}"))
            .await
            .unwrap_or_else(|e| panic!("{staged:?} {shape}: row join: {e}"));
        assert_eq!(
            rows.len(),
            want,
            "{staged:?} {shape}: the row join pairs the staged rows: {rows:?}"
        );
        let count = server
            .query_rows(&format!("SELECT COUNT(*) {from}"))
            .await
            .unwrap_or_else(|e| panic!("{staged:?} {shape}: join aggregate: {e}"));
        assert_eq!(
            count[0][0],
            want.to_string(),
            "{staged:?} {shape}: the join aggregate counts the staged rows"
        );
    }

    server.client.simple_query("ROLLBACK").await.unwrap();
    for (shape, from) in shapes(prefix) {
        let count = server
            .query_rows(&format!("SELECT COUNT(*) {from}"))
            .await
            .unwrap_or_else(|e| panic!("{staged:?} {shape}: after ROLLBACK: {e}"));
        assert_eq!(count[0][0], "1", "{staged:?} {shape}: ROLLBACK discards");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_reads_staged_left_rows() {
    join_sees_staged_rows(Staged::Left).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_reads_staged_right_rows() {
    join_sees_staged_rows(Staged::Right).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_reads_staged_rows_on_both_sides() {
    join_sees_staged_rows(Staged::Both).await;
}
