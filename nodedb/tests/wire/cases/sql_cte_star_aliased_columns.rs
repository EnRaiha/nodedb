// SPDX-License-Identifier: BUSL-1.1

//! `SELECT *` over a CTE whose body renames a column (`n AS x`) returns the
//! value under the new name, over the simple and the extended protocol.

use crate::harness::TestServer;

/// One strict row: `id = 'r1'`, `n = 7`.
async fn seed(server: &TestServer, collection: &str) {
    for sql in [
        format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, n BIGINT) \
             WITH (engine='document_strict')"
        ),
        format!("INSERT INTO {collection} (id, n) VALUES ('r1', 7)"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// The `x` cell of the single row `sql` returns over the simple protocol.
async fn simple_x(server: &TestServer, sql: &str) -> Option<String> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let rows: Vec<_> = messages
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1, "{sql}: expected one row");
    rows[0]
        .try_get("x")
        .unwrap_or_else(|e| panic!("{sql}: no column x: {e}"))
        .map(str::to_string)
}

/// The `x` cell and its RowDescription OID over the extended protocol.
async fn extended_x(server: &TestServer, sql: &str) -> (Option<i64>, u32) {
    let rows = server
        .client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: expected one row");
    let oid = rows[0]
        .columns()
        .iter()
        .find(|c| c.name() == "x")
        .unwrap_or_else(|| panic!("{sql}: no column x"))
        .type_()
        .oid();
    (rows[0].get("x"), oid)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_star_over_a_renamed_cte_column_returns_its_value() {
    let server = TestServer::start().await;
    seed(&server, "cs_alias").await;
    for sql in [
        "WITH c AS (SELECT n AS x FROM cs_alias) SELECT * FROM c",
        "WITH c AS (SELECT id, n AS x FROM cs_alias) SELECT * FROM c",
        "WITH c AS (SELECT id, n AS x FROM cs_alias) SELECT * FROM c WHERE x > 1",
        "SELECT * FROM (SELECT id, n AS x FROM cs_alias) AS c",
    ] {
        assert_eq!(simple_x(&server, sql).await.as_deref(), Some("7"), "{sql}");
        assert_eq!(extended_x(&server, sql).await, (Some(7), 20), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_named_renamed_cte_column_returns_its_value() {
    let server = TestServer::start().await;
    seed(&server, "cs_named").await;
    let sql = "WITH c AS (SELECT id, n AS x FROM cs_named) SELECT x FROM c";
    assert_eq!(simple_x(&server, sql).await.as_deref(), Some("7"), "{sql}");
    assert_eq!(extended_x(&server, sql).await, (Some(7), 20), "{sql}");
}
