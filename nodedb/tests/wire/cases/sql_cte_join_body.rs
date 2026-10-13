// SPDX-License-Identifier: BUSL-1.1

//! A CTE or derived table whose body is a join. The body's rows are keyed by
//! its declared output names (`amount`, `name`), not by the join's qualified
//! keys (`o.amount`). The outer query reads them qualified or unqualified,
//! as a JOIN side, and under an aggregate, over the simple and the extended
//! protocol.

use crate::harness::TestServer;

/// Customers `c1 ann`, `c2 bob`, `c3 cid`. Orders: c1 10 and 20, c2 30,
/// c3 40.
async fn seed(server: &TestServer, p: &str) {
    for sql in [
        format!("CREATE COLLECTION {p}_customers TYPE DOCUMENT SCHEMALESS"),
        format!("CREATE COLLECTION {p}_orders TYPE DOCUMENT SCHEMALESS"),
        format!(
            "INSERT INTO {p}_customers (id, name) VALUES \
             ('c1', 'ann'), ('c2', 'bob'), ('c3', 'cid')"
        ),
        format!(
            "INSERT INTO {p}_orders (id, cust, amount) VALUES \
             ('o1', 'c1', 10), ('o2', 'c1', 20), ('o3', 'c2', 30), ('o4', 'c3', 40)"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// The join body every test reads.
fn body(p: &str) -> String {
    format!(
        "(SELECT o.amount, u.name FROM {p}_orders o \
         JOIN {p}_customers u ON o.cust = u.id)"
    )
}

/// One cell as text, decoded by its RowDescription type. NULL is `NULL`.
fn cell(row: &tokio_postgres::Row, index: usize) -> String {
    let oid = row.columns()[index].type_().oid();
    match oid {
        25 | 1043 => row.get::<_, Option<&str>>(index).map(str::to_string),
        20 => row.get::<_, Option<i64>>(index).map(|v| v.to_string()),
        23 => row.get::<_, Option<i32>>(index).map(|v| v.to_string()),
        701 => row.get::<_, Option<f64>>(index).map(|v| v.to_string()),
        16 => row.get::<_, Option<bool>>(index).map(|v| v.to_string()),
        other => panic!("column {index}: unexpected OID {other}"),
    }
    .unwrap_or_else(|| "NULL".to_string())
}

/// Every row of `sql` over the simple and the extended protocol, cells
/// joined by `|`, sorted. Both protocols must return `expected`.
async fn assert_rows(server: &TestServer, sql: &str, expected: &[&str]) {
    let mut simple: Vec<String> = server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    simple.sort();
    assert_eq!(simple, expected, "simple protocol: {sql}");

    let mut extended: Vec<String> = server
        .client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| cell(row, i))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    extended.sort();
    assert_eq!(extended, expected, "extended protocol: {sql}");
}

const ROWS: [&str; 4] = ["10|ann", "20|ann", "30|bob", "40|cid"];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cte_over_a_join_reads_its_output_names() {
    let server = TestServer::start().await;
    seed(&server, "cj_ref").await;
    let w = format!("WITH c AS {}", body("cj_ref"));
    assert_rows(&server, &format!("{w} SELECT amount, name FROM c"), &ROWS).await;
    assert_rows(
        &server,
        &format!("{w} SELECT c.amount, c.name FROM c"),
        &ROWS,
    )
    .await;
    assert_rows(&server, &format!("{w} SELECT * FROM c"), &ROWS).await;
    assert_rows(
        &server,
        &format!("{w} SELECT amount, name FROM c WHERE amount > 15"),
        &["20|ann", "30|bob", "40|cid"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_derived_table_over_a_join_reads_its_output_names() {
    let server = TestServer::start().await;
    seed(&server, "cj_from").await;
    let b = body("cj_from");
    assert_rows(
        &server,
        &format!("SELECT amount, name FROM {b} AS c"),
        &ROWS,
    )
    .await;
    assert_rows(
        &server,
        &format!("SELECT c.amount, c.name FROM {b} AS c"),
        &ROWS,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cte_over_a_join_as_a_join_side() {
    let server = TestServer::start().await;
    seed(&server, "cj_side").await;
    let w = format!("WITH c AS {}", body("cj_side"));
    let expected = ["c1|10", "c1|20", "c2|30", "c3|40"];
    assert_rows(
        &server,
        &format!("{w} SELECT k.id, c.amount FROM cj_side_customers k JOIN c ON k.name = c.name"),
        &expected,
    )
    .await;
    // `amount` is declared by the CTE only.
    assert_rows(
        &server,
        &format!("{w} SELECT k.id, amount FROM cj_side_customers k JOIN c ON k.name = c.name"),
        &expected,
    )
    .await;
    // The CTE on the left.
    assert_rows(
        &server,
        &format!("{w} SELECT c.amount, k.id FROM c JOIN cj_side_customers k ON c.name = k.name"),
        &["10|c1", "20|c1", "30|c2", "40|c3"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_over_a_cte_over_a_join() {
    let server = TestServer::start().await;
    seed(&server, "cj_agg").await;
    let w = format!("WITH c AS {}", body("cj_agg"));
    let counts = ["ann|2", "bob|1", "cid|1"];
    assert_rows(
        &server,
        &format!("{w} SELECT name, COUNT(*) FROM c GROUP BY name"),
        &counts,
    )
    .await;
    assert_rows(
        &server,
        &format!("{w} SELECT c.name, COUNT(c.amount) FROM c GROUP BY c.name"),
        &counts,
    )
    .await;
    let mut sums: Vec<String> = server
        .query_rows(&format!(
            "{w} SELECT name, SUM(amount) FROM c GROUP BY name"
        ))
        .await
        .unwrap_or_else(|e| panic!("SUM over the CTE: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    sums.sort();
    assert_eq!(sums, ["ann|30", "bob|30", "cid|40"]);
}
