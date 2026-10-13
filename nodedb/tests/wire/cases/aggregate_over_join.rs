// SPDX-License-Identifier: BUSL-1.1

//! Aggregates over a join: GROUP BY, a scalar aggregate, WHERE, HAVING,
//! FILTER, and ROLLUP all fold the joined rows.

use crate::harness::TestServer;

/// Customers in two regions and five orders. `east` orders total 60,
/// `west` orders total 90.
async fn seed(server: &TestServer, prefix: &str) {
    for c in ["customers", "orders"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {prefix}_{c} TYPE DOCUMENT SCHEMALESS"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {prefix}_{c}: {e}"));
    }
    server
        .exec(&format!(
            "INSERT INTO {prefix}_customers (id, region) VALUES \
             ('c1', 'east'), ('c2', 'east'), ('c3', 'west')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed customers: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {prefix}_orders (id, cust, amount) VALUES \
             ('o1', 'c1', 10), ('o2', 'c1', 20), ('o3', 'c2', 30), \
             ('o4', 'c3', 40), ('o5', 'c3', 50)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed orders: {e}"));
}

/// Every row's cells joined by `|`, sorted.
async fn sorted_rows(server: &TestServer, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    rows.sort();
    rows
}

fn from(prefix: &str) -> String {
    format!("FROM {prefix}_orders o JOIN {prefix}_customers c ON o.cust = c.id")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_over_a_join() {
    let server = TestServer::start().await;
    seed(&server, "jagg_group").await;
    let rows = sorted_rows(
        &server,
        &format!(
            "SELECT c.region, COUNT(*), SUM(o.amount) {} GROUP BY c.region",
            from("jagg_group")
        ),
    )
    .await;
    assert_eq!(rows, vec!["east|3|60", "west|2|90"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_aggregate_over_a_join_with_where() {
    let server = TestServer::start().await;
    seed(&server, "jagg_scalar").await;
    let rows = sorted_rows(
        &server,
        &format!(
            "SELECT COUNT(*), SUM(o.amount) {} WHERE o.amount > 15",
            from("jagg_scalar")
        ),
    )
    .await;
    assert_eq!(
        rows,
        vec!["4|140"],
        "WHERE drops the 10 order before folding"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn having_over_a_join() {
    let server = TestServer::start().await;
    seed(&server, "jagg_having").await;
    let rows = sorted_rows(
        &server,
        &format!(
            "SELECT c.region, SUM(o.amount) AS total {} GROUP BY c.region \
             HAVING SUM(o.amount) > 70",
            from("jagg_having")
        ),
    )
    .await;
    assert_eq!(rows, vec!["west|90"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_over_a_join() {
    let server = TestServer::start().await;
    seed(&server, "jagg_filter").await;
    let rows = sorted_rows(
        &server,
        &format!(
            "SELECT c.region, COUNT(*), COUNT(*) FILTER (WHERE o.amount >= 30) {} \
             GROUP BY c.region",
            from("jagg_filter")
        ),
    )
    .await;
    assert_eq!(rows, vec!["east|3|1", "west|2|2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollup_over_a_join() {
    let server = TestServer::start().await;
    seed(&server, "jagg_rollup").await;
    let rows = sorted_rows(
        &server,
        &format!(
            "SELECT c.region, SUM(o.amount) {} GROUP BY ROLLUP (c.region)",
            from("jagg_rollup")
        ),
    )
    .await;
    assert_eq!(rows, vec!["east|60", "west|90", "|150"]);
}
