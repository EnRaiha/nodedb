// SPDX-License-Identifier: BUSL-1.1

//! A `WITH` CTE used as a JOIN side. The CTE name, or its alias, qualifies
//! the side's columns. It works on either side, with qualified and
//! unqualified columns, under an aggregate, and over the same collection the
//! other side reads.

use crate::harness::TestServer;

/// Customers in three regions; `c4` has no orders. Per-customer order
/// totals: c1 30, c2 30, c3 90.
async fn seed(server: &TestServer, p: &str) {
    for sql in [
        format!("CREATE COLLECTION {p}_customers TYPE DOCUMENT SCHEMALESS"),
        format!("CREATE COLLECTION {p}_orders TYPE DOCUMENT SCHEMALESS"),
        format!(
            "INSERT INTO {p}_customers (id, region) VALUES \
             ('c1', 'east'), ('c2', 'east'), ('c3', 'west'), ('c4', 'north')"
        ),
        format!(
            "INSERT INTO {p}_orders (id, cust, amount) VALUES \
             ('o1', 'c1', 10), ('o2', 'c1', 20), ('o3', 'c2', 30), \
             ('o4', 'c3', 40), ('o5', 'c3', 50)"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// Every row's cells joined by `|`, sorted.
async fn sorted(server: &TestServer, sql: &str) -> Vec<String> {
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

/// The CTE every test joins: customers outside the north region.
fn with_c(p: &str) -> String {
    format!("WITH c AS (SELECT id, region FROM {p}_customers WHERE region <> 'north')")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cte_on_the_right_reads_its_rows() {
    let server = TestServer::start().await;
    seed(&server, "jc_right").await;
    let w = with_c("jc_right");
    let expected = vec!["o1|east", "o2|east", "o3|east", "o4|west", "o5|west"];
    assert_eq!(
        sorted(
            &server,
            &format!("{w} SELECT o.id, c.region FROM jc_right_orders o JOIN c ON o.cust = c.id")
        )
        .await,
        expected
    );
    // `region` is declared by the CTE only, `amount` by the orders only.
    assert_eq!(
        sorted(
            &server,
            &format!("{w} SELECT amount, region FROM jc_right_orders o JOIN c ON o.cust = c.id")
        )
        .await,
        vec!["10|east", "20|east", "30|east", "40|west", "50|west"]
    );
    // An alias on the CTE reference qualifies the side in its place.
    assert_eq!(
        sorted(
            &server,
            &format!(
                "{w} SELECT o.id, k.region FROM jc_right_orders o JOIN c AS k ON o.cust = k.id"
            )
        )
        .await,
        expected
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cte_on_the_left_reads_its_rows() {
    let server = TestServer::start().await;
    seed(&server, "jc_left").await;
    let w = with_c("jc_left");
    assert_eq!(
        sorted(
            &server,
            &format!("{w} SELECT c.id, o.amount FROM c JOIN jc_left_orders o ON c.id = o.cust")
        )
        .await,
        vec!["c1|10", "c1|20", "c2|30", "c3|40", "c3|50"]
    );
    assert_eq!(
        sorted(
            &server,
            &format!("{w} SELECT region, amount FROM c JOIN jc_left_orders o ON c.id = o.cust")
        )
        .await,
        vec!["east|10", "east|20", "east|30", "west|40", "west|50"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_left_join_keeps_rows_the_cte_lacks() {
    let server = TestServer::start().await;
    seed(&server, "jc_outer").await;
    let w = with_c("jc_outer");
    assert_eq!(
        sorted(
            &server,
            &format!(
                "{w} SELECT k.id, c.region FROM jc_outer_customers k \
                 LEFT JOIN c ON k.id = c.id"
            )
        )
        .await,
        vec!["c1|east", "c2|east", "c3|west", "c4|"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_over_a_cte_join() {
    let server = TestServer::start().await;
    seed(&server, "jc_agg").await;
    let w = with_c("jc_agg");
    assert_eq!(
        sorted(
            &server,
            &format!(
                "{w} SELECT c.region, SUM(o.amount) FROM jc_agg_orders o \
                 JOIN c ON o.cust = c.id GROUP BY c.region"
            )
        )
        .await,
        vec!["east|60", "west|90"]
    );
    assert_eq!(
        sorted(
            &server,
            &format!(
                "{w} SELECT region, COUNT(*) FROM jc_agg_orders o \
                 JOIN c ON o.cust = c.id GROUP BY region"
            )
        )
        .await,
        vec!["east|3", "west|2"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cte_over_the_other_sides_collection() {
    let server = TestServer::start().await;
    seed(&server, "jc_self").await;
    assert_eq!(
        sorted(
            &server,
            "WITH big AS (SELECT id, amount FROM jc_self_orders WHERE amount >= 30) \
             SELECT o.id, big.amount FROM jc_self_orders o JOIN big ON o.id = big.id"
        )
        .await,
        vec!["o3|30", "o4|40", "o5|50"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cte_side_over_the_extended_protocol() {
    let server = TestServer::start().await;
    seed(&server, "jc_ext").await;
    let sql = format!(
        "{} SELECT o.id, c.region FROM jc_ext_orders o JOIN c ON o.cust = c.id",
        with_c("jc_ext")
    );
    let rows = server
        .client
        .query(sql.as_str(), &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut cells: Vec<String> = rows
        .iter()
        .map(|row| {
            let id: &str = row.get("id");
            let region: Option<&str> = row.get("region");
            format!("{id}|{}", region.unwrap_or("NULL"))
        })
        .collect();
    cells.sort();
    assert_eq!(
        cells,
        vec!["o1|east", "o2|east", "o3|east", "o4|west", "o5|west"]
    );
}
