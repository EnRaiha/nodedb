// SPDX-License-Identifier: BUSL-1.1

//! Derived tables as JOIN sides: `JOIN (SELECT ...) AS t ON ...`.
//!
//! The derived side's rows come from its subquery, and its alias qualifies
//! its columns. It works on either side, for every join type, under an
//! aggregate, and with its own WHERE / ORDER BY / LIMIT. A derived table
//! that refers to another relation of its FROM clause needs LATERAL.

use crate::harness::TestServer;

/// Customers in three regions; `c4` has no orders. Order `o6` names a
/// customer that does not exist. Per-customer totals: c1 30, c2 30, c3 90,
/// c9 5.
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
             ('o4', 'c3', 40), ('o5', 'c3', 50), ('o6', 'c9', 5)"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// Per-customer order totals, as a derived table body.
fn totals(p: &str) -> String {
    format!("(SELECT cust, SUM(amount) AS total FROM {p}_orders GROUP BY cust)")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inner_join_reads_the_derived_rows() {
    let server = TestServer::start().await;
    seed(&server, "dj_inner").await;
    let t = totals("dj_inner");
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT c.id, t.total FROM dj_inner_customers c JOIN {t} t ON c.id = t.cust")
        )
        .await,
        vec!["c1|30", "c2|30", "c3|90"]
    );
    // `total` is declared by the derived relation only.
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT c.id, total FROM dj_inner_customers c JOIN {t} t ON c.id = t.cust")
        )
        .await,
        vec!["c1|30", "c2|30", "c3|90"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_joins_keep_unmatched_rows() {
    let server = TestServer::start().await;
    seed(&server, "dj_outer").await;
    let t = totals("dj_outer");
    let from = |kind: &str| format!("FROM dj_outer_customers c {kind} {t} t ON c.id = t.cust");
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT c.id, t.total {}", from("LEFT JOIN"))
        )
        .await,
        vec!["c1|30", "c2|30", "c3|90", "c4|"]
    );
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT c.id, t.cust {}", from("RIGHT JOIN"))
        )
        .await,
        vec!["c1|c1", "c2|c2", "c3|c3", "|c9"]
    );
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT c.id, t.cust {}", from("FULL JOIN"))
        )
        .await,
        vec!["c1|c1", "c2|c2", "c3|c3", "c4|", "|c9"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_join_pairs_every_derived_row() {
    let server = TestServer::start().await;
    seed(&server, "dj_cross").await;
    assert_eq!(
        sorted(
            &server,
            "SELECT COUNT(*) FROM dj_cross_customers c \
             CROSS JOIN (SELECT id FROM dj_cross_orders WHERE amount > 35) t"
        )
        .await,
        vec!["8"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_over_a_derived_join() {
    let server = TestServer::start().await;
    seed(&server, "dj_agg").await;
    let t = totals("dj_agg");
    assert_eq!(
        sorted(
            &server,
            &format!(
                "SELECT c.region, SUM(t.total) FROM dj_agg_customers c \
                 JOIN {t} t ON c.id = t.cust GROUP BY c.region"
            )
        )
        .await,
        vec!["east|60", "west|90"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn derived_body_applies_its_where_order_and_limit() {
    let server = TestServer::start().await;
    seed(&server, "dj_tail").await;
    assert_eq!(
        sorted(
            &server,
            "SELECT c.id, t.id FROM dj_tail_customers c JOIN \
             (SELECT id, cust FROM dj_tail_orders WHERE amount > 15 \
              ORDER BY amount DESC LIMIT 2) t ON c.id = t.cust"
        )
        .await,
        vec!["c3|o4", "c3|o5"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn derived_table_on_the_left() {
    let server = TestServer::start().await;
    seed(&server, "dj_left").await;
    assert_eq!(
        sorted(
            &server,
            "SELECT c.id, o.id FROM \
             (SELECT id, region FROM dj_left_customers WHERE region = 'east') c \
             JOIN dj_left_orders o ON o.cust = c.id"
        )
        .await,
        vec!["c1|o1", "c1|o2", "c2|o3"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn derived_side_in_a_three_way_join() {
    let server = TestServer::start().await;
    seed(&server, "dj_three").await;
    assert_eq!(
        sorted(
            &server,
            "SELECT o.id, t.region FROM dj_three_orders o \
             JOIN dj_three_customers c ON o.cust = c.id \
             JOIN (SELECT id, region FROM dj_three_customers WHERE region = 'west') t \
             ON t.id = c.id"
        )
        .await,
        vec!["o4|west", "o5|west"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn correlated_derived_table_needs_lateral() {
    let server = TestServer::start().await;
    seed(&server, "dj_corr").await;
    let sql = "SELECT c.id FROM dj_corr_customers c JOIN \
               (SELECT id FROM dj_corr_orders WHERE dj_corr_orders.cust = c.id) t \
               ON t.id = c.id";
    server.expect_error(sql, "42601").await;
    server.expect_error(sql, "LATERAL").await;
}
