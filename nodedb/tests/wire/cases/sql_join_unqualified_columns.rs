// SPDX-License-Identifier: BUSL-1.1

//! Unqualified column names over a join.
//!
//! An unqualified column resolves to the one relation that has it, in every
//! clause: the SELECT list, WHERE, ON, GROUP BY, HAVING, ORDER BY, and
//! aggregate arguments. A declared column wins over a schemaless relation.
//! A name two relations may hold is refused with SQLSTATE 42702 and must be
//! qualified.

use crate::harness::TestServer;

/// `articles` is strict and declares `id`, `title` and `views`. `orders`
/// and `users` are schemaless, and each declares its `id` key. Order `o4`
/// has no `qty`.
async fn seed(server: &TestServer, p: &str) {
    for sql in [
        format!(
            "CREATE COLLECTION {p}_articles (id STRING PRIMARY KEY, title STRING, views INT) \
             WITH (engine='document_strict')"
        ),
        format!("CREATE COLLECTION {p}_orders TYPE DOCUMENT SCHEMALESS"),
        format!("CREATE COLLECTION {p}_users TYPE DOCUMENT SCHEMALESS"),
        format!(
            "INSERT INTO {p}_articles (id, title, views) VALUES ('a1', 'Rust', 10), ('a2', 'Go', 20)"
        ),
        format!(
            "INSERT INTO {p}_orders (id, article_id, user_id, amount, qty) VALUES \
             ('o1', 'a1', 'u1', 5, 1), ('o2', 'a1', 'u1', 15, 2), ('o3', 'a2', 'u2', 30, 3)"
        ),
        format!(
            "INSERT INTO {p}_orders (id, article_id, user_id, amount) VALUES ('o4', 'a2', 'u2', 40)"
        ),
        format!("INSERT INTO {p}_users (id, region) VALUES ('u1', 'eu'), ('u2', 'us')"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn articles_orders(p: &str) -> String {
    format!("FROM {p}_articles a JOIN {p}_orders o ON a.id = o.article_id")
}

/// Every row's cells joined by `|`, in result order.
async fn rows(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect()
}

/// Every row's cells joined by `|`, sorted.
async fn sorted(server: &TestServer, sql: &str) -> Vec<String> {
    let mut rows = rows(server, sql).await;
    rows.sort();
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projection_reads_each_side() {
    let server = TestServer::start().await;
    seed(&server, "uq_proj").await;
    let from = articles_orders("uq_proj");
    assert_eq!(
        sorted(&server, &format!("SELECT title, amount {from}")).await,
        vec!["Go|30", "Go|40", "Rust|15", "Rust|5"]
    );
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT views * 2 AS twice, amount + 1 AS more {from}")
        )
        .await,
        vec!["20|16", "20|6", "40|31", "40|41"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn where_reads_each_side() {
    let server = TestServer::start().await;
    seed(&server, "uq_where").await;
    let from = articles_orders("uq_where");
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT o.id {from} WHERE amount * 2 > 20")
        )
        .await,
        vec!["o2", "o3", "o4"]
    );
    assert_eq!(
        sorted(&server, &format!("SELECT o.id {from} WHERE views > 15")).await,
        vec!["o3", "o4"]
    );
    assert_eq!(
        sorted(&server, &format!("SELECT o.id {from} WHERE qty IS NULL")).await,
        vec!["o4"],
        "IS NULL finds the row without qty only"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_reads_each_side() {
    let server = TestServer::start().await;
    seed(&server, "uq_on").await;
    let p = "uq_on";
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT o.id FROM {p}_articles a JOIN {p}_orders o ON article_id = a.id")
        )
        .await,
        vec!["o1", "o2", "o3", "o4"],
        "a bare equi key orients to its side"
    );
    // Both sides declare `id`: the strict side in its schema, the
    // schemaless side as its key.
    server
        .expect_error(
            &format!("SELECT o.id FROM {p}_articles a JOIN {p}_orders o ON id = article_id"),
            "42702",
        )
        .await;
    assert_eq!(
        sorted(
            &server,
            &format!(
                "SELECT o.id FROM {p}_articles a JOIN {p}_orders o \
                 ON a.id = o.article_id AND views < amount"
            )
        )
        .await,
        vec!["o2", "o3", "o4"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_and_aggregates_read_each_side() {
    let server = TestServer::start().await;
    seed(&server, "uq_group").await;
    let from = articles_orders("uq_group");
    assert_eq!(
        sorted(
            &server,
            &format!(
                "SELECT title, SUM(amount), COUNT(*) FILTER (WHERE qty > 1) {from} GROUP BY title"
            )
        )
        .await,
        vec!["Go|70|1", "Rust|20|1"]
    );
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT title AS title, COUNT(*) {from} GROUP BY title")
        )
        .await,
        vec!["Go|2", "Rust|2"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn having_reads_group_columns_and_aggregates() {
    let server = TestServer::start().await;
    seed(&server, "uq_having").await;
    let from = articles_orders("uq_having");
    assert_eq!(
        rows(
            &server,
            &format!("SELECT title, SUM(amount) {from} GROUP BY title HAVING title <> 'Go'")
        )
        .await,
        vec!["Rust|20"]
    );
    assert_eq!(
        rows(
            &server,
            &format!("SELECT title, SUM(amount) {from} GROUP BY title HAVING SUM(amount) > 50")
        )
        .await,
        vec!["Go|70"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_reads_its_side() {
    let server = TestServer::start().await;
    seed(&server, "uq_order").await;
    let from = articles_orders("uq_order");
    assert_eq!(
        rows(
            &server,
            &format!("SELECT title, amount {from} ORDER BY amount")
        )
        .await,
        vec!["Rust|5", "Rust|15", "Go|30", "Go|40"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn name_two_schemaless_sides_may_hold_is_refused() {
    let server = TestServer::start().await;
    seed(&server, "uq_ambig").await;
    let from = "FROM uq_ambig_orders o JOIN uq_ambig_users u ON o.user_id = u.id";
    for sql in [
        format!("SELECT region {from}"),
        format!("SELECT o.id {from} WHERE region = 'eu'"),
        format!("SELECT COUNT(*) {from} GROUP BY region"),
        format!("SELECT o.id {from} ORDER BY region"),
        format!("SELECT MAX(region) {from}"),
        "SELECT o.id FROM uq_ambig_orders o JOIN uq_ambig_users u ON user_id = u.id".to_string(),
    ] {
        server.expect_error(&sql, "42702").await;
    }
    assert_eq!(
        sorted(
            &server,
            &format!("SELECT u.region, COUNT(*) {from} GROUP BY u.region")
        )
        .await,
        vec!["eu|2", "us|2"],
        "the qualified column reads its side"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn name_both_sides_declare_is_refused() {
    let server = TestServer::start().await;
    seed(&server, "uq_both").await;
    server
        .expect_error(
            "SELECT title FROM uq_both_articles a JOIN uq_both_articles b ON a.id = b.id",
            "42702",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_join_pairs_every_row() {
    let server = TestServer::start().await;
    seed(&server, "uq_cross").await;
    assert_eq!(
        rows(
            &server,
            "SELECT COUNT(*) FROM uq_cross_articles a CROSS JOIN uq_cross_users u"
        )
        .await,
        vec!["4"]
    );
    assert_eq!(
        sorted(
            &server,
            "SELECT a.title, u.region FROM uq_cross_articles a CROSS JOIN uq_cross_users u"
        )
        .await,
        vec!["Go|eu", "Go|us", "Rust|eu", "Rust|us"]
    );
}

/// Outside the default database a side with no alias is still qualified by
/// its bare collection name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unaliased_sides_read_outside_the_default_database() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE DATABASE uq_db",
        "USE DATABASE uq_db",
        "CREATE COLLECTION uq_l TYPE DOCUMENT SCHEMALESS",
        "CREATE COLLECTION uq_r TYPE DOCUMENT SCHEMALESS",
        "INSERT INTO uq_l (id, k, v) VALUES ('l1', 1, 'left')",
        "INSERT INTO uq_r (id, k, w) VALUES ('r1', 1, 'right')",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(
        rows(
            &server,
            "SELECT uq_l.v, uq_r.w FROM uq_l JOIN uq_r ON uq_l.k = uq_r.k"
        )
        .await,
        vec!["left|right"]
    );
    assert_eq!(
        rows(
            &server,
            "SELECT uq_r.w, COUNT(*) FROM uq_l JOIN uq_r ON uq_l.k = uq_r.k GROUP BY uq_r.w"
        )
        .await,
        vec!["right|1"]
    );
}
