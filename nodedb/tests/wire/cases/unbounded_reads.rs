// SPDX-License-Identifier: BUSL-1.1

//! A read with no LIMIT returns every row, past 10,000.
//!
//! Aggregates, join aggregates, recursive CTEs and spatial predicates once
//! stopped at a default row count and returned that prefix with nothing
//! saying so. Each case below reads more rows than the old default. SQL
//! `array_agg` names the array engine's reducer, so the generic
//! materializing accumulators are covered by the Data Plane's own tests.

use crate::harness::TestServer;

const ROWS: usize = 10_500;
const CHUNK: usize = 500;

/// Insert `ROWS` rows built by `row(i)` into `collection`, `CHUNK` per
/// statement.
async fn seed(server: &TestServer, collection: &str, columns: &str, row: impl Fn(usize) -> String) {
    for start in (0..ROWS).step_by(CHUNK) {
        let values: Vec<String> = (start..(start + CHUNK).min(ROWS)).map(&row).collect();
        server
            .exec(&format!(
                "INSERT INTO {collection} ({columns}) VALUES {}",
                values.join(", ")
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_returns_every_group() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ub_groups (id TEXT PRIMARY KEY, g TEXT)")
        .await
        .expect("create collection");
    seed(&server, "ub_groups", "id, g", |i| {
        format!("('r{i}', 'g{i}')")
    })
    .await;
    let groups = server
        .query_text("SELECT g, COUNT(*) FROM ub_groups GROUP BY g")
        .await
        .expect("group by");
    assert_eq!(groups.len(), ROWS, "every group must come back");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_group_by_returns_every_group() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ub_left (id TEXT PRIMARY KEY, g TEXT)")
        .await
        .expect("create left");
    server
        .exec("CREATE COLLECTION ub_right (id TEXT PRIMARY KEY, left_id TEXT)")
        .await
        .expect("create right");
    seed(&server, "ub_left", "id, g", |i| format!("('l{i}', 'g{i}')")).await;
    seed(&server, "ub_right", "id, left_id", |i| {
        format!("('r{i}', 'l{i}')")
    })
    .await;
    let groups = server
        .query_text(
            "SELECT a.g, COUNT(*) FROM ub_left a JOIN ub_right b ON a.id = b.left_id GROUP BY a.g",
        )
        .await
        .expect("join group by");
    assert_eq!(groups.len(), ROWS, "every joined group must come back");
}

/// One root with `ROWS` children: one recursion step returns them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_returns_every_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ub_tree (id TEXT PRIMARY KEY, parent TEXT)")
        .await
        .expect("create tree");
    server
        .exec("INSERT INTO ub_tree (id, parent) VALUES ('root', NULL)")
        .await
        .expect("seed root");
    seed(&server, "ub_tree", "id, parent", |i| {
        format!("('c{i}', 'root')")
    })
    .await;
    let rows = server
        .query_text(
            "WITH RECURSIVE t AS (\
                 SELECT * FROM ub_tree WHERE id = 'root' \
                 UNION ALL \
                 SELECT c.* FROM ub_tree c JOIN t ON c.parent = t.id\
             ) SELECT id FROM t",
        )
        .await
        .expect("recursive cte");
    // Name any id the recursion returns more than once, so a failure shows
    // which row repeats rather than only the count.
    let mut counts: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for id in &rows {
        *counts.entry(id.as_str()).or_default() += 1;
    }
    let repeated: Vec<(&str, usize)> = counts
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(id, n)| (*id, *n))
        .collect();
    assert!(
        repeated.is_empty(),
        "ids returned more than once: {repeated:?}"
    );
    assert_eq!(
        rows.len(),
        ROWS + 1,
        "the root and every child must come back; distinct ids: {}",
        counts.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_predicate_returns_every_match() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ub_places (id TEXT, location GEOMETRY SPATIAL_INDEX, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .expect("create spatial collection");
    seed(&server, "ub_places", "id, location, name", |i| {
        format!("('p{i}', ST_Point(-73.9857, 40.7580), 'n{i}')")
    })
    .await;
    let rows = server
        .query_text(
            "SELECT id FROM ub_places WHERE ST_DWithin(location, \
             '{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}', 5000)",
        )
        .await
        .expect("spatial predicate");
    assert_eq!(rows.len(), ROWS, "every matching point must come back");
}
