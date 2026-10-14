// SPDX-License-Identifier: BUSL-1.1

//! The source of `INSERT ... SELECT` has exactly SELECT semantics.
//!
//! WHERE, ORDER BY with LIMIT, OFFSET, DISTINCT, `AS OF SYSTEM TIME`, and the
//! source's read policy all hold for the copy. No LIMIT copies every row: a
//! source larger than 10,000 rows copies in full, never a silent prefix. A
//! copy into its own source reads the whole source before it writes.

use crate::harness::TestServer;

/// Rows in the large-source tests: past the 10,000-row default that once
/// truncated an unlimited source read.
const LARGE_ROWS: usize = 10_500;

/// Rows per seeding INSERT.
const SEED_CHUNK: usize = 500;

async fn count(server: &TestServer, collection: &str) -> usize {
    let rows = server
        .query_text(&format!("SELECT COUNT(*) FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("count {collection}: {e}"));
    rows.first()
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("count {collection} returned {rows:?}"))
}

async fn seed_scores(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, g, score) VALUES \
             ('a', 'x', 5), ('b', 'x', 3), ('c', 'y', 9), ('d', 'x', 1), ('e', 'y', 7)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
}

async fn ids(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT id FROM {collection} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_source_past_ten_thousand_rows_copies_in_full() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION isr_big")
        .await
        .expect("create source");
    for start in (0..LARGE_ROWS).step_by(SEED_CHUNK) {
        let values: Vec<String> = (start..(start + SEED_CHUNK).min(LARGE_ROWS))
            .map(|i| format!("('r{i:05}', {i})"))
            .collect();
        server
            .exec(&format!(
                "INSERT INTO isr_big (id, n) VALUES {}",
                values.join(", ")
            ))
            .await
            .expect("seed source chunk");
    }
    server
        .exec("CREATE COLLECTION isr_big_t")
        .await
        .expect("create target");
    server
        .exec("INSERT INTO isr_big_t SELECT * FROM isr_big")
        .await
        .expect("copy every row");
    assert_eq!(count(&server, "isr_big_t").await, LARGE_ROWS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_source_past_ten_thousand_rows_copies_in_full() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION isr_ts (ts TIMESTAMP TIME_KEY, v FLOAT) WITH (engine='timeseries')",
        )
        .await
        .expect("create timeseries source");
    for start in (0..LARGE_ROWS).step_by(SEED_CHUNK) {
        let values: Vec<String> = (start..(start + SEED_CHUNK).min(LARGE_ROWS))
            .map(|i| format!("({}, {i}.5)", 1_000 + i))
            .collect();
        server
            .exec(&format!(
                "INSERT INTO isr_ts (ts, v) VALUES {}",
                values.join(", ")
            ))
            .await
            .expect("seed timeseries chunk");
    }
    server
        .exec("CREATE COLLECTION isr_ts_t")
        .await
        .expect("create target");
    server
        .exec("INSERT INTO isr_ts_t (v) SELECT v FROM isr_ts")
        .await
        .expect("copy every timeseries row");
    assert_eq!(count(&server, "isr_ts_t").await, LARGE_ROWS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn where_order_by_and_limit_pick_the_top_rows() {
    let server = TestServer::start().await;
    seed_scores(&server, "isr_top").await;
    server
        .exec("CREATE COLLECTION isr_top_t")
        .await
        .expect("create target");
    server
        .exec(
            "INSERT INTO isr_top_t (id, score) \
             SELECT id, score FROM isr_top WHERE g = 'x' ORDER BY score DESC LIMIT 2",
        )
        .await
        .expect("copy top rows");
    assert_eq!(ids(&server, "isr_top_t").await, vec!["a", "b"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offset_skips_matching_rows() {
    let server = TestServer::start().await;
    seed_scores(&server, "isr_off").await;
    server
        .exec("CREATE COLLECTION isr_off_t")
        .await
        .expect("create target");
    server
        .exec(
            "INSERT INTO isr_off_t (id, score) \
             SELECT id, score FROM isr_off ORDER BY score LIMIT 2 OFFSET 1",
        )
        .await
        .expect("copy with offset");
    // Ascending scores: d=1, b=3, a=5, e=7, c=9. Skip one, take two.
    assert_eq!(ids(&server, "isr_off_t").await, vec!["a", "b"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_copies_each_value_once() {
    let server = TestServer::start().await;
    seed_scores(&server, "isr_dist").await;
    server
        .exec("CREATE COLLECTION isr_dist_t")
        .await
        .expect("create target");
    server
        .exec("INSERT INTO isr_dist_t (g) SELECT DISTINCT g FROM isr_dist")
        .await
        .expect("copy distinct groups");
    let groups = server
        .query_text("SELECT g FROM isr_dist_t ORDER BY g")
        .await
        .expect("read target");
    assert_eq!(groups, vec!["x".to_string(), "y".to_string()]);
}

/// `AS OF SYSTEM TIME` copies the rows as they stood at the cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn as_of_system_time_copies_the_rows_at_the_cut() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION isr_bt (id STRING PRIMARY KEY, n INT) \
             WITH (engine='document_strict', bitemporal=true)",
        )
        .await
        .expect("create bitemporal source");
    server
        .exec("INSERT INTO isr_bt (id, n) VALUES ('a', 1)")
        .await
        .expect("seed version one");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let cut = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    server
        .exec("UPDATE isr_bt SET n = 2 WHERE id = 'a'")
        .await
        .expect("write version two");
    server
        .exec("INSERT INTO isr_bt (id, n) VALUES ('b', 3)")
        .await
        .expect("insert after the cut");

    server
        .exec("CREATE COLLECTION isr_bt_t")
        .await
        .expect("create target");
    server
        .exec(&format!(
            "INSERT INTO isr_bt_t (id, n) SELECT id, n FROM isr_bt AS OF SYSTEM TIME {cut}"
        ))
        .await
        .expect("copy at the cut");
    let copied = server
        .query_rows("SELECT id, n FROM isr_bt_t ORDER BY id")
        .await
        .expect("read target");
    assert_eq!(copied, vec![vec!["a".to_string(), "1".to_string()]]);
}

/// A copy into its own source copies each existing row once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_into_its_own_source_reads_each_row_once() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION isr_self")
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO isr_self (n) VALUES (1), (2), (3)")
        .await
        .expect("seed collection");
    server
        .exec("INSERT INTO isr_self (n) SELECT n FROM isr_self")
        .await
        .expect("copy into itself");
    assert_eq!(count(&server, "isr_self").await, 6);
}
