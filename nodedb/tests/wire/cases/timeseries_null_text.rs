// SPDX-License-Identifier: BUSL-1.1

//! A NULL or omitted text value in a timeseries collection is SQL NULL,
//! never the empty string.
//!
//! `''` is a value of its own. `WHERE tag IS NULL` finds the NULL rows only,
//! `WHERE tag = ''` the empty-string row only, and `GROUP BY tag` shows a
//! NULL group apart from the `''` group. This holds for SQL `INSERT` and for
//! ILP, in the live memtable, after a flush, and after a restart.

use std::time::{Duration, Instant};

use nodedb_test_support::ilp_client;

use crate::harness::TestServer;

/// One row per shape: a value, `''`, an explicit NULL, and an omitted tag.
async fn seed_sql(server: &TestServer, c: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {c} (ts BIGINT TIME_KEY, tag TEXT, v FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    for sql in [
        format!("INSERT INTO {c} (ts, tag, v) VALUES (1000, 'a', 1)"),
        format!("INSERT INTO {c} (ts, tag, v) VALUES (2000, '', 2)"),
        format!("INSERT INTO {c} (ts, tag, v) VALUES (3000, NULL, 3)"),
        format!("INSERT INTO {c} (ts, v) VALUES (4000, 4)"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// Every row of `sql`, NULL cells as `None`.
async fn rows(server: &TestServer, sql: &str) -> Vec<Vec<Option<String>>> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn some(text: &str) -> Option<String> {
    Some(text.to_string())
}

/// The NULL rows are `null_ts`, the `''` row is `empty_ts`, and the value
/// row is `a_ts`.
async fn check(server: &TestServer, c: &str, a_ts: &str, empty_ts: &str, null_ts: &[&str]) {
    let mut nulls: Vec<Option<String>> =
        rows(server, &format!("SELECT ts FROM {c} WHERE tag IS NULL"))
            .await
            .into_iter()
            .map(|r| r[0].clone())
            .collect();
    nulls.sort();
    let want: Vec<Option<String>> = null_ts.iter().map(|t| some(t)).collect();
    assert_eq!(nulls, want, "{c}: IS NULL finds the NULL rows only");

    assert_eq!(
        rows(server, &format!("SELECT ts FROM {c} WHERE tag = ''")).await,
        vec![vec![some(empty_ts)]],
        "{c}: '' finds the empty-string row only"
    );
    assert_eq!(
        rows(server, &format!("SELECT ts FROM {c} WHERE tag <> ''")).await,
        vec![vec![some(a_ts)]],
        "{c}: a NULL row is not unequal to ''"
    );

    let null_count = null_ts.len().to_string();
    let want_groups = vec![
        vec![None, some(&null_count)],
        vec![some(""), some("1")],
        vec![some("a"), some("1")],
    ];
    for sql in [
        format!("SELECT tag, COUNT(*) FROM {c} GROUP BY tag"),
        format!("SELECT tag, COUNT(*) FILTER (WHERE v > 0) FROM {c} GROUP BY tag"),
    ] {
        let mut groups = rows(server, &sql).await;
        groups.sort();
        assert_eq!(
            groups, want_groups,
            "{c}: {sql}: NULL and '' are two groups"
        );
    }
}

/// SQL `INSERT` rows in the live memtable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_null_text_is_null_in_the_memtable() {
    let server = TestServer::start().await;
    seed_sql(&server, "null_text_live").await;
    check(&server, "null_text_live", "1000", "2000", &["3000", "4000"]).await;
}

/// SQL `INSERT` rows after every row flushed to a partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_null_text_is_null_after_a_flush() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed_sql(&server, "null_text_flushed").await;
    check(
        &server,
        "null_text_flushed",
        "1000",
        "2000",
        &["3000", "4000"],
    )
    .await;
}

/// SQL `INSERT` rows replayed from the WAL after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_null_text_is_null_after_a_restart() {
    let server = TestServer::start().await;
    seed_sql(&server, "null_text_replayed").await;
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;
    check(
        &server,
        "null_text_replayed",
        "1000",
        "2000",
        &["3000", "4000"],
    )
    .await;
}

/// SQL `INSERT` rows read from partitions after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_null_text_is_null_in_partitions_after_a_restart() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed_sql(&server, "null_text_reopened").await;
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path_with_timeseries_memtable_budget(dir, 1).await;
    check(
        &server,
        "null_text_reopened",
        "1000",
        "2000",
        &["3000", "4000"],
    )
    .await;
}

/// ILP lines: a tag value, an empty string field, and a line without the
/// tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ilp_absent_tag_is_null() {
    let server = TestServer::start().await;
    let c = "null_text_ilp";
    server
        .exec(&format!(
            "CREATE COLLECTION {c} (ts BIGINT TIME_KEY, tag TEXT, v FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), server.ilp_port);
    let mut ilp = ilp_client::connect_trust(addr, "nodedb").await;
    // ILP timestamps are nanoseconds. 1_000_000_000 ns stores as ts 1000.
    for line in [
        format!("{c},tag=a v=1 1000000000"),
        format!("{c} tag=\"\",v=2 2000000000"),
        format!("{c} v=3 3000000000"),
    ] {
        ilp_client::send_line(&mut ilp, &line).await;
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let count = rows(&server, &format!("SELECT COUNT(*) FROM {c}")).await;
        if count.first().and_then(|r| r[0].clone()).as_deref() == Some("3") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ILP rows never arrived: {count:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    drop(ilp);
    check(&server, c, "1000", "2000", &["3000"]).await;
}
