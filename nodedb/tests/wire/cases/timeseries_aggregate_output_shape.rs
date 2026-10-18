// SPDX-License-Identifier: BUSL-1.1

//! The SELECT list of an aggregate over a timeseries collection decides its
//! result columns.
//!
//! Every aggregate shape reports its columns in SELECT-list order, under
//! their aliases, with the type OIDs a columnar collection reports for the
//! same query. Each shape runs over the simple protocol, the extended
//! protocol, and the native protocol.

use std::time::{Duration, SystemTime};

use nodedb_test_support::native_harness::{do_handshake, send_sql};
use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;

use crate::harness::TestServer;

const INT8: u32 = 20;
const TEXT: u32 = 25;
const FLOAT8: u32 = 701;
const TIMESTAMP: u32 = 1114;

/// `2026-10-01T00:00:00Z` in seconds since the Unix epoch.
const FIRST_SEEN_SECS: u64 = 1_790_812_800;

/// Three rows: two `TMR` rows in the first hour (`h_nt` mean 40001.0) and
/// one `KAK` row in the second hour.
async fn seed(server: &TestServer, c: &str, engine: &str) {
    let ddl = match engine {
        "timeseries" => format!(
            "CREATE COLLECTION {c} (ts TIMESTAMP TIME_KEY, station VARCHAR, h_nt FLOAT) \
             WITH (engine='timeseries', partition_by='1d')"
        ),
        _ => format!(
            "CREATE COLLECTION {c} (ts TIMESTAMP, station VARCHAR, h_nt FLOAT) \
             WITH (engine='{engine}')"
        ),
    };
    let insert = format!(
        "INSERT INTO {c} (ts, station, h_nt) VALUES \
         ('2026-10-01T00:00:00Z', 'TMR', 40000.5), \
         ('2026-10-01T00:01:00Z', 'TMR', 40001.5), \
         ('2026-10-01T01:00:00Z', 'KAK', 39990.0)"
    );
    for sql in [ddl, insert] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{engine}: {sql}: {e}"));
    }
}

/// One shape: the query over `{c}`, and the `(name, OID)` of each column.
struct Shape {
    sql: &'static str,
    columns: &'static [(&'static str, u32)],
}

/// Every aggregate shape both engines answer the same way.
const SHAPES: &[Shape] = &[
    Shape {
        sql: "SELECT AVG(h_nt) AS mean_h FROM {c}",
        columns: &[("mean_h", FLOAT8)],
    },
    Shape {
        sql: "SELECT AVG(h_nt) FROM {c}",
        columns: &[("avg(h_nt)", FLOAT8)],
    },
    Shape {
        sql: "SELECT station, AVG(h_nt) AS mean_h FROM {c} GROUP BY station",
        columns: &[("station", TEXT), ("mean_h", FLOAT8)],
    },
    Shape {
        sql: "SELECT AVG(h_nt) AS mean_h, station AS s FROM {c} GROUP BY station",
        columns: &[("mean_h", FLOAT8), ("s", TEXT)],
    },
    Shape {
        sql: "SELECT COUNT(*) AS n FROM {c} GROUP BY station",
        columns: &[("n", INT8)],
    },
    Shape {
        sql: "SELECT time_bucket('1 hour', ts) AS b, COUNT(*) AS n FROM {c} GROUP BY b",
        columns: &[("b", TIMESTAMP), ("n", INT8)],
    },
    Shape {
        sql: "SELECT time_bucket('1 hour', ts) AS b, station, AVG(h_nt) AS mean_h \
              FROM {c} GROUP BY b, station",
        columns: &[("b", TIMESTAMP), ("station", TEXT), ("mean_h", FLOAT8)],
    },
    Shape {
        sql: "SELECT AVG(h_nt) AS mean_h, station, time_bucket('1 hour', ts) AS b \
              FROM {c} GROUP BY b, station",
        columns: &[("mean_h", FLOAT8), ("station", TEXT), ("b", TIMESTAMP)],
    },
    Shape {
        sql: "SELECT station, AVG(h_nt) AS mean_h FROM {c} GROUP BY station \
              ORDER BY mean_h DESC",
        columns: &[("station", TEXT), ("mean_h", FLOAT8)],
    },
    Shape {
        sql: "SELECT time_bucket('1 hour', ts) AS b, COUNT(*) AS n FROM {c} GROUP BY b \
              ORDER BY b DESC",
        columns: &[("b", TIMESTAMP), ("n", INT8)],
    },
    Shape {
        sql: "SELECT station, AVG(h_nt) AS mean_h FROM {c} GROUP BY station \
              HAVING AVG(h_nt) > 0",
        columns: &[("station", TEXT), ("mean_h", FLOAT8)],
    },
    Shape {
        sql: "SELECT time_bucket('1 hour', ts) AS b, COUNT(*) AS n FROM {c} GROUP BY b \
              HAVING COUNT(*) > 0",
        columns: &[("b", TIMESTAMP), ("n", INT8)],
    },
    Shape {
        sql: "SELECT station, MIN(ts) AS first_seen, MAX(h_nt) AS peak FROM {c} \
              GROUP BY station",
        columns: &[
            ("station", TEXT),
            ("first_seen", TIMESTAMP),
            ("peak", FLOAT8),
        ],
    },
    Shape {
        sql: "SELECT station, approx_count_distinct(h_nt) AS d, \
              approx_percentile(h_nt, 0.5) AS p FROM {c} GROUP BY station",
        columns: &[("station", TEXT), ("d", INT8), ("p", FLOAT8)],
    },
];

/// The column names the simple protocol reports for `sql`.
async fn simple_columns(server: &TestServer, sql: &str) -> Vec<String> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("simple protocol: {sql}: {e}"));
    messages
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(row.columns().iter().map(|c| c.name().to_string()).collect())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("simple protocol: {sql}: no rows"))
}

/// The `(name, OID)` of each column the extended protocol reports for `sql`.
async fn extended_columns(server: &TestServer, sql: &str) -> Vec<(String, u32)> {
    let statement = server
        .client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("extended protocol: prepare {sql}: {e}"));
    let rows = server
        .client
        .query(&statement, &[])
        .await
        .unwrap_or_else(|e| panic!("extended protocol: {sql}: {e}"));
    assert!(!rows.is_empty(), "extended protocol: {sql}: no rows");
    statement
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), c.type_().oid()))
        .collect()
}

/// The column names the native protocol reports for `sql`.
async fn native_columns(server: &TestServer, sql: &str) -> Vec<String> {
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), server.native_port);
    let (mut stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let resp = send_sql(&mut stream, 1, sql).await;
    assert_eq!(
        resp.status,
        ResponseStatus::Ok,
        "native: {sql}: {:?}",
        resp.error
    );
    resp.columns
        .unwrap_or_else(|| panic!("native: {sql}: no columns"))
}

/// Checks every shape over `c` on all three protocols.
async fn check_every_shape(server: &TestServer, c: &str, engine: &str) {
    for shape in SHAPES {
        let sql = shape.sql.replace("{c}", c);
        let want: Vec<(String, u32)> = shape
            .columns
            .iter()
            .map(|(name, oid)| ((*name).to_string(), *oid))
            .collect();
        let names: Vec<String> = want.iter().map(|(name, _)| name.clone()).collect();
        assert_eq!(
            simple_columns(server, &sql).await,
            names,
            "{engine}: simple protocol: {sql}"
        );
        assert_eq!(
            extended_columns(server, &sql).await,
            want,
            "{engine}: extended protocol: {sql}"
        );
        assert_eq!(
            native_columns(server, &sql).await,
            names,
            "{engine}: native protocol: {sql}"
        );
    }
}

/// The typed cells decode as their announced types over the extended
/// protocol.
async fn check_typed_cells(server: &TestServer, c: &str, engine: &str) {
    let sql = format!(
        "SELECT AVG(h_nt) AS mean_h, station AS s, MIN(ts) AS first_seen FROM {c} \
         WHERE station = 'TMR' GROUP BY station"
    );
    let rows = server
        .client
        .query(sql.as_str(), &[])
        .await
        .unwrap_or_else(|e| panic!("{engine}: {sql}: {e}"));
    assert_eq!(rows.len(), 1, "{engine}: {sql}: one TMR group");
    let mean: f64 = rows[0].get("mean_h");
    let station: &str = rows[0].get("s");
    let first_seen: SystemTime = rows[0].get("first_seen");
    assert!(
        (mean - 40001.0).abs() < 1e-9,
        "{engine}: mean of 40000.5 and 40001.5: {mean}"
    );
    assert_eq!(station, "TMR", "{engine}: station under its alias");
    assert_eq!(
        first_seen,
        SystemTime::UNIX_EPOCH + Duration::from_secs(FIRST_SEEN_SECS),
        "{engine}: MIN(ts) is the first stored instant"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_aggregates_follow_the_select_list() {
    let server = TestServer::start().await;
    seed(&server, "ts_out_shape", "timeseries").await;
    check_every_shape(&server, "ts_out_shape", "timeseries").await;
    check_typed_cells(&server, "ts_out_shape", "timeseries").await;
}

/// The same shapes over flushed partitions answer the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_aggregates_over_flushed_partitions_follow_the_select_list() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "ts_out_flushed", "timeseries").await;
    check_every_shape(&server, "ts_out_flushed", "timeseries").await;
    check_typed_cells(&server, "ts_out_flushed", "timeseries").await;
}

/// The columnar engine answers every shape the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_aggregates_follow_the_select_list() {
    let server = TestServer::start().await;
    seed(&server, "col_out_shape", "columnar").await;
    check_every_shape(&server, "col_out_shape", "columnar").await;
    check_typed_cells(&server, "col_out_shape", "columnar").await;
}

/// An unaliased time bucket on the grouped timeseries scan reads `bucket`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unaliased_time_bucket_is_named_bucket() {
    let server = TestServer::start().await;
    seed(&server, "ts_out_bucket", "timeseries").await;
    let sql = "SELECT time_bucket('1 hour', ts), COUNT(*) FROM ts_out_bucket GROUP BY 1";
    assert_eq!(
        extended_columns(&server, sql).await,
        vec![
            ("bucket".to_string(), TIMESTAMP),
            ("count(*)".to_string(), INT8)
        ],
        "{sql}"
    );
}

/// A derived table over a grouped timeseries scan reads each aggregate
/// under its alias.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_derived_table_reads_the_aggregate_under_its_alias() {
    let server = TestServer::start().await;
    seed(&server, "ts_out_derived", "timeseries").await;
    for sql in [
        "SELECT * FROM (SELECT AVG(h_nt) AS mean_h FROM ts_out_derived) AS t",
        "SELECT mean_h FROM (SELECT AVG(h_nt) AS mean_h FROM ts_out_derived) AS t",
    ] {
        let rows = server
            .client
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(rows.len(), 1, "{sql}: one row");
        let mean: f64 = rows[0].get("mean_h");
        let want = (40000.5 + 40001.5 + 39990.0) / 3.0;
        assert!((mean - want).abs() < 1e-9, "{sql}: {mean} != {want}");
    }
}

/// Reading a continuous aggregate, and aggregating over it, follows the
/// SELECT list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_aggregate_reads_follow_the_select_list() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    for sql in [
        "CREATE COLLECTION ts_out_src (ts BIGINT TIME_KEY, station TEXT, h_nt FLOAT) \
         WITH (engine='timeseries')",
        "CREATE CONTINUOUS AGGREGATE ts_out_view ON ts_out_src BUCKET '1m' \
         AGGREGATE AVG(h_nt) AS a, COUNT(*) AS c GROUP BY station",
        "INSERT INTO ts_out_src (ts, station, h_nt) VALUES (1000, 'TMR', 1.5)",
        "INSERT INTO ts_out_src (ts, station, h_nt) VALUES (2000, 'TMR', 2.5)",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    // The flush that feeds an `OnFlush` aggregate runs after the insert
    // answers, so the read polls until the bucket is there.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !server
        .query_rows("SELECT c FROM ts_out_view")
        .await
        .is_ok_and(|rows| !rows.is_empty())
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let cases: [(&str, Vec<(String, u32)>); 2] = [
        (
            "SELECT c, a, station, bucket FROM ts_out_view",
            vec![
                ("c".to_string(), INT8),
                ("a".to_string(), FLOAT8),
                ("station".to_string(), TEXT),
                ("bucket".to_string(), INT8),
            ],
        ),
        (
            "SELECT station AS s, MAX(a) AS top FROM ts_out_view GROUP BY station",
            vec![("s".to_string(), TEXT), ("top".to_string(), FLOAT8)],
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(extended_columns(&server, sql).await, want, "{sql}");
        let names: Vec<String> = want.iter().map(|(name, _)| name.clone()).collect();
        assert_eq!(simple_columns(&server, sql).await, names, "{sql}");
        assert_eq!(native_columns(&server, sql).await, names, "{sql}");
    }
}
