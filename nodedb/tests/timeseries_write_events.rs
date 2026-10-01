// SPDX-License-Identifier: BUSL-1.1

//! Event Plane consumers of a timeseries collection see its inserts.
//!
//! An AFTER trigger and a change stream on one timeseries collection each see
//! a row ingested over ILP and a row inserted over SQL. The collection runs
//! once with its WAL and once with `wal = 'false'`, which ingests without a
//! WAL record. Runs against the real server binary: ILP ingest has its own
//! listener and is Calvin-sequenced.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::{CrashHarness, Session};
use nodedb_test_support::ilp_client;

const COLLECTION: &str = "ts_events";
const TRIGGER_LOG: &str = "ts_events_seen";
const STREAM: &str = "ts_events_feed";
const GROUP: &str = "ts_events_group";
const INGEST_USER: &str = "ts_events_ilp";
const INGEST_PASSWORD: &str = "ts-events-ilp-secret-1";

/// Long enough for the ILP batch timer, the Event Plane and the trigger body
/// to finish many times over, so "never arrived" cannot mean "not yet".
const ARRIVAL_WAIT: Duration = Duration::from_secs(20);

/// Column of `SELECT * FROM STREAM` that holds the event kind.
const EVENT_TYPE_COLUMN: usize = 3;

fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos()
}

/// Boot a server with a timeseries collection whose WAL setting is `wal`,
/// an AFTER INSERT trigger that logs each row, and a change stream with a
/// consumer group.
async fn start(wal: &str) -> CrashHarness {
    let mut harness = CrashHarness::new();
    harness.spawn();
    harness.wait_ready();
    harness.wait_for_calvin_ready(Duration::from_secs(20)).await;

    harness
        .exec(&format!(
            "CREATE TIMESERIES {COLLECTION} (timestamp TIMESTAMP, value FLOAT) \
             WITH (wal = '{wal}')"
        ))
        .await;
    harness
        .exec(&format!("CREATE COLLECTION {TRIGGER_LOG}"))
        .await;
    harness
        .exec(&format!(
            "CREATE TRIGGER ts_events_log AFTER INSERT ON {COLLECTION} FOR EACH ROW \
             BEGIN INSERT INTO {TRIGGER_LOG} (value) VALUES (NEW.value); END"
        ))
        .await;
    harness
        .exec(&format!("CREATE CHANGE STREAM {STREAM} ON {COLLECTION}"))
        .await;
    harness
        .exec(&format!("CREATE CONSUMER GROUP {GROUP} ON {STREAM}"))
        .await;
    harness
        .exec(&format!(
            "CREATE USER {INGEST_USER} PASSWORD '{INGEST_PASSWORD}'"
        ))
        .await;
    harness
        .exec(&format!("GRANT ROLE readwrite TO {INGEST_USER}"))
        .await;
    harness
}

/// Poll column `column` of `sql` on `session` until `seen` holds, or panic
/// naming `what` and the last rows read.
async fn wait_until(
    session: &Session<'_>,
    sql: &str,
    column: usize,
    what: &str,
    seen: impl Fn(&[String]) -> bool,
) {
    let deadline = Instant::now() + ARRIVAL_WAIT;
    loop {
        let rows = session
            .try_query_col_idx(sql, column)
            .await
            .unwrap_or_default();
        if seen(&rows) {
            return;
        }
        if Instant::now() >= deadline {
            panic!("{what} within {ARRIVAL_WAIT:?}; last read of `{sql}`: {rows:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A `COUNT(*)` result equal to `expected`.
fn count_is(expected: usize) -> impl Fn(&[String]) -> bool {
    move |rows| rows.first().and_then(|n| n.parse::<usize>().ok()) == Some(expected)
}

/// Stream rows of which exactly `expected` are inserts.
fn inserts_are(expected: usize) -> impl Fn(&[String]) -> bool {
    move |kinds| {
        kinds
            .iter()
            .filter(|kind| kind.as_str() == "INSERT")
            .count()
            == expected
    }
}

/// Ingest one row over ILP, then one over SQL. The trigger and the change
/// stream each see the ILP row, then both rows.
async fn consumers_see_ilp_and_sql_inserts(wal: &str) {
    let harness = start(wal).await;
    let session = harness.connect().await;
    let log_count = format!("SELECT COUNT(*) FROM {TRIGGER_LOG}");
    let stream_read = format!("SELECT * FROM STREAM {STREAM} CONSUMER GROUP {GROUP} LIMIT 100");

    let ilp_addr: std::net::SocketAddr = format!("127.0.0.1:{}", harness.ilp_port)
        .parse()
        .expect("loopback ILP address must parse");
    let mut ilp = ilp_client::connect_and_auth(ilp_addr, INGEST_USER, INGEST_PASSWORD).await;
    ilp_client::send_line(&mut ilp, &format!("{COLLECTION} value=1 {}", now_ns())).await;

    wait_until(
        &session,
        &log_count,
        0,
        &format!("the AFTER trigger never logged the ILP row (wal = '{wal}')"),
        count_is(1),
    )
    .await;
    wait_until(
        &session,
        &stream_read,
        EVENT_TYPE_COLUMN,
        &format!("the change stream never held the ILP row (wal = '{wal}')"),
        inserts_are(1),
    )
    .await;
    // Held open until the row is seen: dropping it earlier races the
    // server's batch flush.
    drop(ilp);

    let now_ms = now_ns() / 1_000_000;
    harness
        .exec(&format!(
            "INSERT INTO {COLLECTION} (timestamp, value) VALUES ({now_ms}, 2.0)"
        ))
        .await;

    wait_until(
        &session,
        &log_count,
        0,
        &format!("the AFTER trigger never logged the SQL row (wal = '{wal}')"),
        count_is(2),
    )
    .await;
    wait_until(
        &session,
        &stream_read,
        EVENT_TYPE_COLUMN,
        &format!("the change stream never held the SQL row (wal = '{wal}')"),
        inserts_are(2),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn consumers_see_timeseries_inserts_with_the_wal() {
    consumers_see_ilp_and_sql_inserts("true").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn consumers_see_timeseries_inserts_without_the_wal() {
    consumers_see_ilp_and_sql_inserts("false").await;
}
