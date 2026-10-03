// SPDX-License-Identifier: BUSL-1.1

//! A timeseries write event the ring drops is recovered from the WAL.
//!
//! A transaction's timeseries ingest into a collection with an AFTER trigger
//! records each row's event image in its redo record. A fail point drops the
//! install's event before the Event Plane takes it, as a full ring drops it.
//! WAL catch-up rebuilds the event from the record, and the trigger fires
//! for the row exactly once.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb::fail_point::FailGuard;
use nodedb_test_support::pgwire_harness::TestServer;

const COLLECTION: &str = "ts_drop";
const TRIGGER_LOG: &str = "ts_drop_seen";

/// Long enough for the Event Plane to notice the drop, run WAL catch-up and
/// run the trigger body many times over.
const ARRIVAL_WAIT: Duration = Duration::from_secs(10);

/// How long a second firing gets to show up after the first.
const SETTLE_WAIT: Duration = Duration::from_secs(2);

async fn logged_values(server: &TestServer) -> Vec<String> {
    server
        .query_text(&format!("SELECT v FROM {TRIGGER_LOG}"))
        .await
        .expect("read the trigger log")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_timeseries_event_fires_its_trigger_once_after_catch_up() {
    let server = TestServer::start().await;
    server
        .exec(&format!(
            "CREATE COLLECTION {COLLECTION} COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
             WITH (engine='timeseries')"
        ))
        .await
        .expect("create timeseries collection");
    server
        .exec(&format!("CREATE COLLECTION {TRIGGER_LOG}"))
        .await
        .expect("create trigger log");
    server
        .exec(&format!(
            "CREATE TRIGGER ts_drop_log AFTER INSERT ON {COLLECTION} FOR EACH ROW \
             BEGIN INSERT INTO {TRIGGER_LOG} (v) VALUES (NEW.v); END"
        ))
        .await
        .expect("create trigger");

    {
        // Armed only while the transaction commits: the install's event is
        // lost before the Event Plane takes it.
        let _drop = FailGuard::fail(&format!("event::bus::drop::{COLLECTION}"), "ring full");
        server.exec("BEGIN").await.expect("begin");
        server
            .exec(&format!(
                "INSERT INTO {COLLECTION} (id, ts, v) VALUES ('r1', 1000, 7)"
            ))
            .await
            .expect("stage the ingest");
        server.exec("COMMIT").await.expect("commit");
    }

    let deadline = tokio::time::Instant::now() + ARRIVAL_WAIT;
    loop {
        let values = logged_values(&server).await;
        if !values.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "WAL catch-up never delivered the dropped timeseries event within {ARRIVAL_WAIT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::sleep(SETTLE_WAIT).await;
    assert_eq!(
        logged_values(&server).await,
        vec!["7".to_string()],
        "the trigger fires for the recovered row exactly once"
    );
}
