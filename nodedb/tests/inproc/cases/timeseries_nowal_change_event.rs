// SPDX-License-Identifier: BUSL-1.1

//! A timeseries write with `wal = 'false'` appends no WAL record, and its
//! change event still carries the write's commit time.
//!
//! The write funnel stamps the commit HLC of every write, logged or not, and
//! the event takes it from the write itself. A timeseries collection emits no
//! Event-Plane write event on any path, so its events are the Control-Plane
//! change stream's. The event's time lies within the window the INSERT ran
//! in, and a replay from the start of that window serves it.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nodedb::control::change_stream::{ChangeOperation, ReplayStart};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, TenantId};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .expect("the wall clock reads after the epoch")
}

#[tokio::test]
async fn an_unlogged_timeseries_write_dates_its_event_by_its_commit() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TIMESERIES ts_nowal WITH (wal = 'false', cdc = 'true')")
        .await
        .expect("CREATE TIMESERIES ts_nowal");

    let mut sub = server
        .shared
        .change_stream
        .subscribe(Some("ts_nowal".into()), None);

    let before = now_ms();
    server
        .exec("INSERT INTO ts_nowal (\"timestamp\", value) VALUES (1000, 1.5)")
        .await
        .expect("INSERT into the unlogged timeseries");
    let after = now_ms();

    let event = match tokio::time::timeout(Duration::from_secs(5), sub.recv_sequenced()).await {
        Ok(Ok(event)) => event,
        Ok(Err(e)) => panic!("change stream closed: {e}"),
        Err(_) => panic!("the unlogged timeseries write published no change event"),
    };
    assert_eq!(event.operation, ChangeOperation::Insert);
    assert!(
        (before..=after).contains(&event.timestamp_ms),
        "the event carries the write's commit time: {} outside {before}..={after}",
        event.timestamp_ms
    );

    let replayed = server
        .shared
        .change_stream
        .query_changes_in_database(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            Some("ts_nowal"),
            ReplayStart::Timestamp(before),
            16,
        )
        .expect("replay from the start of the write's window");
    assert_eq!(
        replayed.events.len(),
        1,
        "a replay from the write's commit window serves its event"
    );

    server.graceful_shutdown().await;
}
