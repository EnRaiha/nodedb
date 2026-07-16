// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for the Event Plane consumer's restart-recovery gap.
//!
//! `consumer_loop` (`nodedb/src/event/consumer.rs`) loads the persisted
//! per-core watermark on boot, but used to hardcode the starting mode to
//! `Normal`. `Normal` mode only ever reads the in-memory ring buffer — which
//! is always empty right after a process starts — and `WalCatchup` mode
//! (which replays the WAL from `watermark.next()` onward and dispatches
//! every replayed event) was only ever entered from *live* Normal-mode
//! processing (slab-shed / backpressure-suspended transitions). So on a real
//! restart, any WAL-committed event that had not yet been consumed and
//! watermarked before the crash was silently dropped: the consumer served
//! the empty ring buffer forever and never replayed `(watermark, WAL head]`.
//!
//! This test simulates that crash condition deterministically: it lets an
//! ASYNC AFTER-INSERT trigger fire for `N` inserts (proving live dispatch
//! works), restarts the server, and — before reopening — rewinds the
//! persisted watermark for every core back to `Lsn::ZERO` via
//! `WatermarkStore`. That reproduces "the persisted watermark trails the WAL
//! head" exactly as a crash before a watermark flush would. On reopen, the
//! fixed consumer boots into `WalCatchup`, replays the whole WAL suffix, and
//! re-dispatches the `N` original inserts, so `fire_log` grows from `N` to
//! `2 * N`. In this test's forced-rewind setup that doubling is a
//! "duplicate," but it is produced by the exact same replay path that, after
//! a genuine crash, RECOVERS events that were committed to the WAL but never
//! dispatched — so reaching `2 * N` is the correct regression guard for the
//! loss bug. With the old hardcoded-`Normal` boot, the consumer ignores the
//! rewound watermark, finds the ring buffer empty, replays nothing, and
//! `fire_log` stays stuck at `N`.

mod common;

use std::time::Duration;

use common::pgwire_harness::TestServer;
use nodedb::event::watermark::WatermarkStore;
use nodedb::types::Lsn;

/// Poll `fire_log` until it holds exactly `expected` rows, or fail with the
/// last observed count once `timeout` elapses. Mirrors the polling helper in
/// `bulk_dml_event_emission.rs` — the Event Plane dispatches asynchronously,
/// so the log population lags both the live inserts and the post-restart
/// WAL-catchup replay.
async fn wait_for_fire_log_count(server: &TestServer, expected: usize, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let rows = server
            .query_text("SELECT marker FROM fire_log")
            .await
            .unwrap();
        if rows.len() == expected {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for fire_log to reach {expected} row(s), got {} row(s): {rows:?}",
            rows.len()
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_replays_wal_suffix_past_rewound_watermark() {
    const N: usize = 3;

    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION src (id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION fire_log").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER on_ins AFTER INSERT ON src FOR EACH ROW \
             BEGIN INSERT INTO fire_log (marker) VALUES ('i'); END;",
        )
        .await
        .unwrap();

    for i in 0..N {
        server
            .exec(&format!("INSERT INTO src (id, v) VALUES ('row{i}', {i})"))
            .await
            .unwrap();
    }

    // Live dispatch: the consumer processes the N inserts via the ring buffer.
    wait_for_fire_log_count(&server, N, Duration::from_secs(5)).await;

    // Restart, rewinding every core's watermark to ZERO in between to
    // deterministically recreate "persisted watermark trails WAL head."
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    {
        let store = WatermarkStore::open(dir.path()).unwrap();
        for core in 0..64 {
            store.save(core, Lsn::ZERO).unwrap();
        }
        // Drop before reopening the server so the redb file lock is released.
    }
    let (server, _dir) = TestServer::open_on_path(dir).await;

    // Boot-time WAL catchup must replay the WAL suffix past the rewound
    // watermark, re-dispatching the N original inserts' triggers.
    wait_for_fire_log_count(&server, 2 * N, Duration::from_secs(5)).await;
}
