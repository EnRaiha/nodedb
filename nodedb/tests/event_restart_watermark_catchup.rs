// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for the Event Plane consumer's restart-recovery gap:
//! on boot it must replay `(watermark, WAL head]`, not just serve the
//! (empty) in-memory ring buffer. Simulated by rewinding every core's
//! persisted watermark to zero across a graceful restart and checking that
//! the `N` original inserts' AFTER triggers re-fire on WAL catchup.

use std::time::Duration;

use crate::harness::TestServer;
use nodedb::event::watermark::WatermarkStore;
use nodedb::types::Lsn;

/// Poll `fire_log` until it holds exactly `expected` rows, or fail with the
/// last observed count once `timeout` elapses. The Event Plane dispatches
/// asynchronously, so the log population lags both the live inserts and the
/// post-restart WAL-catchup replay.
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
