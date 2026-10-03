// SPDX-License-Identifier: BUSL-1.1

//! Scheduled backups end to end on a real server.
//!
//! The test drives the scheduler tick itself: it calls `BackupJobs::fire`
//! with a chosen clock, which dispatches the real `run_scheduled_backup`
//! against the server's `SharedState`. Three due minutes write three
//! envelopes, `keep = 2` leaves the newest two, and each of those restores
//! into a fresh server with the rows it held when its minute ran.

use std::time::{Duration, Instant};

use nodedb::config::server::BackupScheduleSettings;
use nodedb::control::backup::schedule::envelope_name;
use nodedb::control::backup::schedule::marks::settled_through;
use nodedb::event::scheduler::backup_job::BackupJobs;
use nodedb::event::scheduler::dispatcher::{JobDispatcher, JobDispatcherConfig};
use nodedb_test_support::pgwire_harness::TestServer;

const DATABASE: &str = "sbk_shop";
const TARGET_DIR: &str = "nightly";

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Wait until `jobs` has nothing in flight.
async fn settle(jobs: &BackupJobs) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while jobs.in_flight() > 0 {
        assert!(
            Instant::now() < deadline,
            "a scheduled backup did not finish"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Restore the envelope at `source` into a fresh server and count its rows.
async fn restored_rows(source: &std::path::Path) -> Vec<String> {
    let fresh = TestServer::start_with_backup_root().await;
    let object = "incoming/restore.ndbb";
    let to = fresh.backup_root().join(object);
    std::fs::create_dir_all(to.parent().expect("object directory")).expect("create directory");
    std::fs::copy(source, &to).expect("copy envelope");
    exec(
        &fresh,
        &format!(
            "RESTORE DATABASE {DATABASE} FROM '{}'",
            fresh.backup_uri(object)
        ),
    )
    .await;
    exec(&fresh, &format!("USE DATABASE {DATABASE}")).await;
    fresh
        .query_text("SELECT COUNT(*) FROM sbk_orders")
        .await
        .unwrap_or_else(|e| panic!("count restored rows: {e}"))
}

// The in-process server needs the multi-thread runtime: its DDL path
// (`CREATE COLLECTION`, `RESTORE DATABASE`) dispatches with
// `block_in_place`. The scheduled backup itself runs every blocking step on
// the blocking pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_due_minutes_keep_the_newest_two_envelopes_and_each_restores() {
    let server = TestServer::start_with_backup_root().await;
    exec(&server, &format!("CREATE DATABASE {DATABASE}")).await;
    exec(&server, &format!("USE DATABASE {DATABASE}")).await;
    exec(
        &server,
        "CREATE COLLECTION sbk_orders (id TEXT PRIMARY KEY, total INT) \
         WITH (engine='document_strict')",
    )
    .await;

    let schedule = BackupScheduleSettings {
        database: DATABASE.into(),
        target: server.backup_uri(TARGET_DIR),
        cron: "* * * * *".into(),
        keep: 2,
    };
    let jobs = BackupJobs::new(std::slice::from_ref(&schedule));
    let dispatcher = JobDispatcher::new(JobDispatcherConfig {
        max_concurrent_jobs: 4,
        max_result_bytes: u64::MAX,
    });
    let history = &server.shared.job_history;
    let base_min = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs()
        / 60;

    for run in 0..3u64 {
        exec(
            &server,
            &format!("INSERT INTO sbk_orders (id, total) VALUES ('o{run}', {run})"),
        )
        .await;
        let minute = base_min + run;
        jobs.fire(&server.shared, &dispatcher, history, minute * 60 + 5);
        settle(&jobs).await;
        assert_eq!(
            settled_through(&server.shared, &schedule).expect("read mark"),
            Some(minute),
            "run {run} raises the schedule mark to its minute"
        );
        // A second tick in the same minute finds nothing due.
        jobs.fire(&server.shared, &dispatcher, history, minute * 60 + 30);
        settle(&jobs).await;
    }

    // Only the newest two envelopes remain, each named for its minute.
    let dir = server.backup_root().join(TARGET_DIR);
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .expect("list target")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    let expected: Vec<String> = [base_min + 1, base_min + 2]
        .iter()
        .map(|minute| envelope_name(DATABASE, minute * 60_000))
        .collect();
    assert_eq!(names, expected);

    // Job history and metrics count three runs and one retention delete.
    let database_id = server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(DATABASE)
        .expect("catalog lookup")
        .expect("database exists")
        .as_u64();
    let runs = history.last_runs(database_id, 0, &schedule.job_name(), 10);
    assert_eq!(runs.len(), 3, "{runs:?}");
    assert!(runs.iter().all(|run| run.success), "{runs:?}");
    let metrics = server
        .shared
        .system_metrics
        .as_ref()
        .expect("system metrics");
    let load =
        |counter: &std::sync::atomic::AtomicU64| counter.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(load(&metrics.backup_schedule_runs_total), 3);
    assert_eq!(load(&metrics.backup_schedule_failures_total), 0);
    assert_eq!(load(&metrics.backup_schedule_envelopes_deleted_total), 1);
    assert!(load(&metrics.backup_schedule_last_success_timestamp_seconds) > 0);

    dispatcher.shutdown_and_drain().await;

    // Each remaining envelope restores the rows its minute saw.
    assert_eq!(restored_rows(&dir.join(&expected[0])).await, ["2"]);
    assert_eq!(restored_rows(&dir.join(&expected[1])).await, ["3"]);
}
