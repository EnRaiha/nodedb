// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for Event Plane cron scheduler.
//!
//! Tests: cron expression matching, per-collection affinity detection,
//! missed execution policy, overlap enforcement, job history.

use nodedb::event::scheduler::cron::CronExpr;
use nodedb::event::scheduler::executor::pending_minute_ticks;
use nodedb::event::scheduler::history::JobHistoryStore;
use nodedb::event::scheduler::types::{JobRun, MissedPolicy};

#[test]
fn cron_every_minute() {
    let expr = CronExpr::parse("* * * * *").unwrap();
    // Every minute boundary should match.
    assert!(expr.matches_epoch(0)); // 1970-01-01 00:00
    assert!(expr.matches_epoch(60)); // 1970-01-01 00:01
    assert!(expr.matches_epoch(3600)); // 1970-01-01 01:00
}

#[test]
fn cron_specific_minute() {
    let expr = CronExpr::parse("30 * * * *").unwrap();
    // 00:30 UTC → 30*60 = 1800s from epoch.
    assert!(expr.matches_epoch(1800));
    // 00:00 should NOT match.
    assert!(!expr.matches_epoch(0));
}

#[test]
fn cron_specific_hour_and_minute() {
    let expr = CronExpr::parse("0 12 * * *").unwrap();
    // 12:00 UTC → 12*3600 = 43200s from epoch.
    assert!(expr.matches_epoch(43200));
    // 13:00 should NOT match.
    assert!(!expr.matches_epoch(46800));
}

#[test]
fn cron_range() {
    let expr = CronExpr::parse("0 9-17 * * *").unwrap();
    // 09:00 through 17:00 should match.
    assert!(expr.matches_epoch(9 * 3600));
    assert!(expr.matches_epoch(12 * 3600));
    assert!(expr.matches_epoch(17 * 3600));
    // 08:00 should NOT match.
    assert!(!expr.matches_epoch(8 * 3600));
}

#[test]
fn cron_step() {
    let expr = CronExpr::parse("*/15 * * * *").unwrap();
    // Minutes 0, 15, 30, 45.
    assert!(expr.matches_epoch(0));
    assert!(expr.matches_epoch(15 * 60));
    assert!(expr.matches_epoch(30 * 60));
    assert!(!expr.matches_epoch(10 * 60));
}

#[test]
fn cron_invalid_rejected() {
    assert!(CronExpr::parse("").is_err());
    assert!(CronExpr::parse("* * *").is_err()); // Only 3 fields.
    assert!(CronExpr::parse("60 * * * *").is_err()); // Minute > 59.
}

#[test]
fn missed_policy_variants() {
    assert_eq!(MissedPolicy::Skip.as_str(), "SKIP");
    assert_eq!(MissedPolicy::CatchUp.as_str(), "CATCH_UP");
    assert_eq!(MissedPolicy::Queue.as_str(), "QUEUE");
    assert_eq!(MissedPolicy::from_str_opt("SKIP"), Some(MissedPolicy::Skip));
    assert_eq!(
        MissedPolicy::from_str_opt("CATCH_UP"),
        Some(MissedPolicy::CatchUp)
    );
    assert!(MissedPolicy::from_str_opt("INVALID").is_none());
}

#[test]
fn job_history_record_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobHistoryStore::open(dir.path()).unwrap();

    store
        .record(JobRun {
            database_id: 0,
            schedule_name: "cleanup".into(),
            tenant_id: 1,
            started_at: 1000,
            duration_ms: 50,
            success: true,
            error: None,
        })
        .unwrap();

    store
        .record(JobRun {
            database_id: 0,
            schedule_name: "cleanup".into(),
            tenant_id: 1,
            started_at: 2000,
            duration_ms: 30,
            success: false,
            error: Some("timeout".into()),
        })
        .unwrap();

    let runs = store.last_runs(0, 1, "cleanup", 10);
    assert_eq!(runs.len(), 2);
    // last_runs returns most recent first.
    assert!(!runs[0].success); // Most recent = the failing run.
    assert!(runs[1].success); // Older = the successful run.
}

// ── Minute-tick tracking (scheduler loop jitter-safety) ──
//
// The scheduler tick loop runs at 1-second cadence. Under Tokio
// scheduling latency / GC / leader handoff, the observed wall-clock
// second can jitter past `00` — the loop's current gate
// (`now_secs % 60 == 0`) then drops the entire minute.
//
// These tests pin the correct per-schedule contract: given the last
// minute already fired and the current observation, compute every
// matching minute in-between. The helper is mirrored in this test
// module to reproduce the current broken gate; the fix will move this
// helper into `executor.rs` as `pending_minute_ticks` and these tests
// switch to the real one. A one-line swap — no test rewrites.

#[test]
fn scheduler_fires_minute_even_when_observation_jittered() {
    // Spec: at now_secs = 61 with last_fired = 0, minute 1 MUST fire.
    // The old gate dropped it because 61 % 60 != 0.
    let cron = CronExpr::parse("* * * * *").unwrap();
    let fired = pending_minute_ticks(Some(0), 61, &cron, 0);
    assert_eq!(
        fired,
        vec![1],
        "minute 1 dropped on jittered observation at second 61"
    );
}

#[test]
fn scheduler_catches_up_across_skipped_minutes() {
    // Spec: if the loop stalls and observes seconds 195 after last
    // firing at minute 0, all matching minutes (1, 2, 3) must catch up.
    let cron = CronExpr::parse("* * * * *").unwrap();
    let fired = pending_minute_ticks(Some(0), 195, &cron, 0);
    assert_eq!(
        fired,
        vec![1, 2, 3],
        "skipped minutes not caught up; got {fired:?}"
    );
}

#[test]
fn scheduler_daily_cron_survives_jitter_on_trigger_minute() {
    // Spec: `0 3 * * *` (03:00 daily) must fire minute 180 even when
    // the tick observes 10821 (03:00:21) instead of 10800 (03:00:00).
    // Daily schedules missed this way don't fire for 24 hours.
    let cron = CronExpr::parse("0 3 * * *").unwrap();
    let fired = pending_minute_ticks(Some(179), 10821, &cron, 0);
    assert_eq!(
        fired,
        vec![180],
        "daily 03:00 schedule silently skipped on jittered tick"
    );
}

#[test]
fn scheduler_catchup_filters_through_cron() {
    // Spec: when catching up across skipped minutes, each candidate
    // minute must still be checked against the cron expression.
    // `*/5` matches minute 5 but not 1-4; catch-up from minute 0 at
    // second 303 (minute 5) must fire only minute 5.
    let cron = CronExpr::parse("*/5 * * * *").unwrap();
    let fired = pending_minute_ticks(Some(0), 5 * 60 + 3, &cron, 0);
    assert_eq!(
        fired,
        vec![5],
        "catch-up must filter through cron; got {fired:?}"
    );
}

#[test]
fn scheduler_does_not_refire_same_minute() {
    // Spec: after minute 1 has fired, subsequent within-minute ticks
    // (seconds 61..120) must not re-fire it.
    let cron = CronExpr::parse("* * * * *").unwrap();
    let fired = pending_minute_ticks(Some(1), 80, &cron, 0);
    assert!(
        fired.is_empty(),
        "minute 1 re-fired within its own minute window; got {fired:?}"
    );
}

#[test]
fn job_history_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = JobHistoryStore::open(dir.path()).unwrap();
        store
            .record(JobRun {
                database_id: 0,
                schedule_name: "s1".into(),
                tenant_id: 1,
                started_at: 1000,
                duration_ms: 10,
                success: true,
                error: None,
            })
            .unwrap();
    }
    let store = JobHistoryStore::open(dir.path()).unwrap();
    let runs = store.last_runs(0, 1, "s1", 10);
    assert_eq!(runs.len(), 1);
}
