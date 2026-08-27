// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: scheduler must observe shutdown, cap concurrency,
//! and reject jobs when budget is exceeded.
//!
//! Today `scheduler_loop` fires `tokio::spawn` per due schedule with:
//!   * no shutdown observation inside the spawned job — long-running jobs
//!     hold `Arc<SharedState>` and delay teardown;
//!   * no concurrency cap — N schedules at `* * * * *` with
//!     `allow_overlap = true` spawn N heavy SQLs at every minute-mark,
//!     starving request-path queries;
//!   * no memory ceiling — `execute_job` may materialise unbounded result
//!     sets through `execute_block`.
//!
//! The fix extracts a `JobDispatcher` (or equivalent) that owns a
//! semaphore bound and a shutdown receiver. This file gates on that type
//! and its contract.

use std::sync::Arc;

use nodedb::event::scheduler::dispatcher::{DispatchOutcome, JobDispatcher, JobDispatcherConfig};

#[tokio::test]
async fn dispatcher_rejects_when_concurrency_cap_reached() {
    // Cap concurrency at 2. Spawn 5 never-returning jobs: the 3rd, 4th, 5th
    // must be rejected with `OverBudget`, not queued unbounded and not
    // spawned as free-running tasks.
    let cfg = JobDispatcherConfig {
        max_concurrent_jobs: 2,
        max_result_bytes: 1 << 20,
    };
    let dispatcher = Arc::new(JobDispatcher::new(cfg));

    let outcomes: Vec<DispatchOutcome> = (0..5)
        .map(|_| {
            dispatcher.try_spawn(|_shutdown| async move {
                // Simulate a job that never returns until aborted.
                std::future::pending::<()>().await;
                Ok(())
            })
        })
        .collect();

    let accepted = outcomes
        .iter()
        .filter(|o| matches!(o, DispatchOutcome::Spawned))
        .count();
    let rejected = outcomes
        .iter()
        .filter(|o| matches!(o, DispatchOutcome::OverBudget))
        .count();

    assert_eq!(accepted, 2, "only two jobs fit under the concurrency cap");
    assert_eq!(rejected, 3, "excess jobs must be rejected, not queued");

    dispatcher.shutdown_and_drain().await;
}

#[tokio::test]
async fn dispatcher_aborts_inflight_jobs_on_shutdown() {
    // Jobs observe the shutdown receiver. On shutdown the dispatcher must
    // signal shutdown, abort stragglers within the drain deadline, and
    // release its Arc<SharedState>-equivalent resources.
    let cfg = JobDispatcherConfig {
        max_concurrent_jobs: 4,
        max_result_bytes: 1 << 20,
    };
    let dispatcher = Arc::new(JobDispatcher::new(cfg));

    for _ in 0..3 {
        let outcome = dispatcher.try_spawn(|mut shutdown| async move {
            let _ = shutdown.changed().await;
            Ok(())
        });
        assert!(matches!(outcome, DispatchOutcome::Spawned));
    }

    assert_eq!(dispatcher.in_flight(), 3);
    dispatcher.shutdown_and_drain().await;
    assert_eq!(
        dispatcher.in_flight(),
        0,
        "all in-flight jobs must drain after shutdown_and_drain"
    );
}

#[tokio::test]
async fn dispatcher_rejects_jobs_whose_result_set_exceeds_memory_ceiling() {
    // The third compounding hazard: `execute_job` may produce unbounded
    // result sets. The dispatcher is the place that enforces a byte ceiling
    // — jobs exceeding it must surface an `OverBudget` outcome back to the
    // history store rather than silently eating memory.
    let cfg = JobDispatcherConfig {
        max_concurrent_jobs: 8,
        max_result_bytes: 1024,
    };
    let dispatcher = Arc::new(JobDispatcher::new(cfg));

    let outcome = dispatcher.try_spawn_with_budget(1 << 20, |_shutdown| async move {
        // Reporting a 1 MiB result set when the ceiling is 1 KiB must fail.
        Ok(())
    });
    assert!(
        matches!(outcome, DispatchOutcome::OverBudget),
        "job whose declared result-set size exceeds the ceiling must be rejected"
    );

    dispatcher.shutdown_and_drain().await;
}
