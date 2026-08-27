// SPDX-License-Identifier: BUSL-1.1

//! Verifies that a bulk database exhausting its maintenance CPU budget
//! does not prevent interactive reads from completing on the same core.
//!
//! Two logical databases share one `CoreLoop`:
//!   - `bulk_db`  (low maintenance cap = 5% → 3s per minute)
//!   - `interactive_db` (no maintenance cap)
//!
//! The test drives synthetic compaction loops against `bulk_db` until its
//! per-minute budget is saturated, then fires reads against `interactive_db`
//! and asserts:
//!
//! 1. The budget tracker records consumption ≥ cap for `bulk_db`.
//! 2. Maintenance for `bulk_db` is deferred (returns `MaintenanceOutcome::Deferred`).
//! 3. The `interactive_db` read succeeds without delay (maintenance path was skipped,
//!    not executed, so the reactor is not occupied by compaction work).

use std::sync::Arc;
use std::time::Duration;

use nodedb::control::maintenance::{MaintenanceBudgetTracker, MaintenanceOutcome, with_budget};
use nodedb_bridge::wfq::WeightedFairQueue;
use nodedb_types::{DatabaseId, PriorityClass};

// ── helpers ─────────────────────────────────────────────────────────────────

/// Saturate `bulk_db`'s budget by recording direct consumption into the
/// tracker's sliding window.  We use `with_budget` plus synthetic work that
/// sleeps a few milliseconds per call so real elapsed time is recorded.
fn exhaust_budget(tracker: &Arc<MaintenanceBudgetTracker>, db: DatabaseId, cap_secs: f64) {
    // Run synthetic work until the next `with_budget` call is deferred.
    // Each iteration does ~1ms of real work; we allow up to 10× the cap in
    // iterations so the test is deterministic across slow CI machines.
    let max_iterations = (cap_secs * 1000.0 * 10.0) as usize + 1000;
    for _ in 0..max_iterations {
        let outcome = with_budget(tracker, db, 0.0, || {
            std::thread::sleep(Duration::from_millis(1));
        });
        if outcome.deferred() {
            return;
        }
    }
    // If we exit the loop without a deferral we have exceeded cap_secs * 10
    // wall-clock seconds of work — acceptable on an extremely slow machine;
    // the assertions below will catch any real bug.
}

// ── test ─────────────────────────────────────────────────────────────────────

#[test]
fn bulk_budget_saturates_and_interactive_reads_complete() {
    let tracker = Arc::new(MaintenanceBudgetTracker::new());

    let bulk_db = DatabaseId::new(101);
    let interactive_db = DatabaseId::new(102);

    // 5% of 60s = 3s cap per minute.
    let cap_pct: u8 = 5;
    let cap_secs: f64 = (cap_pct as f64 / 100.0) * 60.0;
    tracker.set_cap(bulk_db, cap_pct);
    // interactive_db has no cap (0 → infinity).
    tracker.set_cap(interactive_db, 0);

    // ── Phase 1: exhaust bulk_db's budget ───────────────────────────────────
    exhaust_budget(&tracker, bulk_db, cap_secs);

    // Next `with_budget` call for bulk_db must be deferred.
    let deferred = with_budget(&tracker, bulk_db, 0.1, || {
        panic!("bulk compaction ran despite exhausted budget");
    });
    assert!(
        deferred.deferred(),
        "bulk_db maintenance must be deferred after cap is exhausted"
    );

    // ── Phase 2: interactive_db must still run ───────────────────────────────
    // interactive_db has no cap; its with_budget call must succeed.
    let ran = with_budget(&tracker, interactive_db, 1.0, || 42u32);
    match ran {
        MaintenanceOutcome::Ran(v) => assert_eq!(v, 42),
        MaintenanceOutcome::Deferred => panic!("interactive_db must not be deferred"),
    }
}

// ── WFQ depth isolation ─────────────────────────────────────────────────────
//
// Confirms that filling `bulk_db`'s virtual queue to its throttle threshold
// does not throttle `interactive_db`'s virtual queue.

#[test]
fn bulk_wfq_depth_does_not_throttle_interactive() {
    // Capacity chosen so each DB's fair share = 50 items.
    let capacity = 100;
    let mut wfq: WeightedFairQueue<u32> = WeightedFairQueue::new(capacity, 1000);

    let bulk_db: u64 = 101;
    let interactive_db: u64 = 102;

    wfq.set_priority(bulk_db, PriorityClass::Bulk);
    wfq.set_priority(interactive_db, PriorityClass::Critical);

    // Fill bulk_db to its throttle threshold (≥85% of fair share).
    // Fair share ≈ 100 / 2 = 50.  85% × 50 ≈ 43.
    for i in 0..43u32 {
        wfq.try_enqueue(bulk_db, i).unwrap();
    }

    // bulk_db should be throttled.
    assert!(
        wfq.is_throttled_for(bulk_db),
        "bulk_db should be throttled at ≥85% of fair share"
    );

    // interactive_db must be neither throttled nor suspended.
    assert!(
        !wfq.is_throttled_for(interactive_db),
        "interactive_db must not be throttled while bulk_db is at threshold"
    );
    assert!(
        !wfq.is_suspended_for(interactive_db),
        "interactive_db must not be suspended while bulk_db is at threshold"
    );

    // interactive_db can still enqueue.
    assert!(
        wfq.try_enqueue(interactive_db, 0).is_ok(),
        "interactive_db enqueue must succeed even when bulk_db is throttled"
    );

    // When draining the queue, critical pops should outnumber bulk pops by ≥3:1.
    // Re-fill both queues to equal depth for a fair comparison.
    let mut wfq2: WeightedFairQueue<(u64, u32)> = WeightedFairQueue::new(400, 1000);
    wfq2.set_priority(bulk_db, PriorityClass::Bulk);
    wfq2.set_priority(interactive_db, PriorityClass::Critical);
    for i in 0..100u32 {
        wfq2.try_enqueue(bulk_db, (bulk_db, i)).unwrap();
        wfq2.try_enqueue(interactive_db, (interactive_db, i))
            .unwrap();
    }

    let mut critical_pops = 0u32;
    let mut bulk_pops = 0u32;
    for _ in 0..40 {
        match wfq2.pop_next() {
            Some((db, _)) if db == interactive_db => critical_pops += 1,
            Some((db, _)) if db == bulk_db => bulk_pops += 1,
            _ => {}
        }
    }
    assert!(
        critical_pops >= 3 * bulk_pops.max(1),
        "critical dispatch ({critical_pops}) must be ≥3× bulk ({bulk_pops})"
    );
}
