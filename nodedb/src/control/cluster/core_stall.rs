// SPDX-License-Identifier: BUSL-1.1

//! Detection of a Data Plane core that has stopped completing event-loop
//! iterations, and the node-wide marker that makes it visible.
//!
//! `data::core_health::CoreHealthWatchdog` counts consecutive panics inside
//! one core's loop and is loop-local: a core that stops making progress
//! without panicking is invisible to it, produces no ERROR line, and leaves
//! `/healthz` green. This module is the missing observation — taken from the
//! Control Plane, across every core.
//!
//! The signal is one `AtomicU64` per core on
//! [`SystemMetrics`](crate::control::metrics::SystemMetrics), incremented at
//! the top of each event-loop iteration before any work. A Control Plane
//! monitor samples every counter on a fixed interval and compares the sample
//! against the previous one; a counter that did not advance across a whole
//! window means that core did not complete a single iteration in it.
//!
//! WHAT THIS SIGNAL DOES NOT SEPARATE — read before acting on it. A core
//! executing one pathologically long `tick()` also stops incrementing, for
//! the same observable reason a wedged core does. `MAX_TASKS_PER_ITERATION`
//! in `data::runtime::event_loop` bounds how many requests one iteration
//! processes, not the wall-clock duration of a single `tick()`, and nothing
//! else in the codebase bounds it either. So "stalled" here means exactly
//! "this core is not completing iterations" — strictly more than the silence
//! it replaces, but not by itself a distinction between a wedge and one
//! pathological operation. An operator seeing this must still look at what
//! that core was running.
//!
//! Unlike the sibling markers
//! [`MetadataApplyWedge`](super::metadata_applier::MetadataApplyWedge) and
//! [`SequencerHaltMarker`](super::SequencerHaltMarker), which latch
//! first-writer-wins because their conditions never clear, a stall can
//! recover on its own. [`CoreStallMarker`] is therefore replaced on every
//! sampling window and reports the current set of stalled cores.

use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

/// Indices of every core whose heartbeat did not advance between two
/// consecutive samples, ascending.
///
/// Both slices are indexed by core id; cores are dense `0..num_cores`, so a
/// length mismatch can only come from a truncated sample and the shorter
/// length wins. A core sitting at its initial value in both samples has never
/// reached the top of its loop and is reported like any other non-advancing
/// core — the counter comparison cannot tell those apart, and the operator
/// action is the same.
pub fn detect_stalled_cores(previous: &[u64], current: &[u64]) -> Vec<usize> {
    previous
        .iter()
        .zip(current.iter())
        .enumerate()
        .filter(|(_, (prev, cur))| cur == prev)
        .map(|(core_id, _)| core_id)
        .collect()
}

/// Node-wide record of which Data Plane cores are currently stalled.
///
/// Replaced, not latched: the monitor calls [`set`](Self::set) once per
/// sampling window with the freshly computed set, so recovery clears the
/// marker without operator action.
///
/// `stalled` mirrors "the core list is non-empty" so the health surfaces —
/// polled per `/healthz` scrape and per native `STATUS` — decide with a single
/// atomic load and touch neither the lock nor the allocator while the node is
/// healthy.
#[derive(Debug, Default)]
pub struct CoreStallMarker {
    stalled: AtomicBool,
    cores: RwLock<Vec<usize>>,
}

impl CoreStallMarker {
    /// Replace the current report with `stalled_cores`. An empty set clears
    /// the marker.
    pub fn set(&self, stalled_cores: Vec<usize>) {
        let any = !stalled_cores.is_empty();
        match self.cores.write() {
            Ok(mut guard) => *guard = stalled_cores,
            Err(poisoned) => *poisoned.into_inner() = stalled_cores,
        }
        self.stalled.store(any, Ordering::Release);
    }

    /// Equivalent to `set(Vec::new())`.
    pub fn clear(&self) {
        self.set(Vec::new());
    }

    /// Every currently stalled core index, or `None` when none is stalled.
    pub fn report(&self) -> Option<Vec<usize>> {
        if !self.stalled.load(Ordering::Acquire) {
            return None;
        }
        let cores = match self.cores.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        if cores.is_empty() { None } else { Some(cores) }
    }

    /// Whether any core is currently stalled. One atomic load.
    pub fn is_stalled(&self) -> bool {
        self.stalled.load(Ordering::Acquire)
    }
}

/// Readiness-probe rendering for a stalled node.
///
/// `503`, matching the `metadata_apply_wedge` and `sequencer_halt` branches
/// in `control::server::http::routes::health::healthz`. The body names every
/// stalled core, because "some core is stuck" is not something an operator
/// can act on.
pub fn to_http_response(stalled_cores: &[usize]) -> (axum::http::StatusCode, serde_json::Value) {
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        serde_json::json!({
            "status": "failed",
            "reason": "data_plane_core_stalled",
            "stalled_cores": stalled_cores,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- stall decision --------------------------------------------------

    #[test]
    fn all_cores_advancing_reports_no_stalls() {
        let previous = vec![10u64, 20, 30];
        let current = vec![11u64, 25, 31];
        assert_eq!(
            detect_stalled_cores(&previous, &current),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn one_frozen_core_is_reported() {
        let previous = vec![10u64, 20, 30];
        let current = vec![11u64, 20, 31]; // core 1 unchanged
        assert_eq!(detect_stalled_cores(&previous, &current), vec![1]);
    }

    #[test]
    fn multiple_frozen_cores_are_all_reported_in_order() {
        let previous = vec![10u64, 20, 30, 40];
        // Cores 1 and 2 advance; 0 and 3 do not.
        let current = vec![10u64, 21, 31, 40];
        assert_eq!(detect_stalled_cores(&previous, &current), vec![0, 3]);
    }

    #[test]
    fn core_recovers_after_being_stalled() {
        // Sampling window 1: core 2 does not advance.
        let s0 = vec![5u64, 5, 5];
        let s1 = vec![6u64, 6, 5];
        assert_eq!(detect_stalled_cores(&s0, &s1), vec![2]);

        // Sampling window 2: core 2 advances again — no longer stalled.
        let s2 = vec![7u64, 7, 6];
        assert_eq!(detect_stalled_cores(&s1, &s2), Vec::<usize>::new());
    }

    #[test]
    fn core_that_has_never_ticked_is_reported() {
        // Core 0 is still at its initial value (0) in both samples: it has
        // never reached the top of its event loop even once. The detector
        // reports it the same way it reports a core that ran and later
        // froze — the counter comparison alone cannot and need not tell
        // the two apart.
        let previous = vec![0u64, 100];
        let current = vec![0u64, 105];
        assert_eq!(detect_stalled_cores(&previous, &current), vec![0]);
    }

    #[test]
    fn stall_signal_cannot_distinguish_long_tick_from_freeze() {
        // KNOWN LIMIT OF THIS SIGNAL, not a bug: a core executing one
        // pathologically long tick() also never reaches the top of its
        // loop during the sampling window, for the same observable reason
        // a truly wedged core does not. `MAX_TASKS_PER_ITERATION` bounds
        // requests per iteration, never the wall-clock time of a single
        // tick(), so nothing in the codebase rules this out.
        //
        // This test does NOT assert the detector can tell the two causes
        // apart — it asserts the opposite: the same (previous, current)
        // pair, which could equally have come from either cause, always
        // produces the same "stalled" verdict. Do not read a passing test
        // here as proof of stall detection; it is proof of the ambiguity.
        let previous = vec![42u64];
        let current = vec![42u64];
        assert_eq!(detect_stalled_cores(&previous, &current), vec![0]);
    }

    // ---- marker ------------------------------------------------------------

    #[test]
    fn marker_starts_clear() {
        let marker = CoreStallMarker::default();
        assert!(!marker.is_stalled());
        assert_eq!(marker.report(), None);
    }

    #[test]
    fn marker_reports_which_cores_are_stalled_not_merely_that_some_are() {
        let marker = CoreStallMarker::default();
        marker.set(vec![1, 3]);
        assert!(marker.is_stalled());
        assert_eq!(marker.report(), Some(vec![1, 3]));
    }

    #[test]
    fn marker_clears_back_to_healthy() {
        let marker = CoreStallMarker::default();
        marker.set(vec![2]);
        assert!(marker.is_stalled());

        marker.clear();
        assert!(!marker.is_stalled());
        assert_eq!(marker.report(), None);
    }

    #[test]
    fn marker_replaces_rather_than_accumulates_across_samples() {
        // Unlike `MetadataApplyWedge` / `SequencerHaltMarker` (first writer
        // wins, never clears), a stall can recover, so each new sampling
        // window's `set` must replace — not merge with — the previous
        // report.
        let marker = CoreStallMarker::default();
        marker.set(vec![1]);
        marker.set(vec![2, 3]);
        assert_eq!(marker.report(), Some(vec![2, 3]));

        marker.set(Vec::new());
        assert!(!marker.is_stalled());
    }

    // ---- health rendering ----------------------------------------------------

    #[test]
    fn http_response_is_503_naming_the_stalled_core() {
        let (code, body) = to_http_response(&[2]);
        assert_eq!(code, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["stalled_cores"], serde_json::json!([2]));
    }

    #[test]
    fn http_response_names_every_stalled_core() {
        let (code, body) = to_http_response(&[1, 4]);
        assert_eq!(code, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["stalled_cores"], serde_json::json!([1, 4]));
    }
}
