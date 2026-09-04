// SPDX-License-Identifier: BUSL-1.1

//! Control Plane loop that watches every Data Plane core's liveness counter
//! and publishes the result on [`CoreStallMarker`].
//!
//! The Data Plane is `!Send` and shares nothing across the plane boundary but
//! atomics, so this is the only vantage point from which one core's failure to
//! make progress is observable at all. Reading the counters is the whole cost:
//! no lock is taken on the Data Plane side and no core is interrupted.
//!
//! See [`crate::control::cluster::core_stall`] for what the signal does and
//! does not distinguish — notably, a single very long `tick()` is
//! indistinguishable from a wedge here.

use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info};

use crate::control::cluster::core_stall::detect_stalled_cores;
use crate::control::startup::health::{HealthState, observe};
use crate::control::state::SharedState;

/// Gap between heartbeat samples.
///
/// Two constraints set this. The floor: a core's idle poll times out after
/// 100ms, so a healthy core completes roughly ten iterations per second, and a
/// core doing real work completes far more — 5s leaves a fifty-fold margin
/// before a merely busy core is mistaken for a stalled one, and gives any
/// single legitimately slow operation five seconds to finish. The ceiling:
/// this interval is also how late a real stall is noticed, since a wedge is
/// named one window after it starts and cleared one window after it ends. 5s
/// keeps that within a typical readiness-probe period, so `/healthz` reports
/// the stall on one of the first scrapes after it begins rather than minutes
/// later.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// Spawn the stall monitor. No-op when this process has no Data Plane cores
/// of its own — there is nothing to sample and no stall to report.
pub fn spawn_core_stall_monitor(shared: &Arc<SharedState>) {
    let Some(metrics) = shared.system_metrics.as_ref().map(Arc::clone) else {
        return;
    };
    if metrics.core_heartbeats.is_empty() {
        return;
    }

    let num_cores = metrics.core_heartbeats.len();
    let shared_monitor = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "core_stall_monitor",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
            // Both buffers are allocated once and swapped, so a loop that runs
            // for the life of the process allocates nothing per sample.
            let mut previous: Vec<u64> = Vec::with_capacity(num_cores);
            let mut current: Vec<u64> = Vec::with_capacity(num_cores);
            // The set named by the last log line, so a persistent stall is
            // reported when it starts and when it changes, never once per
            // sample.
            let mut reported: Vec<usize> = Vec::new();
            // `interval` yields its first tick immediately. Consume it before
            // taking the baseline, so the first comparison spans a full window
            // instead of two samples taken at the same instant — which would
            // read every core as stalled.
            tick.tick().await;
            metrics.core_heartbeats.sample_into(&mut previous);

            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }

                metrics.core_heartbeats.sample_into(&mut current);

                // Before the gateway opens, cores are still restoring
                // checkpoints and replaying the WAL and have not entered their
                // event loops. A counter that has not moved yet means "not
                // started", not "stalled", so re-baseline and judge nothing.
                if !matches!(observe(&shared_monitor.startup), HealthState::Ok) {
                    std::mem::swap(&mut previous, &mut current);
                    continue;
                }

                let stalled = detect_stalled_cores(&previous, &current);
                std::mem::swap(&mut previous, &mut current);

                if stalled != reported {
                    if stalled.is_empty() {
                        info!(
                            recovered_cores = ?reported,
                            "data plane cores are completing iterations again"
                        );
                    } else {
                        error!(
                            stalled_cores = ?stalled,
                            window_secs = SAMPLE_INTERVAL.as_secs(),
                            "data plane cores completed no event-loop iteration in the \
                             sampling window; work routed to them cannot complete"
                        );
                    }
                    reported.clear();
                    reported.extend_from_slice(&stalled);
                }

                shared_monitor.core_stall.set(stalled);
            }
        },
    );
}
