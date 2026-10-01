// SPDX-License-Identifier: BUSL-1.1

//! The response poller: routes Data Plane responses to waiting sessions.

use std::sync::Arc;
use std::time::Duration;

use crate::control::shutdown::{ShutdownBus, ShutdownPhase, spawn_loop_no_abort};
use crate::control::state::SharedState;

/// Idle passes that only yield before the poller starts sleeping.
const YIELD_IDLE_PASSES: u32 = 256;
/// Idle passes that sleep `SHORT_IDLE_SLEEP` before the longer sleep.
const SHORT_SLEEP_IDLE_PASSES: u32 = 1024;
const SHORT_IDLE_SLEEP: Duration = Duration::from_millis(1);
const LONG_IDLE_SLEEP: Duration = Duration::from_millis(10);

/// Spawn the response poller loop (routes Data Plane responses to waiting sessions).
///
/// The poller is a `DrainingDataPlane` participant, and it exits on the Data
/// Plane drain latch rather than on the flat shutdown signal. Two things depend
/// on that: the final checkpoint dispatched during `DrainingControlPlane` waits
/// for one response per core, and the Data Plane drain itself is finished only
/// once every in-flight response has reached its session. A poller that stops
/// at the flat signal strands both.
///
/// The registry must never abort it, so it registers as no-abort. The drain
/// latch is its one safe exit boundary, and a cancellation short of that
/// strands the responses the poller exists to route.
pub fn spawn_response_poller(shared: &Arc<SharedState>, bus: &ShutdownBus) {
    let shared_poller = Arc::clone(shared);
    let drain_guard = bus.register_task(ShutdownPhase::DrainingDataPlane, "response_poller", None);
    spawn_loop_no_abort(
        &shared.loop_registry,
        &shared.shutdown,
        "response_poller",
        ShutdownPhase::DrainingDataPlane,
        move |_shutdown| async move {
            let mut idle_iters: u32 = 0;
            loop {
                if shared_poller.data_plane_drain.is_complete() {
                    // One last pass so a response the drain's own final poll
                    // raced is still routed before the loop ends.
                    shared_poller.poll_and_route_responses();
                    break;
                }
                let routed = shared_poller.poll_and_route_responses();
                if routed > 0 {
                    idle_iters = 0;
                    tokio::task::yield_now().await;
                    continue;
                }
                idle_iters = idle_iters.saturating_add(1);
                if idle_iters <= YIELD_IDLE_PASSES {
                    tokio::task::yield_now().await;
                } else if idle_iters <= SHORT_SLEEP_IDLE_PASSES {
                    tokio::time::sleep(SHORT_IDLE_SLEEP).await;
                } else {
                    tokio::time::sleep(LONG_IDLE_SLEEP).await;
                }
            }
            drain_guard.report_drained();
        },
    );
}
