// SPDX-License-Identifier: BUSL-1.1

//! The periodic restore point task.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::startup::StartupPhase;
use crate::control::state::SharedState;

use super::create::create_restore_point;

/// Create a restore point every `interval`. `None` starts nothing. Only the
/// node that runs the cluster's singleton workers creates them, so the
/// cluster takes one point per interval.
pub fn spawn_restore_point_task(shared: &Arc<SharedState>, interval: Option<Duration>) {
    let Some(interval) = interval else {
        return;
    };
    let task_state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "pitr_restore_point",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let state = task_state;
            let ready = tokio::select! {
                ready = state.startup.await_phase(StartupPhase::GatewayEnable) => ready.is_ok(),
                _ = shutdown.wait_cancelled() => false,
            };
            if !ready {
                return;
            }
            info!(
                interval_secs = interval.as_secs(),
                "restore point task started"
            );
            let mut tick =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if !state.is_singleton_worker() {
                    continue;
                }
                match create_restore_point(&state).await {
                    Ok(point) => info!(
                        restore_point = point.id,
                        hlc = point.hlc,
                        "periodic restore point created"
                    ),
                    Err(e) => warn!(error = %e, "periodic restore point not created"),
                }
            }
        },
    );
}
