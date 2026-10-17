// SPDX-License-Identifier: BUSL-1.1

//! Fixed-interval Control Plane timers: tenant rate reset, audit flush,
//! and tenant memory estimation.

use std::sync::Arc;
use std::time::Duration;

use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

/// Period of the tenant rate counter reset.
const TENANT_RATE_RESET_PERIOD: Duration = Duration::from_secs(1);
/// Period of the audit log flush to the catalog.
const AUDIT_FLUSH_PERIOD: Duration = Duration::from_secs(10);
/// Period of the tenant memory estimate.
const TENANT_MEMORY_PERIOD: Duration = Duration::from_secs(30);

/// Spawn a Control Plane loop that runs `on_tick` once per `period` until
/// shutdown. The loop joins at the Control Plane drain.
pub fn spawn_tick_loop<F>(
    shared: &Arc<SharedState>,
    name: &'static str,
    period: Duration,
    on_tick: F,
) where
    F: Fn(&SharedState) + Send + 'static,
{
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        name,
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(period);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => on_tick(state.as_ref()),
                }
            }
        },
    );
}

/// Spawn the audit log flush loop.
///
/// The loop flushes every `AUDIT_FLUSH_PERIOD` and also when the buffer requests an early flush.
/// It is the flush executor: recorders past the backpressure point wait for its flushes.
/// On shutdown it flushes once more, so a clean stop leaves no entry only in memory.
fn spawn_audit_flush_loop(shared: &Arc<SharedState>) {
    let state = Arc::clone(shared);
    let control = match shared.audit.lock() {
        Ok(log) => log.flush_control(),
        Err(poisoned) => poisoned.into_inner().flush_control(),
    };
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "audit_log_flush",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            control.attach_executor();
            let mut tick = tokio::time::interval(AUDIT_FLUSH_PERIOD);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                    _ = control.flush_requested() => {}
                }
                control.take_flush_request();
                state.flush_audit_log();
            }
            state.flush_audit_log();
            control.detach_executor();
        },
    );
}

/// Spawn the tenant rate reset, audit log flush, and tenant memory
/// estimate timers.
pub fn spawn_tenant_timers(shared: &Arc<SharedState>) {
    spawn_tick_loop(
        shared,
        "tenant_rate_reset",
        TENANT_RATE_RESET_PERIOD,
        SharedState::reset_tenant_rate_counters,
    );
    spawn_audit_flush_loop(shared);
    spawn_tick_loop(
        shared,
        "tenant_memory_estimate",
        TENANT_MEMORY_PERIOD,
        SharedState::update_tenant_memory_estimates,
    );
}
