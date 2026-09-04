// SPDX-License-Identifier: BUSL-1.1

//! The checkpoint manager's background task: a periodic cycle plus one final
//! cycle at shutdown.

use std::sync::Arc;

use tracing::{info, warn};

use super::checkpoint_manager::{
    CheckpointCycleInputs, CheckpointManagerConfig, run_checkpoint_cycle,
};

/// Spawn the checkpoint manager as a background Tokio task.
///
/// Runs `run_checkpoint_cycle` at the configured interval, and one final cycle
/// when the shutdown bus enters `DrainingControlPlane`.
///
/// The phase is load-bearing. The final cycle dispatches a checkpoint request
/// to every Data Plane core and waits for the answers, so it must complete
/// before `DrainingDataPlane` closes the enqueue gate. Registering it as a
/// critical task at the preceding phase is what orders the two: the bus cannot
/// enter the Data Plane drain until this task reports drained.
pub fn spawn_checkpoint_task(
    shared: Arc<crate::control::state::SharedState>,
    watermark_store: Arc<crate::event::watermark::WatermarkStore>,
    num_cores: usize,
    config: CheckpointManagerConfig,
    shutdown_bus: &crate::control::shutdown::ShutdownBus,
) -> tokio::task::JoinHandle<()> {
    let mut guard = shutdown_bus.register_critical_task(
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        "checkpoint_manager::final",
    );
    tokio::spawn(async move {
        info!(
            interval_secs = config.interval.as_secs(),
            "checkpoint manager started"
        );

        loop {
            let mut draining = false;
            tokio::select! {
                _ = tokio::time::sleep(config.interval) => {}
                _ = guard.await_signal() => { draining = true; }
            }

            if draining {
                info!("shutdown: running final checkpoint");
                // Bounded by the shutdown deadline, not by the steady-state
                // per-core timeout. This cycle is a phase barrier, so a core
                // that never answers costs the shutdown its deadline and no more.
                let budget = shared.tuning.shutdown.deadline();
                let cycle = run_checkpoint_cycle(CheckpointCycleInputs {
                    dispatcher: &shared.dispatcher,
                    tracker: &shared.tracker,
                    wal: &shared.wal,
                    watermark_store: &watermark_store,
                    num_cores,
                    timeout: budget,
                    cold_storage: shared.cold_storage.clone(),
                    catalog: Some(shared.credentials.catalog()),
                });
                if tokio::time::timeout(budget, cycle).await.is_err() {
                    warn!(
                        budget_ms = budget.as_millis() as u64,
                        "final checkpoint exceeded the shutdown deadline — the WAL suffix \
                         it would have made redundant is replayed on restart"
                    );
                }
                guard.report_drained();
                info!("checkpoint manager stopped");
                return;
            }

            run_checkpoint_cycle(CheckpointCycleInputs {
                dispatcher: &shared.dispatcher,
                tracker: &shared.tracker,
                wal: &shared.wal,
                watermark_store: &watermark_store,
                num_cores,
                timeout: config.core_timeout,
                cold_storage: shared.cold_storage.clone(),
                catalog: Some(shared.credentials.catalog()),
            })
            .await;
        }
    })
}
