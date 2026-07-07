// SPDX-License-Identifier: BUSL-1.1

//! Background loop and subsystem spawning after SharedState is ready.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::ServerConfig;
use crate::control::state::SharedState;
use crate::event::bus::EventConsumerRx;
use crate::event::trigger::TriggerDlq;
use crate::event::watermark::WatermarkStore;
use crate::wal::WalManager;

/// Event Plane components passed to [`spawn_background_loops`].
pub struct EventPlaneComponents {
    pub wal: Arc<WalManager>,
    pub event_consumers: Vec<EventConsumerRx>,
    pub watermark_store: Arc<WatermarkStore>,
    pub trigger_dlq: Arc<std::sync::Mutex<TriggerDlq>>,
}

/// Enumerate mirror databases that need their observer link re-established
/// after a server restart, and log the restart decisions.
///
/// This is called once during startup, after [`SharedState`] and the catalog
/// are fully open. Databases with `MirrorStatus::Promoted` are excluded:
/// they are normal writable databases and must NOT attempt to reconnect.
/// The actual link objects are created by the cluster layer when it
/// processes each restart decision; this function only reads the catalog
/// and logs.
pub fn log_mirror_restart_decisions(shared: &Arc<crate::control::state::SharedState>) {
    let catalog = shared.credentials.catalog();
    match crate::control::mirror::enumerate_resumable_mirrors(catalog) {
        Ok(decisions) => {
            for d in &decisions {
                tracing::info!(
                    database = %d.database_name,
                    resume_lsn = d.resume_from_lsn,
                    needs_bootstrap = d.needs_bootstrap,
                    "mirror restart: observer link will resume"
                );
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "mirror restart: failed to enumerate mirrors; skipping");
        }
    }
}

/// Spawn all persistent background subsystems.
///
/// Includes: Event Plane consumers, event trigger processor, webhook manager wiring,
/// collection GC, L2 cleanup, tenant rate/audit/memory timers, checkpoint manager,
/// usage metering flush, and cold tier task.
///
/// Returns the [`EventPlane`] handle. The caller MUST hold this for the
/// server's lifetime — dropping it aborts every consumer task and turns
/// the per-core event ring buffers into one-way drains, which silently
/// loses every WriteEvent emitted by the Data Plane until process exit.
#[must_use = "EventPlane must be held for the server's lifetime; dropping it stops all event consumers"]
pub fn spawn_background_loops(
    shared: &Arc<SharedState>,
    components: EventPlaneComponents,
    config: &ServerConfig,
    num_cores: usize,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> crate::event::EventPlane {
    let EventPlaneComponents {
        wal,
        event_consumers,
        watermark_store,
        trigger_dlq,
    } = components;
    // Mirror restart: enumerate databases that need observer links re-established
    // and log the decisions. The cluster layer processes these asynchronously
    // via the mirror_link_registry once QUIC transport is available.
    log_mirror_restart_decisions(shared);

    // Event trigger processor.
    crate::control::event_trigger::spawn_event_trigger_processor(Arc::clone(shared));

    // Mirror lag monitor (5-second interval).
    // Reads `_system.mirror_lag` for every active mirror and updates
    // the `nodedb_database_mirror_lag_ms` metric. Also drives status
    // transitions (Following → Degraded → Disconnected) and clears the
    // metric when a mirror is promoted.
    {
        let shared_mirror = Arc::clone(shared);
        crate::control::shutdown::spawn_loop(
            &shared.loop_registry,
            &shared.shutdown,
            "mirror_lag_monitor",
            move |mut shutdown| async move {
                let mut tick = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = shutdown.wait_cancelled() => break,
                        _ = tick.tick() => {}
                    }
                    if shutdown.is_cancelled() {
                        break;
                    }
                    let catalog = shared_mirror.credentials.catalog();
                    let databases = match catalog.list_databases() {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::warn!(error = %e, "mirror_lag_monitor: catalog list error");
                            continue;
                        }
                    };
                    for db in databases {
                        let origin = match db.mirror_origin.as_ref() {
                            Some(o) => o,
                            None => continue,
                        };
                        // Promoted mirrors are normal writable databases — skip.
                        if matches!(origin.status, nodedb_types::MirrorStatus::Promoted) {
                            continue;
                        }
                        // Read the real receive timestamp from the link registry.
                        // `None` means no link is registered for this database (the
                        // cluster layer has not yet (re)established it after restart);
                        // `update_lag_status` falls back to the catalog's apply time
                        // in that case so the disconnect timer still advances.
                        let last_received =
                            shared_mirror.mirror_link_registry.last_received_ms(db.id);
                        crate::control::mirror::update_lag_status(
                            catalog,
                            db.id,
                            &db.name,
                            &origin.status,
                            last_received,
                            false,
                            &shared_mirror.database_metrics,
                        );
                    }
                }
            },
        );
        info!("mirror lag monitor running");
    }

    // Wire webhook manager.
    shared.webhook_manager.set_state(Arc::clone(shared));

    // Event Plane: one consumer Tokio task per Data Plane core.
    // Returned to the caller — must outlive the server, otherwise its
    // Drop impl aborts every consumer and the Data Plane producers
    // start dropping every WriteEvent they emit.
    let event_plane = crate::event::EventPlane::spawn(
        event_consumers,
        Arc::clone(&wal),
        watermark_store,
        Arc::clone(shared),
        trigger_dlq,
        Arc::clone(&shared.cdc_router),
        Arc::clone(&shared.shutdown),
    );
    info!(num_cores, "event plane running");

    // Collection hard-delete retention GC.
    if let Ok(mut w) = shared.retention_settings.write() {
        *w = config.retention.clone();
    }
    let _collection_gc = crate::event::collection_gc::spawn_collection_gc(Arc::clone(shared));
    info!(
        retention_days = config.retention.deactivated_collection_retention_days,
        sweep_interval_secs = config.retention.gc_sweep_interval_secs,
        "collection-gc sweeper running"
    );

    // L2 cleanup worker.
    let _l2_cleanup = crate::event::collection_gc::spawn_l2_cleanup(Arc::clone(shared));

    // Pending engine-reclaim worker: retries the redb + versioned engine
    // purge for any dropped collection whose result-checked purge failed
    // on this node, so a per-node hiccup can never leave engine storage
    // rows behind a removed catalog row.
    let _pending_reclaim = crate::event::collection_gc::spawn_pending_reclaim(Arc::clone(shared));

    // Tenant rate counter reset (1-second timer).
    let shared_rate = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "tenant_rate_reset",
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => shared_rate.reset_tenant_rate_counters(),
                }
            }
        },
    );

    // Idle session sweep (5-second timer).
    // Closes sessions whose idle timeout or OIDC token expiry has elapsed.
    crate::control::security::sessions::spawn_idle_sweep_loop(shared);
    info!("idle session sweep loop running");

    // Audit log flush (10-second timer).
    let shared_audit = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "audit_log_flush",
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => shared_audit.flush_audit_log(),
                }
            }
        },
    );

    // Tenant memory estimation (30-second timer).
    let shared_mem = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "tenant_memory_estimate",
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => shared_mem.update_tenant_memory_estimates(),
                }
            }
        },
    );

    // Checkpoint manager.
    let shared_ckpt = Arc::clone(shared);
    let shutdown_rx_ckpt = shutdown_rx.clone();
    crate::control::checkpoint_manager::spawn_checkpoint_task(
        shared_ckpt,
        num_cores,
        config.checkpoint.to_manager_config(),
        shutdown_rx_ckpt,
    );

    // Usage metering flush.
    let _metering_flush = crate::control::security::metering::counter::spawn_flush_task(
        Arc::clone(&shared.usage_counter),
        Arc::clone(&shared.usage_store),
        60,
    );

    // Background clone materializer sweep.
    // Automatically progresses cloned collections from Shadowed → Materialized
    // without requiring explicit DDL.  The foreground ALTER DATABASE MATERIALIZE
    // and DROP DATABASE FORCE paths bypass this loop and call the blocking
    // materializer directly.
    {
        let shared_sweep = Arc::clone(shared);
        let sweep_ms = std::env::var("NODEDB_CLONE_SWEEP_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30_000);
        let sweep_interval = Duration::from_millis(sweep_ms);
        crate::control::shutdown::spawn_loop(
            &shared.loop_registry,
            &shared.shutdown,
            "clone_materializer_sweep",
            move |mut shutdown| async move {
                let mut tick = tokio::time::interval(sweep_interval);
                loop {
                    tokio::select! {
                        _ = shutdown.wait_cancelled() => break,
                        _ = tick.tick() => {}
                    }
                    if shutdown.is_cancelled() {
                        break;
                    }
                    let state_for_sweep = Arc::clone(&shared_sweep);
                    let result = tokio::task::spawn_blocking(move || {
                        let catalog = state_for_sweep.credentials.catalog();
                        let cancel = std::sync::atomic::AtomicBool::new(false);
                        if let Err(e) =
                            crate::control::maintenance::clone_materializer::run_scheduled_sweep(
                                &state_for_sweep,
                                catalog,
                                &cancel,
                            )
                        {
                            tracing::warn!(error = %e, "clone materializer sweep error");
                        }
                    })
                    .await;
                    if let Err(e) = result {
                        tracing::warn!(error = %e, "clone materializer sweep task panicked");
                    }
                }
            },
        );
        info!(
            interval_ms = sweep_ms,
            "clone materializer background sweep running"
        );
    }

    // CRDT constraint reconcile (1-second timer, leader-gated).
    // The metadata leader re-derives each collection's constraint set from the
    // catalog and replicates it to every data-group replica's CRDT validator,
    // so a collection created/altered under any leader converges everywhere.
    crate::bootstrap::constraint_reconcile::spawn_constraint_reconcile(Arc::clone(shared));
    info!("constraint reconcile loop running");

    // Cold tier task (if configured).
    if let Some(ref cold_settings) = config.cold_storage {
        let shared_cold = Arc::clone(shared);
        let cold_settings_clone = cold_settings.clone();
        let data_dir_clone = config.server.data_dir.clone();
        let shutdown_rx_cold = shutdown_rx.clone();
        crate::control::cold_tier::spawn_cold_tier_task(
            shared_cold,
            cold_settings_clone,
            data_dir_clone,
            shutdown_rx_cold,
        );
        info!("cold tier task spawned");
    }

    event_plane
}

/// Spawn the response poller loop (routes Data Plane responses to waiting sessions).
pub fn spawn_response_poller(shared: &Arc<SharedState>) {
    let shared_poller = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "response_poller",
        move |shutdown| async move {
            let mut idle_iters: u32 = 0;
            loop {
                if shutdown.is_cancelled() {
                    break;
                }
                let routed = shared_poller.poll_and_route_responses();
                if routed > 0 {
                    idle_iters = 0;
                    tokio::task::yield_now().await;
                    continue;
                }
                idle_iters = idle_iters.saturating_add(1);
                if idle_iters <= 256 {
                    tokio::task::yield_now().await;
                } else if idle_iters <= 1024 {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        },
    );
}
