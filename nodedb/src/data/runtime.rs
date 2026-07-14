// SPDX-License-Identifier: BUSL-1.1

//! TPC (Thread-per-Core) runtime for Data Plane cores.
//!
//! Replaces the naive `sleep(50µs)` busy-poll with an eventfd-driven wake
//! mechanism. Each core thread:
//!
//! 1. Pins itself to a dedicated jemalloc arena (zero allocator contention).
//! 2. Blocks on `libc::poll(eventfd)` when idle (zero CPU waste).
//! 3. Wakes instantly when the Control Plane signals via `EventFdNotifier`.
//! 4. Processes all pending requests in a tight loop, then re-parks.
//!
//! # Panic Isolation
//!
//! Every `core.tick()` invocation is wrapped in `catch_unwind`. A panic in
//! any engine execution (bad index, arithmetic overflow, corrupted data)
//! is caught without killing the core thread. The faulting request receives
//! an `INTERNAL_ERROR` response, and the core continues serving subsequent
//! requests. A health watchdog tracks consecutive panics: if the threshold
//! is exceeded, the core stops accepting new work and logs an alert.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

use tracing::{error, info, warn};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::core_health::{CoreHealthWatchdog, MAX_CONSECUTIVE_PANICS, PANIC_WINDOW_SECS};
use crate::data::eventfd::{EventFd, EventFdNotifier};
use crate::data::executor::core_loop::CoreLoop;

use nodedb_bridge::buffer::{Consumer, Producer};

/// Maximum idle poll timeout in milliseconds.
///
/// Even without signals, cores wake periodically to run maintenance
/// (e.g., deferred retry polling, metrics flush).
const IDLE_POLL_TIMEOUT_MS: i32 = 100;

/// Compaction configuration passed to each Data Plane core.
#[derive(Debug, Clone)]
pub struct CoreCompactionConfig {
    /// How often to run automatic compaction.
    pub interval: std::time::Duration,
    /// Tombstone ratio threshold for auto-compaction.
    pub tombstone_threshold: f64,
    /// Query execution tuning parameters.
    pub query: nodedb_types::config::tuning::QueryTuning,
    /// Graph engine tuning (traversal limits + variable-length expansion caps).
    pub graph: nodedb_types::config::tuning::GraphTuning,
}

impl Default for CoreCompactionConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(600),
            tombstone_threshold: 0.2,
            query: nodedb_types::config::tuning::QueryTuning::default(),
            graph: nodedb_types::config::tuning::GraphTuning::default(),
        }
    }
}

/// Parameters for [`spawn_core`].
pub struct SpawnCoreParams<'a> {
    pub core_id: usize,
    pub request_rx: Consumer<BridgeRequest>,
    pub response_tx: Producer<BridgeResponse>,
    pub data_dir: &'a Path,
    pub wal_records: Arc<[nodedb_wal::WalRecord]>,
    pub tombstones: nodedb_wal::TombstoneSet,
    pub num_cores: usize,
    pub compaction_config: CoreCompactionConfig,
    pub system_metrics: Option<Arc<crate::control::metrics::SystemMetrics>>,
    pub event_producer: Option<crate::event::bus::EventProducer>,
    pub governor: Arc<nodedb_mem::MemoryGovernor>,
    pub quiesce: Option<Arc<crate::bridge::quiesce::CollectionQuiesce>>,
    pub hlc: Arc<nodedb_types::OrdinalClock>,
    pub array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
    pub quarantine_registry: Arc<crate::storage::quarantine::QuarantineRegistry>,
    pub maintenance_budget: Arc<crate::control::maintenance::MaintenanceBudgetTracker>,
}

/// Spawn a Data Plane core on a dedicated OS thread with TPC isolation.
///
/// Returns the `JoinHandle` and the `EventFdNotifier` that the Control Plane
/// uses to wake this core after pushing a request into the SPSC queue.
///
/// If `wal_records` is non-empty, the core replays vector WAL records
/// during startup (before entering the event loop) to rebuild HNSW indexes.
pub fn spawn_core(
    params: SpawnCoreParams<'_>,
) -> std::io::Result<(JoinHandle<()>, EventFdNotifier)> {
    let SpawnCoreParams {
        core_id,
        request_rx,
        response_tx,
        data_dir,
        wal_records,
        tombstones,
        num_cores,
        compaction_config,
        system_metrics,
        event_producer,
        governor,
        quiesce,
        hlc,
        array_catalog,
        quarantine_registry,
        maintenance_budget,
    } = params;

    let data_dir = data_dir.to_path_buf();

    // Create eventfd and extract notifier before moving EventFd to core thread.
    let efd = EventFd::new().map_err(std::io::Error::other)?;
    let notifier = efd.notifier();

    let handle = std::thread::Builder::new()
        .name(format!("data-core-{core_id}"))
        .spawn(move || {
            // 1. Pin to dedicated jemalloc arena.
            match nodedb_mem::arena::pin_thread_arena(core_id as u32) {
                Ok(arena) => info!(core_id, arena, "pinned to jemalloc arena"),
                Err(e) => warn!(core_id, error = %e, "failed to pin jemalloc arena, continuing with default"),
            }

            // 2. Open engines.
            let mut core = CoreLoop::open_with_array_catalog(
                core_id,
                request_rx,
                response_tx,
                &data_dir,
                hlc,
                array_catalog,
            )
            .expect("failed to open CoreLoop engines");

            // 2b. Apply memory governor.
            core.set_governor(governor);
            core.set_maintenance_budget(maintenance_budget);

            // 2b. Apply metrics reference.
            if let Some(m) = system_metrics {
                core.set_metrics(m);
            }

            // 2b. Wire Event Plane producer (Data Plane → Event Plane).
            if let Some(ep) = event_producer {
                core.set_event_producer(ep);
            }

            // 2b. Wire the shared scan-quiesce registry so scan
            // handlers can refuse new scans against a draining
            // collection (prerequisite for the safe hard-delete
            // unlink ordering).
            if let Some(q) = quiesce {
                core.set_quiesce(q);
            }

            // 2b. Wire the quarantine registry for corrupt-segment detection.
            core.set_quarantine_registry(quarantine_registry);

            // 2c. Apply compaction config.
            core.set_compaction_config(
                compaction_config.interval,
                compaction_config.tombstone_threshold,
            );

            // 2c. Apply query tuning config.
            core.set_query_tuning(compaction_config.query);

            // 2d. Apply graph engine tuning (traversal limits + varlen caps).
            core.set_graph_tuning(compaction_config.graph);

            // 3. Load vector + spatial + sparse vector checkpoints (fast recovery).
            core.load_vector_checkpoints();
            core.load_spatial_checkpoints();
            core.load_sparse_vector_checkpoints();
            core.load_crdt_checkpoints();

            // 4. Replay WAL records for crash recovery.
            //
            // Tombstones are pre-built by the caller from
            // (persisted `_system.wal_tombstones` ∪
            // `extract_tombstones(&wal_records)`). The persisted half
            // is load-bearing once segment-truncation advances past a
            // tombstone record: the tombstone falls out of the live
            // WAL, but shadowed writes in un-truncated older segments
            // must still be skipped. Every per-engine replay method
            // consults the merged set.
            core.replay_all_wal(&wal_records, num_cores, &tombstones);

            info!(core_id, "data plane core started (eventfd-driven)");

            let mut watchdog = CoreHealthWatchdog::new();
            let mut last_checkpoint = Instant::now();
            let mut last_event_emit = Instant::now();
            let mut heartbeat_interval = heartbeat_interval_with_jitter();
            /// Checkpoint interval: 5 minutes.
            const CHECKPOINT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

            /// Maximum requests to process per event loop iteration before
            /// yielding to maintenance tasks. Prevents maintenance starvation
            /// under sustained high write load.
            const MAX_TASKS_PER_ITERATION: usize = 256;

            // 5. Event loop: poll → drain → tick → checkpoint → repeat.
            loop {
                // Block until signaled or timeout.
                efd.poll_wait(IDLE_POLL_TIMEOUT_MS);

                // Drain all accumulated signals.
                while efd.drain() > 0 {}

                // If degraded, drain and reject all pending requests.
                if watchdog.is_degraded() {
                    drain_and_reject(&mut core, core_id);
                    continue;
                }

                // Process pending requests with panic isolation.
                // Bounded to MAX_TASKS_PER_ITERATION to prevent maintenance
                // starvation under sustained high load.
                let mut tasks_processed = 0usize;
                loop {
                    // catch_unwind requires FnOnce: &mut core is not UnwindSafe
                    // by default, but we explicitly opt in. The CoreLoop state
                    // may be partially inconsistent after a panic (e.g., a
                    // half-inserted HNSW node), but:
                    //   - Reads are safe: stale or partial data is better than dead core.
                    //   - Writes: the WAL ensures crash consistency on replay;
                    //     a panicked write was never acknowledged to the client.
                    //   - The watchdog degrades the core before repeated panics
                    //     can compound corruption.
                    let result =
                        catch_unwind(AssertUnwindSafe(|| core.tick()));

                    match result {
                        Ok(0) => break, // No more pending requests.
                        Ok(_) => {
                            watchdog.record_success();
                            tasks_processed += 1;
                            if tasks_processed >= MAX_TASKS_PER_ITERATION {
                                break; // Yield to maintenance.
                            }
                        }
                        Err(panic_payload) => {
                            // Extract panic message for logging.
                            let msg = panic_message(&panic_payload);
                            error!(
                                core_id,
                                panic_count = watchdog.consecutive_panics + 1,
                                message = %msg,
                                "data plane core caught panic during tick"
                            );

                            let is_degraded = watchdog.record_panic();
                            if is_degraded {
                                error!(
                                    core_id,
                                    threshold = MAX_CONSECUTIVE_PANICS,
                                    window_secs = PANIC_WINDOW_SECS,
                                    "core entered DEGRADED mode — rejecting all requests"
                                );
                                drain_and_reject(&mut core, core_id);
                            }

                            // The panicked tick may have consumed a request from
                            // the queue without sending a response. The in-flight
                            // request's oneshot channel in RequestTracker will
                            // time out on the Control Plane side (deadline expiry),
                            // which is the correct behavior — the client sees
                            // DEADLINE_EXCEEDED rather than hanging forever.

                            break; // Exit inner loop; re-enter poll_wait.
                        }
                    }
                }

                // Periodic vector + sparse vector checkpoint (when idle and interval elapsed).
                if last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
                    core.checkpoint_vector_indexes();
                    core.checkpoint_sparse_vector_indexes();
                    last_checkpoint = Instant::now();
                }

                // Periodic compaction + maintenance (tombstone cleanup, CSR compact, edge sweep).
                core.maybe_run_maintenance();

                // Heartbeat: if no user writes for ~1 second (±100ms jitter),
                // emit a heartbeat to advance the Event Plane's partition
                // watermark. Without this, streaming MV global_watermark()
                // stalls on idle partitions. Jitter prevents multi-core
                // thundering herd when all cores go idle simultaneously.
                if tasks_processed > 0 {
                    last_event_emit = Instant::now();
                } else if last_event_emit.elapsed() >= heartbeat_interval {
                    core.emit_heartbeat();
                    last_event_emit = Instant::now();
                    heartbeat_interval = heartbeat_interval_with_jitter();
                }
            }
        })?;

    Ok((handle, notifier))
}

/// Drain all pending requests from a core's SPSC queue and send back
/// `CoreDegraded` error responses. Used when the watchdog has flagged
/// the core as unhealthy.
fn drain_and_reject(core: &mut CoreLoop, core_id: usize) {
    core.drain_requests();
    while let Some(task) = core.task_queue.pop_front() {
        let response = Response {
            request_id: task.request_id(),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: core.watermark,
            error_code: Some(Box::new(ErrorCode::Internal {
                detail: format!("core-{core_id} is degraded after repeated panics"),
            })),
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        };
        if let Err(e) = core
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(core_id, error = %e, "failed to send degraded-rejection response");
        }
    }
}

/// Compute heartbeat interval with ±100ms jitter.
///
/// Returns a Duration in the range [900ms, 1100ms]. The jitter spreads
/// heartbeat emissions across cores so they don't all fire in the same
/// poll iteration when the system goes idle.
///
/// Uses a fast splitmix64-style hash of the current timestamp nanos to
/// produce pseudo-random jitter without requiring the `rand` crate in
/// production code (it's dev-only).
fn heartbeat_interval_with_jitter() -> std::time::Duration {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    // splitmix64
    let mut x = seed;
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    // Map to [0, 200] → offset by -100 → [-100, +100] ms.
    let jitter_ms = (x % 201) as i64 - 100;
    std::time::Duration::from_millis((1000 + jitter_ms) as u64)
}

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
