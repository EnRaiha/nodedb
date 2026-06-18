// SPDX-License-Identifier: BUSL-1.1

//! Idle maintenance loop: checkpoint coordinator, KV expiry wheel, idle
//! flush of timeseries memtables, and the periodic compaction trigger.
//!
//! Driven by the runtime event loop on every idle wake; rate-limited via
//! `compaction_interval` so the heavy `run_compaction` path runs at most
//! once per interval.

use tracing::info;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Run maintenance tasks if enough time has elapsed.
    ///
    /// Called from the runtime event loop on every idle wake. Tracks the
    /// last maintenance time internally and skips if the interval hasn't
    /// elapsed. Returns `true` if maintenance was executed.
    pub fn maybe_run_maintenance(&mut self) -> bool {
        // Checkpoint coordinator tick: incremental dirty page flushing.
        // Runs on its own interval (independent from compaction interval).
        let flush_plan = self.checkpoint_coordinator.tick();
        for (engine, pages) in &flush_plan {
            match engine.as_str() {
                "vector" => {
                    let flushed = self.checkpoint_vector_indexes();
                    self.checkpoint_coordinator
                        .record_flush("vector", flushed.min(*pages));
                }
                "crdt" => {
                    let flushed = self.checkpoint_crdt_engines();
                    self.checkpoint_coordinator
                        .record_flush("crdt", flushed.min(*pages));
                }
                "sparse" => {
                    // redb is ACID — writes are already durable.
                    self.checkpoint_coordinator.record_flush("sparse", *pages);
                }
                "timeseries" => {
                    // Idle flush: if no ingest for 5 seconds, flush all
                    // non-empty memtables so data becomes queryable.
                    let idle_threshold = std::time::Duration::from_secs(5);
                    let is_idle = self
                        .last_ts_ingest
                        .map(|t| t.elapsed() >= idle_threshold)
                        .unwrap_or(false);

                    if is_idle {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        let collections: Vec<(crate::types::TenantId, String)> = self
                            .columnar_memtables
                            .iter()
                            .filter(|(_, mt)| !mt.is_empty())
                            .map(|(k, _)| k.clone())
                            .collect();
                        let mut flushed = 0usize;
                        for (tid, collection) in &collections {
                            self.flush_ts_collection(*tid, collection, now_ms);
                            flushed += 1;
                        }
                        if flushed > 0 {
                            info!(
                                core = self.core_id,
                                flushed, "idle flush: timeseries memtables flushed"
                            );
                        }
                        // Reset so we don't re-flush until next ingest.
                        self.last_ts_ingest = None;
                        self.checkpoint_coordinator
                            .record_flush("timeseries", flushed.max(*pages));
                    } else {
                        self.checkpoint_coordinator
                            .record_flush("timeseries", *pages);
                    }
                }
                _ => {}
            }
        }
        if self.checkpoint_coordinator.is_clean()
            && !flush_plan.is_empty()
            && self.checkpoint_coordinator.total_dirty_pages() == 0
        {
            self.checkpoint_coordinator
                .complete_checkpoint(self.watermark.as_u64());
        }

        // KV expiry wheel tick: process expired keys on every maintenance call.
        // Bounded by the per-tick reap budget internally — safe for the reactor.
        // Expired keys are emitted as structured log events for CDC visibility.
        {
            let now_ms = crate::engine::kv::current_ms();
            let expired_keys = self.kv_engine.tick_expiry(now_ms);
            if !expired_keys.is_empty() {
                tracing::debug!(
                    core = self.core_id,
                    reaped = expired_keys.len(),
                    backlog = self.kv_engine.expiry_backlog(),
                    "kv expiry wheel tick"
                );

                for ek in &expired_keys {
                    info!(
                        target: "nodedb::kv::expired",
                        database_id = ek.database_id,
                        tenant_id = ek.tenant_id,
                        collection = %ek.collection,
                        key_len = ek.key.len(),
                        "kv key expired"
                    );
                }
            }
        }

        // Compaction: periodic tombstone removal + segment merge.
        let now = std::time::Instant::now();
        if let Some(last) = self.last_maintenance
            && now.duration_since(last) < self.compaction_interval
        {
            return !flush_plan.is_empty();
        }
        self.last_maintenance = Some(now);
        self.run_compaction(false);
        true
    }
}
