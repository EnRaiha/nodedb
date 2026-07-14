// SPDX-License-Identifier: BUSL-1.1

//! Memory-pressure awareness for the Data Plane core loop.
//!
//! Two concerns live here:
//!
//! 1. **Per-handler pressure check** (`check_engine_pressure`): called at the top
//!    of every write handler. On Critical it increments the metric counter and
//!    proceeds (flush semantics are engine-specific and handled in the handler
//!    itself). On Emergency it increments the metric counter and returns a typed
//!    `Backpressure` error so the caller can surface SQLSTATE 53200.
//!
//! 2. **Core-loop SPSC throttle** (`apply_spsc_pressure`): called once per tick
//!    iteration. On Critical it halves `spsc_read_depth` (floor 1). On Emergency
//!    it sets `pressure_suspend_reads = true`. Hysteresis: restores
//!    `spsc_read_depth` only after `PRESSURE_NORMAL_HYSTERESIS` consecutive
//!    Normal/Warning ticks.

use nodedb_mem::{EngineId, PressureLevel};
use tracing::warn;

use super::CoreLoop;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::task::ExecutionTask;

/// Default SPSC drain batch size when pressure is Normal.
pub const SPSC_READ_DEPTH_NORMAL: usize = 64;

/// Number of consecutive Normal/Warning ticks required before
/// `spsc_read_depth` is restored to `SPSC_READ_DEPTH_NORMAL`.
const PRESSURE_NORMAL_HYSTERESIS: u32 = 8;

impl CoreLoop {
    /// Check engine-level memory pressure at the start of a write handler.
    ///
    /// - `Normal` / `Warning`: returns `None` — proceed normally.
    /// - `Critical`: increments the critical metric counter, returns `None`
    ///   (handler proceeds; engine-specific flush is the handler's own
    ///   responsibility — see timeseries ingest for the pattern).
    /// - `Emergency`: increments the emergency metric counter, returns
    ///   `Some(Response)` with `ErrorCode::ResourcesExhausted`. The caller
    ///   must return this response immediately without executing the write.
    pub fn check_engine_pressure(
        &self,
        task: &ExecutionTask,
        engine: EngineId,
    ) -> Option<Response> {
        let governor = self.governor.as_ref()?;
        let pressure = governor.engine_pressure(engine);
        match pressure {
            PressureLevel::Normal | PressureLevel::Warning => None,
            PressureLevel::Critical => {
                if let Some(ref m) = self.metrics {
                    m.record_backpressure_critical(&engine.to_string());
                }
                warn!(
                    core = self.core_id,
                    engine = %engine,
                    "Critical memory pressure — proceeding with engine-specific flush"
                );
                None
            }
            PressureLevel::Emergency => {
                if let Some(ref m) = self.metrics {
                    m.record_backpressure_emergency(&engine.to_string());
                }
                warn!(
                    core = self.core_id,
                    engine = %engine,
                    "Emergency memory pressure — rejecting write with backpressure"
                );
                Some(self.response_error(task, ErrorCode::ResourcesExhausted))
            }
        }
    }

    /// Adjust SPSC read depth based on worst-case pressure across all engines.
    ///
    /// Called once per tick. Mutates `spsc_read_depth`, `pressure_suspend_reads`,
    /// and `pressure_normal_ticks`.
    pub fn apply_spsc_pressure(&mut self) {
        let Some(ref governor) = self.governor else {
            return;
        };

        // Worst-case pressure across all engines that have a budget.
        let worst = governor.worst_engine_pressure();

        match worst {
            PressureLevel::Normal | PressureLevel::Warning => {
                self.pressure_normal_ticks = self.pressure_normal_ticks.saturating_add(1);
                if self.pressure_suspend_reads
                    && self.pressure_normal_ticks >= PRESSURE_NORMAL_HYSTERESIS
                {
                    // Emergency cleared — lift suspension first.
                    self.pressure_suspend_reads = false;
                    warn!(
                        core = self.core_id,
                        "pressure cleared — resuming SPSC reads"
                    );
                }
                if self.spsc_read_depth < SPSC_READ_DEPTH_NORMAL
                    && self.pressure_normal_ticks >= PRESSURE_NORMAL_HYSTERESIS
                {
                    self.spsc_read_depth = SPSC_READ_DEPTH_NORMAL;
                    warn!(
                        core = self.core_id,
                        read_depth = self.spsc_read_depth,
                        "pressure normal — restored SPSC read depth"
                    );
                }
            }
            PressureLevel::Critical => {
                self.pressure_normal_ticks = 0;
                if self.pressure_suspend_reads {
                    // Coming down from Emergency: lift suspension, set throttled depth.
                    self.pressure_suspend_reads = false;
                    warn!(
                        core = self.core_id,
                        "pressure Critical — lifting SPSC suspension"
                    );
                }
                let new_depth = (self.spsc_read_depth / 2).max(1);
                if new_depth != self.spsc_read_depth {
                    self.spsc_read_depth = new_depth;
                    warn!(
                        core = self.core_id,
                        read_depth = new_depth,
                        "Critical memory pressure — reduced SPSC read depth"
                    );
                }
            }
            PressureLevel::Emergency => {
                self.pressure_normal_ticks = 0;
                if !self.pressure_suspend_reads {
                    self.pressure_suspend_reads = true;
                    self.spsc_read_depth = 1;
                    warn!(
                        core = self.core_id,
                        "Emergency memory pressure — suspending new SPSC reads"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_mem::{EngineId, GovernorConfig, MemoryGovernor};

    use super::*;
    use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Priority, Request};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::*;
    use nodedb_physical::physical_plan::VectorOp;
    use nodedb_types::Surrogate;

    fn make_core() -> (
        CoreLoop,
        nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (req_tx, req_rx) = RingBuffer::channel::<crate::bridge::dispatch::BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<crate::bridge::dispatch::BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .unwrap();
        (core, req_tx, resp_rx, dir)
    }

    /// Build a governor with every engine having `budget_bytes`, then
    /// pre-fill `engine` to `utilization_percent`.
    fn make_governor_at(engine: EngineId, utilization_percent: u8) -> Arc<MemoryGovernor> {
        let budget_bytes: usize = 10_000;
        let mut engine_limits = HashMap::new();
        for id in EngineId::ALL {
            engine_limits.insert(*id, budget_bytes);
        }
        let global_ceiling = budget_bytes * EngineId::ALL.len() * 2;
        let gov = MemoryGovernor::new(GovernorConfig {
            global_ceiling,
            engine_limits,
        })
        .unwrap();
        let fill = (budget_bytes as u64 * utilization_percent as u64 / 100) as usize;
        if fill > 0 {
            // Intentionally leak the token so the engine budget stays allocated
            // for the lifetime of the test governor. Test-only pattern.
            if let Ok(tok) = gov.try_reserve(DatabaseId::DEFAULT, TenantId::new(1), engine, fill) {
                std::mem::forget(tok);
            }
        }
        Arc::new(gov)
    }

    fn make_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Vector(VectorOp::Insert {
                collection: "test".into(),
                vector: vec![0.1],
                dim: 1,
                field_name: "emb".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: None,
                provenance: None,
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    #[test]
    fn no_governor_always_allows() {
        let (core, _tx, _rx, _dir) = make_core();
        let task = make_task();
        assert!(
            core.check_engine_pressure(&task, EngineId::Vector)
                .is_none(),
            "no governor: must always allow"
        );
    }

    #[test]
    fn normal_pressure_allows() {
        let (mut core, _tx, _rx, _dir) = make_core();
        core.set_governor(make_governor_at(EngineId::Vector, 0));
        let task = make_task();
        assert!(
            core.check_engine_pressure(&task, EngineId::Vector)
                .is_none()
        );
    }

    #[test]
    fn warning_pressure_allows() {
        let (mut core, _tx, _rx, _dir) = make_core();
        core.set_governor(make_governor_at(EngineId::Vector, 75));
        let task = make_task();
        assert!(
            core.check_engine_pressure(&task, EngineId::Vector)
                .is_none()
        );
    }

    #[test]
    fn critical_pressure_allows_and_increments_metric() {
        let (mut core, _tx, _rx, _dir) = make_core();
        let metrics = Arc::new(crate::control::metrics::SystemMetrics::new());
        core.set_metrics(metrics.clone());
        core.set_governor(make_governor_at(EngineId::Vector, 88));
        let task = make_task();
        let result = core.check_engine_pressure(&task, EngineId::Vector);
        assert!(result.is_none(), "Critical must allow (None)");
        let m = metrics.backpressure_critical_by_engine.read().unwrap();
        assert_eq!(m.get("vector").copied().unwrap_or(0), 1);
    }

    #[test]
    fn emergency_pressure_rejects_and_increments_metric() {
        let (mut core, _tx, _rx, _dir) = make_core();
        let metrics = Arc::new(crate::control::metrics::SystemMetrics::new());
        core.set_metrics(metrics.clone());
        core.set_governor(make_governor_at(EngineId::Vector, 97));
        let task = make_task();
        let result = core.check_engine_pressure(&task, EngineId::Vector);
        assert!(result.is_some(), "Emergency must reject (Some)");
        assert_eq!(
            result.unwrap().error_code.as_deref(),
            Some(&ErrorCode::ResourcesExhausted)
        );
        let m = metrics.backpressure_emergency_by_engine.read().unwrap();
        assert_eq!(m.get("vector").copied().unwrap_or(0), 1);
    }

    // ── SPSC throttle ──

    #[test]
    fn spsc_emergency_sets_suspend_flag() {
        let (mut core, _tx, _rx, _dir) = make_core();
        // Fill Vector to Emergency; all other engines at 0 — worst = Emergency.
        core.set_governor(make_governor_at(EngineId::Vector, 97));
        core.apply_spsc_pressure();
        assert!(
            core.pressure_suspend_reads,
            "Emergency must set suspend flag"
        );
    }

    #[test]
    fn spsc_emergency_suspension_clears_after_sustained_normal() {
        // A core that entered Emergency suspend because the governor
        // *reported* exhaustion must lift the suspension once the
        // governor's worst-case pressure is back to Normal — otherwise a
        // transient (or bogus, e.g. accounting-drift) Emergency wedges the
        // core's SPSC reads forever, and every DDL waiting on that core's
        // schema-register ACK times out. `pressure_normal_ticks` is the
        // hysteresis gate; the suspend flag must be cleared once it
        // crosses the threshold.
        let (mut core, _tx, _rx, _dir) = make_core();
        core.set_governor(make_governor_at(EngineId::Vector, 97));
        core.apply_spsc_pressure();
        assert!(
            core.pressure_suspend_reads,
            "Emergency must enter the suspended state first"
        );

        // Governor pressure returns to Normal (a fresh governor at 0%).
        core.set_governor(make_governor_at(EngineId::Vector, 0));
        for _ in 0..(PRESSURE_NORMAL_HYSTERESIS - 1) {
            core.apply_spsc_pressure();
        }
        assert!(
            core.pressure_suspend_reads,
            "must not lift suspension before the hysteresis threshold"
        );
        core.apply_spsc_pressure();
        assert!(
            !core.pressure_suspend_reads,
            "after {PRESSURE_NORMAL_HYSTERESIS} consecutive Normal ticks the \
             Emergency suspension must be lifted — a stuck suspend flag is the \
             cascade that turns transient or accounting-drift memory pressure \
             into permanent schema-register barrier timeouts and a server that \
             answers /healthz but fails every DDL"
        );
    }

    #[test]
    fn spsc_critical_halves_depth() {
        let (mut core, _tx, _rx, _dir) = make_core();
        core.spsc_read_depth = SPSC_READ_DEPTH_NORMAL;
        core.set_governor(make_governor_at(EngineId::Vector, 88));
        core.apply_spsc_pressure();
        assert_eq!(
            core.spsc_read_depth,
            SPSC_READ_DEPTH_NORMAL / 2,
            "Critical must halve depth"
        );
    }

    #[test]
    fn spsc_hysteresis_restores_depth_after_n_normal_ticks() {
        let (mut core, _tx, _rx, _dir) = make_core();
        core.spsc_read_depth = SPSC_READ_DEPTH_NORMAL / 2;
        core.pressure_normal_ticks = 0;
        // Normal governor (0% on all engines).
        core.set_governor(make_governor_at(EngineId::Vector, 0));
        // 7 ticks — must NOT restore.
        for _ in 0..7 {
            core.apply_spsc_pressure();
        }
        assert!(
            core.spsc_read_depth < SPSC_READ_DEPTH_NORMAL,
            "Must not restore before hysteresis threshold"
        );
        // 8th tick — must restore.
        core.apply_spsc_pressure();
        assert_eq!(
            core.spsc_read_depth, SPSC_READ_DEPTH_NORMAL,
            "Must restore after hysteresis threshold"
        );
    }

    #[test]
    fn spsc_suspend_drain_is_noop() {
        let (mut core, mut tx, _rx, _dir) = make_core();
        use crate::bridge::dispatch::BridgeRequest;
        // Push a request.
        let req = Request {
            request_id: RequestId::new(2),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Vector(VectorOp::Insert {
                collection: "t".into(),
                vector: vec![0.5],
                dim: 1,
                field_name: "emb".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: None,
                provenance: None,
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        };
        tx.try_push(BridgeRequest { inner: req }).unwrap();
        // Set suspend flag.
        core.pressure_suspend_reads = true;
        core.drain_requests();
        assert_eq!(
            core.pending_count(),
            0,
            "drain must be no-op when suspended"
        );
    }

    #[test]
    fn spsc_reduced_depth_limits_drain() {
        let (mut core, mut tx, _rx, _dir) = make_core();
        use crate::bridge::dispatch::BridgeRequest;
        for i in 0..10u64 {
            let req = Request {
                request_id: RequestId::new(i + 1),
                tenant_id: TenantId::new(1),
                database_id: DatabaseId::DEFAULT,
                vshard_id: VShardId::new(0),
                plan: PhysicalPlan::Vector(VectorOp::Insert {
                    collection: "t".into(),
                    vector: vec![0.5],
                    dim: 1,
                    field_name: "emb".into(),
                    surrogate: Surrogate::ZERO,
                    pk_bytes: None,
                    provenance: None,
                }),
                deadline: Instant::now() + Duration::from_secs(5),
                priority: Priority::Normal,
                trace_id: TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::Read,
                ),
            };
            tx.try_push(BridgeRequest { inner: req }).unwrap();
        }
        core.spsc_read_depth = 3;
        core.drain_requests();
        assert_eq!(
            core.pending_count(),
            3,
            "must drain exactly read_depth items"
        );
    }
}
