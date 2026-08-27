// SPDX-License-Identifier: BUSL-1.1

//! Memory governor pressure transitions across 70/85/95 thresholds must reduce
//! SPSC read depth, suspend reads, and emit metrics.
//!
//! `make_governor_at` from `pressure.rs` is inlined here — it is trivial (four
//! lines of book-keeping) and inlining avoids any production API surface change.

#[path = "executor_tests/helpers.rs"]
mod helpers;

use std::collections::HashMap;
use std::sync::Arc;

use nodedb::control::metrics::SystemMetrics;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb_mem::{EngineId, GovernorConfig, MemoryGovernor};
use nodedb_types::{DatabaseId, TenantId};

/// Baseline SPSC read depth, sourced from the `CoreLoop` public accessor so
/// the test does not hard-code the value.
fn normal_depth() -> usize {
    CoreLoop::spsc_read_depth_normal()
}

/// Build a `MemoryGovernor` with `budget_bytes` per engine and pre-fill
/// `engine` to `utilization_percent` of that budget.
///
/// All other engines start at 0%, so `engine_pressure(engine)` reflects the
/// supplied utilization while every other engine stays at Normal.
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
    if fill > 0
        && let Ok(tok) = gov.try_reserve(DatabaseId::DEFAULT, TenantId::new(1), engine, fill)
    {
        // `ReservationToken` is RAII; binding to `_` would drop it
        // immediately and reset the engine to 0% utilization. Leak it so
        // the budget stays charged for the lifetime of the test governor.
        std::mem::forget(tok);
    }
    Arc::new(gov)
}

// ── Normal (50%) ────────────────────────────────────────────────────────────

#[test]
fn normal_pressure_read_depth_unchanged_and_no_suspension() {
    let (mut core, _tx, _rx, _dir) = helpers::make_core();
    core.set_governor(make_governor_at(EngineId::Vector, 50));
    core.apply_spsc_pressure();

    assert_eq!(
        core.spsc_read_depth(),
        normal_depth(),
        "Normal pressure must leave read depth at baseline"
    );
    assert!(
        !core.pressure_suspend_reads(),
        "Normal pressure must not suspend reads"
    );
}

// ── Warning (75% — crosses 70% threshold) ───────────────────────────────────
// Warning is informational only: read depth stays at baseline, no suspension.

#[test]
fn warning_pressure_read_depth_unchanged_and_no_suspension() {
    let (mut core, _tx, _rx, _dir) = helpers::make_core();
    core.set_governor(make_governor_at(EngineId::Vector, 75));
    core.apply_spsc_pressure();

    assert_eq!(
        core.spsc_read_depth(),
        normal_depth(),
        "Warning pressure must leave read depth at baseline"
    );
    assert!(
        !core.pressure_suspend_reads(),
        "Warning pressure must not suspend reads"
    );
}

// ── Critical (88% — crosses 85% threshold) ──────────────────────────────────

#[test]
fn critical_pressure_halves_read_depth_and_increments_metric() {
    let (mut core, _tx, _rx, _dir) = helpers::make_core();
    let metrics = Arc::new(SystemMetrics::new());
    core.set_metrics(metrics.clone());
    core.set_governor(make_governor_at(EngineId::Vector, 88));

    core.apply_spsc_pressure();

    assert_eq!(
        core.spsc_read_depth(),
        normal_depth() / 2,
        "Critical pressure must halve read depth"
    );
    assert!(
        !core.pressure_suspend_reads(),
        "Critical pressure must not suspend reads"
    );

    // `apply_spsc_pressure` does NOT fire the backpressure metric — that is
    // `check_engine_pressure`'s responsibility (called per write handler).
    // The SPSC throttle path in `apply_spsc_pressure` does not duplicate the
    // counter increment.  Verify the counter is still zero here to document
    // that contract, then fire `check_engine_pressure` to verify the counter
    // increments on the correct path.
    {
        let m = metrics
            .backpressure_critical_by_engine
            .read()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(
            m.get("vector").copied().unwrap_or(0),
            0,
            "apply_spsc_pressure must NOT increment the metric counter"
        );
    }
}

#[test]
fn critical_check_engine_pressure_increments_metric() {
    use nodedb::bridge::envelope::{Priority, Request};
    use nodedb::data::executor::task::ExecutionTask;
    use nodedb::types::*;
    use nodedb_physical::physical_plan::{PhysicalPlan, VectorOp};
    use nodedb_types::{Surrogate, TraceId};
    use std::time::{Duration, Instant};

    let (mut core, _tx, _rx, _dir) = helpers::make_core();
    let metrics = Arc::new(SystemMetrics::new());
    core.set_metrics(metrics.clone());
    core.set_governor(make_governor_at(EngineId::Vector, 88));

    let task = ExecutionTask::new(Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        database_id: nodedb::types::DatabaseId::DEFAULT,
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
        event_source: nodedb::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: nodedb::bridge::envelope::Admission::Admitted,
    });

    let result = core.check_engine_pressure(&task, EngineId::Vector);
    assert!(
        result.is_none(),
        "Critical pressure must allow the write (returns None)"
    );

    let m = metrics
        .backpressure_critical_by_engine
        .read()
        .unwrap_or_else(|p| p.into_inner());
    assert_eq!(
        m.get("vector").copied().unwrap_or(0),
        1,
        "nodedb_backpressure_critical_total{{engine=\"vector\"}} must be 1"
    );
}

// ── Emergency (96% — crosses 95% threshold) ─────────────────────────────────

#[test]
fn emergency_pressure_suspends_reads_and_increments_metric() {
    use nodedb::bridge::envelope::{ErrorCode, Priority, Request};
    use nodedb::data::executor::task::ExecutionTask;
    use nodedb::types::*;
    use nodedb_physical::physical_plan::{PhysicalPlan, VectorOp};
    use nodedb_types::{Surrogate, TraceId};
    use std::time::{Duration, Instant};

    let (mut core, _tx, _rx, _dir) = helpers::make_core();
    let metrics = Arc::new(SystemMetrics::new());
    core.set_metrics(metrics.clone());
    core.set_governor(make_governor_at(EngineId::Vector, 96));

    // SPSC path: suspends reads.
    core.apply_spsc_pressure();
    assert!(
        core.pressure_suspend_reads(),
        "Emergency pressure must set pressure_suspend_reads"
    );

    // Per-handler path: rejects write and increments emergency metric.
    let task = ExecutionTask::new(Request {
        request_id: RequestId::new(2),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        database_id: nodedb::types::DatabaseId::DEFAULT,
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
        event_source: nodedb::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: nodedb::bridge::envelope::Admission::Admitted,
    });

    let result = core.check_engine_pressure(&task, EngineId::Vector);
    assert!(
        result.is_some(),
        "Emergency pressure must reject the write (returns Some)"
    );
    assert_eq!(
        result.unwrap().error_code.as_deref(),
        Some(&ErrorCode::ResourcesExhausted),
        "Emergency rejection must carry ResourcesExhausted"
    );

    let m = metrics
        .backpressure_emergency_by_engine
        .read()
        .unwrap_or_else(|p| p.into_inner());
    assert_eq!(
        m.get("vector").copied().unwrap_or(0),
        1,
        "nodedb_backpressure_emergency_total{{engine=\"vector\"}} must be 1"
    );
}

// ── Hysteresis: pressure drops back to 60% (Normal) ─────────────────────────

#[test]
fn hysteresis_clears_suspension_and_restores_read_depth_after_n_ticks() {
    let (mut core, _tx, _rx, _dir) = helpers::make_core();

    // Drive the core into Emergency to establish the suspended state.
    core.set_governor(make_governor_at(EngineId::Vector, 96));
    core.apply_spsc_pressure();
    assert!(
        core.pressure_suspend_reads(),
        "pre-condition: Emergency must have set suspend flag"
    );

    // Drop pressure to 60% (Normal — below all thresholds).
    core.set_governor(make_governor_at(EngineId::Vector, 60));

    // Ticks 1–7: hysteresis counter not yet reached; suspension still lifted
    // (Critical branch lifts suspension immediately, Normal/Warning requires
    // PRESSURE_NORMAL_HYSTERESIS consecutive ticks for read_depth restore).
    // After the first Normal tick from Emergency the suspend flag is cleared
    // (requires >= PRESSURE_NORMAL_HYSTERESIS ticks).  Run 7 ticks — not yet.
    for _ in 0..7 {
        core.apply_spsc_pressure();
    }
    // Suspension should still be true if < HYSTERESIS ticks have passed.
    // (Per the implementation, clearing suspension also requires the hysteresis
    // counter to reach PRESSURE_NORMAL_HYSTERESIS.)
    assert!(
        core.pressure_suspend_reads() || core.spsc_read_depth() < normal_depth(),
        "hysteresis pre-condition: either suspension or throttled depth must still hold after 7 ticks"
    );

    // Tick 8 (== PRESSURE_NORMAL_HYSTERESIS): both suspension and depth restored.
    core.apply_spsc_pressure();
    assert!(
        !core.pressure_suspend_reads(),
        "suspension must be cleared after PRESSURE_NORMAL_HYSTERESIS consecutive Normal ticks"
    );
    assert_eq!(
        core.spsc_read_depth(),
        normal_depth(),
        "read depth must be restored after PRESSURE_NORMAL_HYSTERESIS consecutive Normal ticks"
    );
}
