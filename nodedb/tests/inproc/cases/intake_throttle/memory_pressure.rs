// SPDX-License-Identifier: BUSL-1.1

//! Engine memory pressure across the governor's 70/85/95 thresholds.

use std::sync::Arc;

use nodedb::control::metrics::SystemMetrics;
use nodedb::data::executor::core_loop::pressure::ThrottleLevel;
use nodedb_mem::EngineId;
use nodedb_types::QualifiedCollection;

use super::helpers::{SUSPENDED_READ_DEPTH, make_governor_at, normal_depth};

// ── Normal (50%) ────────────────────────────────────────────────────────────

#[test]
fn normal_pressure_read_depth_unchanged_and_no_suspension() {
    let (mut core, _tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();
    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 50));
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
    let (mut core, _tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();
    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 75));
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
    let (mut core, _tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();
    let metrics = Arc::new(SystemMetrics::new());
    core.set_metrics(metrics.clone());
    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 88));

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
    use nodedb::data::executor::task::ExecutionTask;
    use nodedb_physical::physical_plan::{PhysicalPlan, VectorOp};
    use nodedb_types::Surrogate;

    let (mut core, _tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();
    let metrics = Arc::new(SystemMetrics::new());
    core.set_metrics(metrics.clone());
    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 88));

    let task = ExecutionTask::new(crate::cases::core_loop::helpers::make_request_with_id(
        1,
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: QualifiedCollection::new(nodedb::types::DatabaseId::DEFAULT, "test"),
            vector: vec![0.1],
            dim: 1,
            field_name: "emb".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: None,
            provenance: None,
        }),
    ));

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
    use nodedb::bridge::envelope::ErrorCode;
    use nodedb::data::executor::task::ExecutionTask;
    use nodedb_physical::physical_plan::{PhysicalPlan, VectorOp};
    use nodedb_types::Surrogate;

    let (mut core, _tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();
    let metrics = Arc::new(SystemMetrics::new());
    core.set_metrics(metrics.clone());
    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 96));

    // SPSC path: suspends reads.
    core.apply_spsc_pressure();
    assert!(
        core.pressure_suspend_reads(),
        "Emergency pressure must set pressure_suspend_reads"
    );

    // Per-handler path: rejects write and increments emergency metric.
    let task = ExecutionTask::new(crate::cases::core_loop::helpers::make_request_with_id(
        2,
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: QualifiedCollection::new(nodedb::types::DatabaseId::DEFAULT, "test"),
            vector: vec![0.1],
            dim: 1,
            field_name: "emb".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: None,
            provenance: None,
        }),
    ));

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
    let (mut core, _tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();

    // Drive the core into Emergency to establish the suspended state.
    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 96));
    core.apply_spsc_pressure();
    assert!(
        core.pressure_suspend_reads(),
        "pre-condition: Emergency must have set suspend flag"
    );

    // Drop pressure to 60% (Normal — below all thresholds).
    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 60));

    // Ticks 1-7: the release window (THROTTLE_RELEASE_TICKS = 8 consecutive
    // Full-level observations) has not closed, so the core holds one level
    // above Full until the 8th calm tick.
    for _ in 0..7 {
        core.apply_spsc_pressure();
    }
    assert!(
        core.pressure_suspend_reads() || core.spsc_read_depth() < normal_depth(),
        "hysteresis pre-condition: either suspension or throttled depth must still hold after 7 ticks"
    );

    // Tick 8 (== THROTTLE_RELEASE_TICKS): both suspension and depth restored.
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

// ── Sustained Critical: one fixed throttle step, held ───────────────────────

/// The throttled depth is a function of the current level, so repeated
/// Critical ticks settle at `normal / 2`.
#[test]
fn sustained_critical_pressure_holds_throttled_read_depth() {
    let (mut core, _tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();
    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 88));

    for tick in 1..=6 {
        core.apply_spsc_pressure();
        assert_eq!(
            core.spsc_read_depth(),
            normal_depth() / 2,
            "tick {tick}: sustained Critical must hold the throttled depth"
        );
        assert_eq!(
            core.throttle_level(),
            ThrottleLevel::Throttled,
            "tick {tick}: Critical holds the throttled level"
        );
        // Only a suspended core reaches depth 1.
        assert_ne!(
            core.spsc_read_depth(),
            SUSPENDED_READ_DEPTH,
            "tick {tick}: Critical stays distinguishable from a suspended core"
        );
        assert!(
            !core.pressure_suspend_reads(),
            "tick {tick}: Critical must not suspend reads"
        );
    }
}

/// Leaving Emergency for Critical restores the Critical throttle level.
#[test]
fn critical_after_emergency_restores_throttled_read_depth() {
    let (mut core, _tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();

    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 96));
    core.apply_spsc_pressure();
    assert!(
        core.pressure_suspend_reads(),
        "pre-condition: Emergency must have suspended reads"
    );

    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 88));
    core.apply_spsc_pressure();

    assert!(
        !core.pressure_suspend_reads(),
        "Critical must lift the Emergency suspension"
    );
    assert_eq!(
        core.spsc_read_depth(),
        normal_depth() / 2,
        "Critical after Emergency must restore the throttled depth"
    );
}

/// Sustained Normal after sustained Critical restores the full depth.
#[test]
fn sustained_critical_then_normal_hysteresis_restores_full_read_depth() {
    let (mut core, _tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();

    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 88));
    for _ in 0..6 {
        core.apply_spsc_pressure();
    }

    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 60));
    for _ in 0..8 {
        core.apply_spsc_pressure();
    }

    assert_eq!(
        core.spsc_read_depth(),
        normal_depth(),
        "sustained Normal must restore the baseline depth"
    );
    assert!(
        !core.pressure_suspend_reads(),
        "sustained Normal must leave reads unsuspended"
    );
}
