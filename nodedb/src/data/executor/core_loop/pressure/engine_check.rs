// SPDX-License-Identifier: BUSL-1.1

//! Per-handler memory-pressure gate for writes.

use nodedb_mem::{EngineId, PressureLevel};
use tracing::warn;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

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
        match self.governor.engine_pressure(engine) {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_mem::EngineId;

    use super::super::fixtures::{make_core, make_governor_at, make_task};
    use crate::bridge::envelope::ErrorCode;
    use crate::control::metrics::SystemMetrics;

    #[test]
    fn normal_pressure_allows() {
        let (mut core, _tx, _rx, _dir) = make_core();
        core.governor = make_governor_at(EngineId::Vector, 0);
        assert!(
            core.check_engine_pressure(&make_task(), EngineId::Vector)
                .is_none()
        );
    }

    #[test]
    fn warning_pressure_allows() {
        let (mut core, _tx, _rx, _dir) = make_core();
        core.governor = make_governor_at(EngineId::Vector, 75);
        assert!(
            core.check_engine_pressure(&make_task(), EngineId::Vector)
                .is_none()
        );
    }

    #[test]
    fn critical_pressure_allows_and_increments_metric() {
        let (mut core, _tx, _rx, _dir) = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        core.set_metrics(metrics.clone());
        core.governor = make_governor_at(EngineId::Vector, 88);

        assert!(
            core.check_engine_pressure(&make_task(), EngineId::Vector)
                .is_none(),
            "Critical must allow the write"
        );
        let m = metrics
            .backpressure_critical_by_engine
            .read()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(m.get("vector").copied().unwrap_or(0), 1);
    }

    #[test]
    fn emergency_pressure_rejects_and_increments_metric() {
        let (mut core, _tx, _rx, _dir) = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        core.set_metrics(metrics.clone());
        core.governor = make_governor_at(EngineId::Vector, 97);

        let result = core.check_engine_pressure(&make_task(), EngineId::Vector);
        assert_eq!(
            result.expect("Emergency must reject").error_code.as_deref(),
            Some(&ErrorCode::ResourcesExhausted)
        );
        let m = metrics
            .backpressure_emergency_by_engine
            .read()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(m.get("vector").copied().unwrap_or(0), 1);
    }
}
