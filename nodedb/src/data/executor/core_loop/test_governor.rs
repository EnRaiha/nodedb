// SPDX-License-Identifier: BUSL-1.1

//! Shared `MemoryGovernor` builder for `CoreLoop` construction sites that
//! have no real governor in scope (unit tests and integration tests).
//! Production code always supplies a real governor from `nodedb::memory::init_governor`.

use std::sync::Arc;

use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor};

use super::CoreLoop;

/// Per-engine budget for [`test_governor`]. Large enough that no existing
/// test trips a pressure threshold by accident.
const TEST_ENGINE_BUDGET_BYTES: usize = 1 << 30;

/// Build a `MemoryGovernor` with every engine at Normal pressure.
///
/// `CoreLoop` and `SharedState` no longer accept a missing governor, so
/// every construction site needs one. A test that exercises specific
/// pressure levels builds its own governor instead — see
/// `core_loop::pressure::tests::make_governor_at`.
///
/// Public (not `#[cfg(test)]`) because `nodedb/tests/inproc` links this
/// crate as an external dependency and cannot see `cfg(test)` items.
#[doc(hidden)]
pub fn test_governor() -> Arc<MemoryGovernor> {
    let engine_limits = EngineLimits::uniform(TEST_ENGINE_BUDGET_BYTES);
    // `GovernorConfig::validate` requires the ceiling to cover every engine
    // limit summed — not an arbitrary margin.
    let global_ceiling = TEST_ENGINE_BUDGET_BYTES * EngineId::ALL.len();
    Arc::new(
        MemoryGovernor::new(GovernorConfig {
            global_ceiling,
            engine_limits,
        })
        .expect("test governor config satisfies GovernorConfig invariants"),
    )
}

impl CoreLoop {
    /// Replace the governor a test drives pressure scenarios through.
    ///
    /// Test-only: production wiring supplies the governor once, at
    /// construction, and never replaces it.
    #[doc(hidden)]
    pub fn set_governor_for_testing(&mut self, governor: Arc<MemoryGovernor>) {
        self.governor = governor;
    }
}
