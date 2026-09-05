// SPDX-License-Identifier: BUSL-1.1

//! Shared test fixtures for the `vector_suite` integration test binary.
//!
//! Not reachable from `src/`'s inline `#[cfg(test)]` modules — those compile
//! into the library crate, while everything under `tests/` compiles as a
//! separate crate against the public API.

use std::sync::Arc;

use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor, ScopedMemory};
use nodedb_types::{DatabaseId, TenantId};

/// A scope backed by a governor whose ceiling covers every engine's limit.
pub fn test_memory() -> ScopedMemory {
    let per_engine = usize::MAX / EngineId::ALL.len();
    let governor = Arc::new(
        MemoryGovernor::new(GovernorConfig {
            global_ceiling: per_engine * EngineId::ALL.len(),
            engine_limits: EngineLimits::uniform(per_engine),
        })
        .expect("test governor"),
    );
    ScopedMemory::new(
        governor,
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Vector,
    )
}
