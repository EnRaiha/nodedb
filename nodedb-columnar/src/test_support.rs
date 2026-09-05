// SPDX-License-Identifier: Apache-2.0

//! Shared test fixtures for this crate's inline `#[cfg(test)]` modules.

use std::sync::Arc;

use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor, ScopedMemory};
use nodedb_types::{DatabaseId, TenantId};

/// Per-engine limit. The ceiling multiplies it back up, because the governor
/// rejects a config whose engine limits outgrow the global ceiling.
const PER_ENGINE: usize = usize::MAX / EngineId::COUNT;

/// A scope backed by a governor whose ceiling covers every engine's limit.
pub(crate) fn test_memory() -> ScopedMemory {
    let governor = Arc::new(
        MemoryGovernor::new(GovernorConfig {
            global_ceiling: PER_ENGINE * EngineId::ALL.len(),
            engine_limits: EngineLimits::uniform(PER_ENGINE),
        })
        .expect("test governor"),
    );
    ScopedMemory::new(
        governor,
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Columnar,
    )
}
