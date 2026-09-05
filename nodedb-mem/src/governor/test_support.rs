// SPDX-License-Identifier: Apache-2.0

//! Shared test fixtures for the governor submodule test blocks.

use nodedb_types::{DatabaseId, TenantId};

use crate::engine::EngineId;
use crate::engine_limits::EngineLimits;
use crate::governor::config::GovernorConfig;

/// Every engine other than the three under direct test keeps the zero
/// default: an unallocated zero-limit engine reports Normal pressure,
/// and any nonzero reservation against it still denies.
pub(super) fn test_config() -> GovernorConfig {
    let engine_limits = EngineLimits::zeroed()
        .with(EngineId::Vector, 4096)
        .with(EngineId::Query, 2048)
        .with(EngineId::Timeseries, 1024);

    GovernorConfig {
        global_ceiling: 8192,
        engine_limits,
    }
}

pub(super) fn db() -> DatabaseId {
    DatabaseId::DEFAULT
}

pub(super) fn tenant() -> TenantId {
    TenantId::new(1)
}
