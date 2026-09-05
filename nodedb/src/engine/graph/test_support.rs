// SPDX-License-Identifier: BUSL-1.1

//! Shared test-only helpers for graph engine unit tests.

use nodedb_mem::{EngineId, ScopedMemory};
use nodedb_types::{DatabaseId, TenantId};

/// A `ScopedMemory` bound to the default database/tenant/graph scope,
/// backed by a real governor with ample per-engine limits.
pub(crate) fn test_scoped_memory() -> ScopedMemory {
    ScopedMemory::new(
        crate::data::executor::core_loop::test_governor(),
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Graph,
    )
}
