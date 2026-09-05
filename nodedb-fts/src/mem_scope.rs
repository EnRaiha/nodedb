// SPDX-License-Identifier: Apache-2.0

//! Builds the `(database, tenant, Fts)` scoped memory handle callers derive
//! from a raw governor reference.

use std::sync::Arc;

use nodedb_mem::{EngineId, MemoryGovernor, ScopedMemory};
use nodedb_types::{DatabaseId, TenantId};

/// Bind `governor` to `(database_id, tid, EngineId::Fts)`.
pub(crate) fn fts_scope(
    governor: &Arc<MemoryGovernor>,
    database_id: u64,
    tid: u64,
) -> ScopedMemory {
    ScopedMemory::new(
        Arc::clone(governor),
        DatabaseId::new(database_id),
        TenantId::new(tid),
        EngineId::Fts,
    )
}
