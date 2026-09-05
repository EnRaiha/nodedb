// SPDX-License-Identifier: BUSL-1.1

//! Shared `ScopedMemory` builder for tests that hit a memory-governed
//! constructor (`CsrIndex::new`, `RTree::new`, `SegmentWriter::new`,
//! `InvertedIndex::open`, `MmapVectorSegment` opens) but do not exercise
//! budget pressure themselves.

#![allow(dead_code)] // Not every test binary that links `support` needs this helper.

use nodedb_mem::{EngineId, ScopedMemory};
use nodedb_types::{DatabaseId, TenantId};

/// Build a `ScopedMemory` bound to `engine`, backed by the shared
/// large-budget test governor.
pub fn test_scoped_memory(engine: EngineId) -> ScopedMemory {
    ScopedMemory::new(
        nodedb::data::executor::core_loop::test_governor(),
        DatabaseId::DEFAULT,
        TenantId::new(0),
        engine,
    )
}
