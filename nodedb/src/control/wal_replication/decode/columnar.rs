// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Columnar`.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::ColumnarOp;
use nodedb_types::RlsWriteCheck;

/// Reconstruct the columnar predicate-DML plan. The apply re-scans local
/// columnar state at this committed log position and mutates the predicate
/// matches — deterministic across replicas by Raft log order (identical
/// prior state ⇒ identical matching set).
pub(super) fn bulk_dml(
    collection: &str,
    filters: &[u8],
    is_update: bool,
    updates: &[(String, Vec<u8>)],
) -> PhysicalPlan {
    // No RLS predicate here: this node is a follower applying an
    // already-committed write. The writing identity is not available on
    // this node. The leader enforces RLS before proposing the write.
    if is_update {
        PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: collection.to_owned(),
            filters: filters.to_vec(),
            updates: updates.to_vec(),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        })
    } else {
        PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: collection.to_owned(),
            filters: filters.to_vec(),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        })
    }
}
