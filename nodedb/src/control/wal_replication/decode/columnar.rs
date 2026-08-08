// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Columnar`.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::ColumnarOp;

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
    // No RLS predicate: the leader already decided these rows against the
    // writing identity, which the follower does not have.
    if is_update {
        PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: collection.to_owned(),
            filters: filters.to_vec(),
            updates: updates.to_vec(),
            rls_write_check: Vec::new(),
        })
    } else {
        PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: collection.to_owned(),
            filters: filters.to_vec(),
            rls_write_check: Vec::new(),
        })
    }
}
