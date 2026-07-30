// SPDX-License-Identifier: BUSL-1.1

//! The guard that keeps a CRDT apply from reaching the Data Plane without
//! having gone through admission.
//!
//! An apply that skips admission skips the post-merge RLS evaluation and the
//! frontier fence with it, so it would durably land a delta nobody authorized
//! against its real post-image. Refusing loudly here is what makes the
//! admission path non-optional rather than merely conventional.

use crate::bridge::envelope::PhysicalPlan;

pub(super) fn reject_unadmitted_crdt_apply(plan: &PhysicalPlan) -> crate::Result<()> {
    if matches!(
        plan,
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
                | nodedb_physical::physical_plan::CrdtOp::ImportSnapshot { .. }
        )
    ) {
        return Err(crate::Error::CrdtApplyRequiresAdmission);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::CrdtOp;
    use nodedb_types::Surrogate;

    use super::*;

    #[test]
    fn generic_sync_dispatch_rejects_unadmitted_apply() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: "docs".into(),
            document_id: "doc-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        assert!(matches!(
            reject_unadmitted_crdt_apply(&plan),
            Err(crate::Error::CrdtApplyRequiresAdmission)
        ));
    }
}
