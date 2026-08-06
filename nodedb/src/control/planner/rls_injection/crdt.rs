// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for CRDT-engine operations.
//!
//! No CRDT read carries a filter slot: a document read returns the merged Loro
//! state and a delta export returns the oplog those states were built from.
//! Reads of row content therefore refuse while a policy applies, while reads
//! of collection configuration — the installed constraint set, the conflict
//! policy, the oplog version vector — carry no row content and pass.

use nodedb_physical::physical_plan::CrdtOp;

use super::context::RlsCtx;

const ROW_CONTENT_REASON: &str =
    "the CRDT read returns merged document state through a payload that carries no row filter";

/// Exhaustive over [`CrdtOp`] so a new CRDT operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_crdt(ctx: &RlsCtx<'_>, op: &CrdtOp) -> crate::Result<()> {
    match op {
        // Refuse: all four return stored row content — the current state, a
        // historical state, the oplog deltas those states were built from, or
        // the state a delta would produce — and none has a slot the policy
        // could occupy.
        CrdtOp::Read { collection, .. }
        | CrdtOp::ReadAtVersion { collection, .. }
        | CrdtOp::ExportDelta { collection, .. }
        | CrdtOp::PreviewApply { collection, .. } => {
            ctx.refuse_if_policy(collection, ROW_CONTENT_REASON)
        }

        // No-op: collection configuration and sync bookkeeping. The installed
        // constraint set, the conflict-resolution policy, and the oplog version
        // vector describe the collection, not its rows, so a row policy has
        // nothing to restrict in them.
        CrdtOp::ReadConstraints { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::GetVersionVector { .. } => Ok(()),

        // No-op: writes, snapshot install, history maintenance, and the
        // constraint / policy DDL. Write policies are enforced separately by
        // `RlsPolicyStore::check_write_with_auth`.
        CrdtOp::Apply { .. }
        | CrdtOp::ApplyAuthenticated { .. }
        | CrdtOp::ImportSnapshot { .. }
        | CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::RestoreToVersion { .. }
        | CrdtOp::CompactAtVersion { .. }
        | CrdtOp::ListInsert { .. }
        | CrdtOp::ListDelete { .. }
        | CrdtOp::ListMove { .. }
        | CrdtOp::DocUpsert { .. }
        | CrdtOp::DocDelete { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::CrdtOp;

    use super::super::plan::test_support::{
        assert_refused, inject, inject_without_policy, store_with_read_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn crdt_read(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::Read {
            collection: collection.into(),
            document_id: "d1".into(),
        })
    }

    /// A CRDT document read returns merged state with no filter slot.
    #[test]
    fn crdt_read_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("notes");
        let mut plan = crdt_read("notes");
        assert_refused(inject(&mut plan, &store), "notes");
    }

    /// …and is untouched when no policy applies.
    #[test]
    fn crdt_read_without_a_policy_is_untouched() {
        let mut plan = crdt_read("notes");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// Reading the conflict policy discloses configuration, not rows.
    #[test]
    fn get_policy_is_allowed_under_a_read_policy() {
        let store = store_with_read_policy("notes");
        let mut plan = PhysicalPlan::Crdt(CrdtOp::GetPolicy {
            collection: "notes".into(),
        });
        assert!(inject(&mut plan, &store).is_ok());
    }
}
