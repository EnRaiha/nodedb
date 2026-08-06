// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for CRDT-engine operations.
//!
//! No CRDT read carries a filter slot: a document read returns the merged Loro
//! state and a delta export returns the oplog those states were built from.
//! Reads of row content therefore refuse while a tree governs the collection,
//! while reads of collection configuration — the installed constraint set, the
//! conflict policy, the oplog version vector — carry no row content and pass.
//!
//! Mutations name the document they act on rather than selecting it with a
//! predicate, so they take the blanket level check.

use nodedb_physical::physical_plan::CrdtOp;

use super::context::{PermCtx, PermTreeLevel};

const ROW_CONTENT_REASON: &str =
    "the CRDT read returns merged document state through a payload that carries no subtree filter";

/// Exhaustive over [`CrdtOp`] so a new CRDT operation forces a decision
/// between filtering, refusing, and no-op.
pub(super) fn apply_crdt(ctx: &PermCtx<'_>, op: &CrdtOp) -> crate::Result<()> {
    match op {
        // Refuse: all four return stored row content — the current state, a
        // historical state, the oplog deltas those states were built from, or
        // the state a delta would produce — and none has a slot the subtree
        // filter could occupy.
        CrdtOp::Read { collection, .. }
        | CrdtOp::ReadAtVersion { collection, .. }
        | CrdtOp::ExportDelta { collection, .. }
        | CrdtOp::PreviewApply { collection, .. } => {
            ctx.refuse_if_tree(collection, ROW_CONTENT_REASON)
        }

        // No-op: collection configuration and sync bookkeeping. The installed
        // constraint set, the conflict-resolution policy, and the oplog
        // version vector describe the collection, not its rows.
        CrdtOp::ReadConstraints { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::GetVersionVector { .. } => Ok(()),

        // Filter (write level, blanket): every one of these mutates the state
        // of a document it names directly — a delta apply, a list edit, a
        // document upsert, a snapshot import, or a rewind to an earlier
        // version.
        CrdtOp::Apply { collection, .. }
        | CrdtOp::ApplyAuthenticated { collection, .. }
        | CrdtOp::ImportSnapshot { collection, .. }
        | CrdtOp::RestoreToVersion { collection, .. }
        | CrdtOp::ListInsert { collection, .. }
        | CrdtOp::ListMove { collection, .. }
        | CrdtOp::DocUpsert { collection, .. } => ctx.authorize(collection, PermTreeLevel::Write),

        // Filter (delete level, blanket): a document delete removes the row;
        // a list delete removes stored content from within it.
        CrdtOp::DocDelete { collection, .. } | CrdtOp::ListDelete { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Delete)
        }

        // No-op: the constraint / policy DDL and oplog compaction. They
        // configure or maintain the collection rather than acting on rows.
        CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::CompactAtVersion { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::CrdtOp;

    use super::super::plan::test_support::{
        apply, apply_without_tree, assert_refused, cache_with_tree,
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
    fn crdt_read_is_refused_under_a_tree() {
        let cache = cache_with_tree("notes");
        let mut plan = crdt_read("notes");
        assert_refused(apply(&mut plan, &cache), "notes");
    }

    /// …and is untouched when no tree governs the collection.
    #[test]
    fn crdt_read_without_a_tree_is_untouched() {
        let mut plan = crdt_read("notes");
        let before = plan.clone();
        assert!(apply_without_tree(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// Reading the conflict policy discloses configuration, not rows.
    #[test]
    fn get_policy_is_allowed_under_a_tree() {
        let cache = cache_with_tree("notes");
        let mut plan = PhysicalPlan::Crdt(CrdtOp::GetPolicy {
            collection: "notes".into(),
        });
        assert!(apply(&mut plan, &cache).is_ok());
    }
}
