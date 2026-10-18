// SPDX-License-Identifier: BUSL-1.1

//! Column reads of the document engine's operations.

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_plan::document::MergeActionOp;

use super::reads::{Clause, NodeReads};

/// The columns `op` reads outside its plain projection.
///
/// An orchestrated write's source rows are a child plan, checked on their
/// own. A source relation with a redaction rule is refused whole elsewhere.
/// An assignment's source-qualified columns (`s.col`) belong to that source,
/// so only its target columns are read here.
pub(super) fn reads(op: &DocumentOp, out: &mut NodeReads) -> crate::Result<()> {
    match op {
        DocumentOp::Scan {
            sort_keys,
            filters,
            distinct,
            projection,
            computed_columns,
            window_functions,
            ..
        } => {
            out.filter_bytes(filters)?;
            out.sort_keys(sort_keys);
            out.computed_bytes(computed_columns)?;
            out.window_bytes(window_functions)?;
            out.distinct(*distinct, projection);
        }
        DocumentOp::RangeScan { field, .. } => out.field(field, Clause::IndexProbe),
        DocumentOp::IndexLookup { path, .. } => out.field(path, Clause::IndexProbe),
        DocumentOp::IndexedFetch { path, filters, .. } => {
            out.field(path, Clause::IndexProbe);
            out.filter_bytes(filters)?;
        }
        // A read, update, or delete by key reports whether the key exists.
        DocumentOp::PointGet { .. } | DocumentOp::PointDelete { .. } => out.key_lookup = true,
        DocumentOp::PointUpdate { updates, .. } => {
            out.key_lookup = true;
            out.updates(updates);
        }
        DocumentOp::Upsert {
            on_conflict_updates: updates,
            ..
        }
        | DocumentOp::BatchUpsert {
            on_conflict_updates: updates,
            ..
        } => out.updates(updates),
        DocumentOp::BulkUpdate {
            filters, updates, ..
        } => {
            out.filter_bytes(filters)?;
            out.updates(updates);
        }
        DocumentOp::BulkDelete { filters, .. } => out.filter_bytes(filters)?,
        DocumentOp::UpdateFromJoin {
            target_join_col,
            target_filters,
            updates,
            source_alias,
            ..
        } => {
            out.field(target_join_col, Clause::JoinKey);
            out.filter_bytes(target_filters)?;
            out.updates_of_target(updates, Some(source_alias));
        }
        DocumentOp::Merge {
            target_join_col,
            clauses,
            source_alias,
            ..
        } => {
            out.field(target_join_col, Clause::JoinKey);
            for clause in clauses {
                match &clause.action {
                    MergeActionOp::Update { updates } => {
                        out.updates_of_target(updates, Some(source_alias));
                    }
                    MergeActionOp::Delete
                    | MergeActionOp::Insert { .. }
                    | MergeActionOp::DoNothing => {}
                }
            }
        }
        DocumentOp::ResolveWrite(inner) => reads(inner, out)?,
        // Writes of a caller-supplied row, bulk loads, DDL, and maintenance
        // read no column in a predicate or an expression.
        DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolvedWrite { .. } => {}
    }
    Ok(())
}
