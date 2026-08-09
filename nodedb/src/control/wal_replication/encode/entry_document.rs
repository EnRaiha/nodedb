// SPDX-License-Identifier: BUSL-1.1

//! Classify a `DocumentOp` into an optional `ReplicatedWrite`.
//!
//! Exhaustive over `DocumentOp` (not a catch-all): a new variant is a compile
//! error here, so no future document write is silently left un-replicated.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::document;
use nodedb_physical::physical_plan::DocumentOp;

/// Encode a `DocumentOp` write variant into its `ReplicatedWrite` wire shape,
/// or `None` when the op is not a single-shard replicated write.
pub(super) fn document_write(op: &DocumentOp) -> Option<ReplicatedWrite> {
    Some(match op {
        DocumentOp::PointPut {
            collection,
            document_id,
            value,
            surrogate,
            pk_bytes: _,
            // The replicated record carries the row, not the projection: a
            // follower re-applies the write, it does not answer the client.
            returning: _,
            rls_filters: _,
            // Resolved against the proposing node's catalog at plan time, and
            // copied onto the record: the applier re-executes this write and
            // maintains the derived total itself, but cannot resolve the target
            // row's identity — see `document`'s module doc.
            resolved_sum_targets,
        } => document::point_put(
            collection,
            document_id,
            value,
            surrogate.as_u32(),
            resolved_sum_targets,
        ),
        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
            returning: _,
            rls_filters: _,
            // See `PointPut`.
            resolved_sum_targets,
            deferred_sum_targets,
        } => document::point_insert(
            collection,
            document_id,
            value,
            *if_absent,
            surrogate.as_u32(),
            resolved_sum_targets,
            deferred_sum_targets,
        ),
        DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            // See `PointPut`.
            resolved_sum_targets,
            ..
        } => document::point_delete(
            collection,
            document_id,
            surrogate.as_u32(),
            resolved_sum_targets,
        ),
        DocumentOp::PointUpdate {
            collection,
            document_id,
            updates,
            surrogate,
            // See `PointPut`.
            resolved_sum_targets,
            ..
        } => document::point_update(
            collection,
            document_id,
            updates,
            surrogate.as_u32(),
            resolved_sum_targets,
        ),
        DocumentOp::Upsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate,
            // The leader already decided this row against the write policy; the
            // replicated record carries the row, not the policy.
            rls_write_check: _,
            returning: _,
            rls_filters: _,
            // See `PointPut`.
            resolved_sum_targets,
        } => document::upsert(
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate.as_u32(),
            resolved_sum_targets,
        ),
        DocumentOp::BulkDelete {
            collection,
            filters,
            returning: _,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: _,
            rls_write_check: _,
            // See `PointPut`. The predicate's MATCHES are re-derived by every
            // replica; the identity of the targets they credit is not.
            resolved_sum_targets,
        } => document::bulk_delete(collection, filters, resolved_sum_targets),
        DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            returning: _,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: _,
            rls_write_check: _,
            // See `BulkDelete`.
            resolved_sum_targets,
        } => document::bulk_update(collection, filters, updates, resolved_sum_targets),
        DocumentOp::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            source_limit,
        } => document::insert_select(
            target_collection,
            source_collection,
            source_filters,
            *source_limit,
        ),

        DocumentOp::BatchInsert {
            collection,
            documents,
            surrogates,
            returning: _,
            rls_filters: _,
            // See `PointPut`.
            resolved_sum_targets,
            deferred_sum_targets,
        } => document::batch_insert(
            collection,
            documents,
            surrogates,
            resolved_sum_targets,
            deferred_sum_targets,
        ),

        // Known replication gaps: genuine writes not yet wired to a
        // `ReplicatedWrite`. The data still lands via the leader's own
        // redb/WAL; only cross-node Raft replication of these ops is missing.
        // `Merge` / `UpdateFromJoin` — cross-collection writes whose
        // source/target co-location is not enforced (`Unroutable` in
        // `plan_vshard`); no ReplicatedWrite shape yet.
        DocumentOp::Merge { .. } | DocumentOp::UpdateFromJoin { .. } => return None,
        DocumentOp::Truncate {
            collection,
            restart_identity,
            // See `PointPut`.
            resolved_sum_targets,
        } => document::truncate(collection, *restart_identity, resolved_sum_targets),
        // OLLP-prepared bulk plans carrying predicted surrogates/edges route
        // via the cross-shard Calvin path, not single-shard Raft proposal, so
        // they are intentionally not encoded here.
        DocumentOp::BulkDelete { .. } | DocumentOp::BulkUpdate { .. } => return None,

        DocumentOp::ApplyBalanceDelta {
            collection,
            document_id,
            surrogate,
            column,
            delta,
            join_column,
            join_value,
        } => document::apply_balance_delta(
            collection,
            document_id,
            surrogate.as_u32(),
            column,
            delta,
            join_column,
            join_value,
        ),

        // Not a write — reads / scans / index DDL-metadata / system ops.
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => return None,
    })
}
