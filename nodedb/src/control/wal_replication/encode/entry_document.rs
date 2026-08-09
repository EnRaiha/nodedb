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
            // Resolved against the leader's catalog at plan time; the record
            // carries the applied row, not the plan that produced it.
            resolved_sum_targets: _,
        } => document::point_put(collection, document_id, value, surrogate.as_u32()),
        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
            returning: _,
            rls_filters: _,
            // See `PointPut`.
            resolved_sum_targets: _,
            deferred_sum_targets: _,
        } => document::point_insert(
            collection,
            document_id,
            value,
            *if_absent,
            surrogate.as_u32(),
        ),
        DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        } => document::point_delete(collection, document_id, surrogate.as_u32()),
        DocumentOp::PointUpdate {
            collection,
            document_id,
            updates,
            surrogate,
            ..
        } => document::point_update(collection, document_id, updates, surrogate.as_u32()),
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
            resolved_sum_targets: _,
        } => document::upsert(
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate.as_u32(),
        ),
        DocumentOp::BulkDelete {
            collection,
            filters,
            returning: _,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: _,
            rls_write_check: _,
            // See `PointPut`: the record is the applied write, not the plan.
            resolved_sum_targets: _,
        } => document::bulk_delete(collection, filters),
        DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            returning: _,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: _,
            rls_write_check: _,
            // See `PointPut`.
            resolved_sum_targets: _,
        } => document::bulk_update(collection, filters, updates),
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
            resolved_sum_targets: _,
            deferred_sum_targets: _,
        } => document::batch_insert(collection, documents, surrogates),

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
            resolved_sum_targets: _,
        } => document::truncate(collection, *restart_identity),
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
