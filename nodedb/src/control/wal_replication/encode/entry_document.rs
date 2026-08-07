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
        } => document::point_put(collection, document_id, value, surrogate.as_u32()),
        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
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
        } => document::bulk_delete(collection, filters),
        DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            returning: _,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: _,
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
        } => document::truncate(collection, *restart_identity),
        // OLLP-prepared bulk plans carrying predicted surrogates/edges route
        // via the cross-shard Calvin path, not single-shard Raft proposal, so
        // they are intentionally not encoded here.
        DocumentOp::BulkDelete { .. } | DocumentOp::BulkUpdate { .. } => return None,

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
