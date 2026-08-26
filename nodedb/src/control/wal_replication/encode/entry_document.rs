// SPDX-License-Identifier: BUSL-1.1

//! Classify a `DocumentOp` into an optional `ReplicatedWrite`.
//!
//! Exhaustive over `DocumentOp` (not a catch-all): a new variant is a compile
//! error here, so no future document write is silently left un-replicated.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::document;
use super::document::{SumFields, WireReturning};
use super::entry::encode_returning;
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
            // Decode re-derives this from `document_id.as_bytes()`, same value.
            pk_bytes: _,
            returning,
            rls_filters,
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
            encode_returning(returning),
            rls_filters,
        ),
        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
            returning,
            rls_filters,
            // See `PointPut`.
            resolved_sum_targets,
            deferred_sum_targets,
        } => document::point_insert(
            collection,
            document_id,
            value,
            *if_absent,
            surrogate.as_u32(),
            SumFields {
                resolved: resolved_sum_targets,
                deferred: deferred_sum_targets,
            },
            WireReturning {
                returning: encode_returning(returning),
                rls_filters,
            },
        ),
        DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            // Decode re-derives this from `document_id.as_bytes()`, same value.
            pk_bytes: _,
            returning,
            rls_filters,
            // A follower has no writing identity; decode stamps
            // `already_decided_elsewhere()`.
            rls_write_check: _,
            // See `PointPut`.
            resolved_sum_targets,
        } => document::point_delete(
            collection,
            document_id,
            surrogate.as_u32(),
            resolved_sum_targets,
            encode_returning(returning),
            rls_filters,
        ),
        DocumentOp::PointUpdate {
            collection,
            document_id,
            surrogate,
            // Decode re-derives this from `document_id.as_bytes()`, same value.
            pk_bytes: _,
            updates,
            returning,
            rls_filters,
            // A follower has no writing identity; decode stamps
            // `already_decided_elsewhere()`.
            rls_write_check: _,
            // See `PointPut`.
            resolved_sum_targets,
        } => document::point_update(
            collection,
            document_id,
            updates,
            surrogate.as_u32(),
            resolved_sum_targets,
            WireReturning {
                returning: encode_returning(returning),
                rls_filters,
            },
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
            returning,
            rls_filters,
            // See `PointPut`.
            resolved_sum_targets,
        } => document::upsert(
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate.as_u32(),
            resolved_sum_targets,
            WireReturning {
                returning: encode_returning(returning),
                rls_filters,
            },
        ),
        DocumentOp::BulkDelete {
            collection,
            filters,
            returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters,
            // The leader already decided every matched row against the write
            // policy; the replicated record carries the predicate, not it.
            rls_write_check: _,
            // See `PointPut`. The predicate's MATCHES are re-derived by every
            // replica; the identity of the targets they credit is not.
            resolved_sum_targets,
        } => document::bulk_delete(
            collection,
            filters,
            resolved_sum_targets,
            encode_returning(returning),
            rls_filters,
        ),
        DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters,
            // See `BulkDelete`.
            rls_write_check: _,
            // See `BulkDelete`.
            resolved_sum_targets,
        } => document::bulk_update(
            collection,
            filters,
            updates,
            resolved_sum_targets,
            encode_returning(returning),
            rls_filters,
        ),
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
            returning,
            rls_filters,
            // See `PointPut`.
            resolved_sum_targets,
            deferred_sum_targets,
        } => document::batch_insert(
            collection,
            documents,
            surrogates,
            resolved_sum_targets,
            deferred_sum_targets,
            encode_returning(returning),
            rls_filters,
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

        // The verdict is already on the plan
        // (`RlsWriteCheck::DecidedEarlierInRequest`), so unlike the predicate
        // arms above there is nothing to re-decide here.
        DocumentOp::ResolvedWrite {
            mutations,
            response_payload,
            // Decode stamps `decided_earlier_in_request()`: the decision this
            // slot carries was made before the entry was proposed, and the
            // record is what proves it.
            rls_write_check: _,
        } => document::resolved_write(mutations, response_payload),

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
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
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
