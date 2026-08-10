// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Document`.
//!
//! Delegated from `decode/entry.rs`'s single grouped match arm (every
//! document-family pattern dispatches here) so that dispatcher stays under the
//! file size limit. `write` is guaranteed by that caller to already be one of
//! these variants — every other `ReplicatedWrite` variant is handled by its
//! own grouped arm in `decode/entry.rs`'s exhaustive match and never reaches
//! here; the trailing arm below exists only because `write`'s static type is
//! the full enum, mirroring how `vector::decode_arm` guards the same
//! dispatch contract.

use super::super::types::{ReplicatedSumTarget, ReplicatedWrite};
use super::ctx::DecodeCtx;
use super::document;
use super::document::WireSumResolution;
use crate::bridge::envelope::PhysicalPlan;

/// Pair a record's two materialized-sum resolution slots.
///
/// Both are handed to the decoder together so it — and not each call site —
/// decides which one answers. Passing the superseded slot alone would strip
/// every entry's target collection, which is the ambiguity the newer slot
/// exists to remove; see [`WireSumResolution`].
fn sums<'a>(
    bindings: &'a [ReplicatedSumTarget],
    legacy: &'a [(String, u32)],
) -> WireSumResolution<'a> {
    WireSumResolution { bindings, legacy }
}

pub(super) fn decode_arm(ctx: &DecodeCtx, write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
            surrogate,
            resolved_sum_targets,
            resolved_sum_target_bindings,
        } => document::point_put(
            ctx,
            collection,
            document_id,
            value,
            *surrogate,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
        ),
        ReplicatedWrite::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
            resolved_sum_targets,
            deferred_sum_targets,
            resolved_sum_target_bindings,
        } => document::point_insert(
            ctx,
            collection,
            document_id,
            value,
            *if_absent,
            *surrogate,
            document::SumDecisions {
                resolved: sums(resolved_sum_target_bindings, resolved_sum_targets),
                deferred: deferred_sum_targets,
            },
        ),
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
            surrogate,
            resolved_sum_targets,
            resolved_sum_target_bindings,
        } => document::point_delete(
            ctx,
            collection,
            document_id,
            *surrogate,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
        ),
        ReplicatedWrite::PointUpdate {
            collection,
            document_id,
            updates,
            surrogate,
            resolved_sum_targets,
            resolved_sum_target_bindings,
        } => document::point_update(
            ctx,
            collection,
            document_id,
            updates,
            *surrogate,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
        ),
        ReplicatedWrite::DocUpsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate,
            resolved_sum_targets,
            resolved_sum_target_bindings,
        } => document::doc_upsert(
            ctx,
            collection,
            document_id,
            value,
            on_conflict_updates,
            *surrogate,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
        ),
        ReplicatedWrite::DocBatchInsert {
            collection,
            documents,
            surrogates,
            resolved_sum_targets,
            deferred_sum_targets,
            resolved_sum_target_bindings,
        } => document::batch_insert(
            ctx,
            collection,
            documents,
            surrogates,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
            deferred_sum_targets,
        ),
        ReplicatedWrite::DocTruncate {
            collection,
            restart_identity,
            resolved_sum_targets,
            resolved_sum_target_bindings,
        } => Ok(document::truncate(
            collection,
            *restart_identity,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
        )),
        ReplicatedWrite::BulkDml {
            collection,
            filters,
            is_update,
            updates,
            resolved_sum_targets,
            resolved_sum_target_bindings,
        } => Ok(document::bulk_dml(
            collection,
            filters,
            *is_update,
            updates,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
        )),
        ReplicatedWrite::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            source_limit,
        } => Ok(document::insert_select(
            target_collection,
            source_collection,
            source_filters,
            *source_limit,
        )),
        ReplicatedWrite::ApplyBalanceDelta {
            collection,
            document_id,
            surrogate,
            column,
            delta,
            join_column,
            join_value,
        } => Ok(document::apply_balance_delta(
            collection,
            document_id,
            *surrogate,
            column,
            delta,
            join_column,
            join_value,
        )),
        _ => Err(crate::Error::Internal {
            detail: "entry_document::decode_arm called with a non-Document ReplicatedWrite \
                variant (dispatch bug in decode/entry.rs's grouped Document match arm)"
                .into(),
        }),
    }
}
