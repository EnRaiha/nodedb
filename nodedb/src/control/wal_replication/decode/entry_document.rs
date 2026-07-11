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

use super::super::types::ReplicatedWrite;
use super::ctx::DecodeCtx;
use super::document;
use crate::bridge::envelope::PhysicalPlan;

pub(super) fn decode_arm(ctx: &DecodeCtx, write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
            surrogate,
        } => document::point_put(ctx, collection, document_id, value, *surrogate),
        ReplicatedWrite::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
        } => document::point_insert(ctx, collection, document_id, value, *if_absent, *surrogate),
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
            surrogate,
        } => document::point_delete(ctx, collection, document_id, *surrogate),
        ReplicatedWrite::PointUpdate {
            collection,
            document_id,
            updates,
            surrogate,
        } => document::point_update(ctx, collection, document_id, updates, *surrogate),
        ReplicatedWrite::DocUpsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate,
        } => document::doc_upsert(
            ctx,
            collection,
            document_id,
            value,
            on_conflict_updates,
            *surrogate,
        ),
        ReplicatedWrite::DocBatchInsert {
            collection,
            documents,
            surrogates,
        } => document::batch_insert(ctx, collection, documents, surrogates),
        ReplicatedWrite::DocTruncate {
            collection,
            restart_identity,
        } => Ok(document::truncate(collection, *restart_identity)),
        ReplicatedWrite::BulkDml {
            collection,
            filters,
            is_update,
            updates,
        } => Ok(document::bulk_dml(collection, filters, *is_update, updates)),
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
        _ => Err(crate::Error::Internal {
            detail: "entry_document::decode_arm called with a non-Document ReplicatedWrite \
                variant (dispatch bug in decode/entry.rs's grouped Document match arm)"
                .into(),
        }),
    }
}
