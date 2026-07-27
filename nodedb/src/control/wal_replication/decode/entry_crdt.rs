// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Crdt`.
//!
//! Delegated from `decode/entry.rs`'s single grouped match arm. `write` is
//! guaranteed by the caller to already be one of these variants — see
//! `entry_document::decode_arm` for the trailing-arm contract.

use super::super::types::ReplicatedWrite;
use super::crdt;
use super::ctx::DecodeCtx;
use crate::bridge::envelope::PhysicalPlan;

pub(super) fn decode_arm(ctx: &DecodeCtx, write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::CrdtApply {
            collection,
            document_id,
            delta,
            peer_id,
            provenance,
            constraint_version_required,
        } => crdt::apply(
            ctx,
            crdt::ApplyArgs {
                collection,
                document_id,
                delta,
                peer_id: *peer_id,
                provenance_bytes: provenance,
                constraint_version_required: *constraint_version_required,
                expected_frontier_digest: None,
                auth_user_id: 0,
                auth_device_id: 0,
                auth_seq_no: 0,
                delta_signature: [0; 32],
                signing_required: false,
                authenticated: false,
            },
        ),
        ReplicatedWrite::CrdtApplyFenced {
            collection,
            document_id,
            delta,
            peer_id,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
        } => crdt::apply(
            ctx,
            crdt::ApplyArgs {
                collection,
                document_id,
                delta,
                peer_id: *peer_id,
                provenance_bytes: provenance,
                constraint_version_required: *constraint_version_required,
                expected_frontier_digest: Some(*expected_frontier_digest),
                auth_user_id: 0,
                auth_device_id: 0,
                auth_seq_no: 0,
                delta_signature: [0; 32],
                signing_required: false,
                authenticated: false,
            },
        ),
        ReplicatedWrite::CrdtApplyAuthenticated {
            collection,
            document_id,
            delta,
            peer_id,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
        } => crdt::apply(
            ctx,
            crdt::ApplyArgs {
                collection,
                document_id,
                delta,
                peer_id: *peer_id,
                provenance_bytes: provenance,
                constraint_version_required: *constraint_version_required,
                expected_frontier_digest: *expected_frontier_digest,
                auth_user_id: *auth_user_id,
                auth_device_id: *auth_device_id,
                auth_seq_no: *auth_seq_no,
                delta_signature: *delta_signature,
                signing_required: *signing_required,
                authenticated: true,
            },
        ),
        ReplicatedWrite::CrdtImportCollection {
            tenant_id,
            collection,
            bytes,
        } => Ok(crdt::import_collection(*tenant_id, collection, bytes)),
        ReplicatedWrite::CrdtListInsert {
            collection,
            document_id,
            list_path,
            index,
            fields_json,
        } => crdt::list_insert(collection, document_id, list_path, *index, fields_json),
        ReplicatedWrite::CrdtListDelete {
            collection,
            document_id,
            list_path,
            index,
        } => crdt::list_delete(collection, document_id, list_path, *index),
        ReplicatedWrite::CrdtListMove {
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
        } => crdt::list_move(collection, document_id, list_path, *from_index, *to_index),
        ReplicatedWrite::CrdtDocUpsert {
            collection,
            document_id,
            surrogate,
            fields_json,
            partial,
        } => Ok(crdt::doc_upsert(
            collection,
            document_id,
            *surrogate,
            fields_json,
            *partial,
        )),
        ReplicatedWrite::CrdtDocDelete {
            collection,
            document_id,
            surrogate,
        } => Ok(crdt::doc_delete(collection, document_id, *surrogate)),
        ReplicatedWrite::ConstraintChange {
            collection,
            op,
            constraint_version,
            constraints,
        } => Ok(crdt::constraint_change(
            collection,
            op,
            *constraint_version,
            constraints,
        )),
        _ => Err(crate::Error::Internal {
            detail: "entry_crdt::decode_arm called with a non-Crdt ReplicatedWrite variant \
                (dispatch bug in decode/entry.rs's grouped Crdt match arm)"
                .into(),
        }),
    }
}
