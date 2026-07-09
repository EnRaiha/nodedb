// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Crdt`.

use super::super::decode_sync_engines;
use super::super::types::ConstraintChangeOp;
use super::ctx::{DecodeCtx, assign_or_zero};
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::CrdtOp;

pub(super) fn apply(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    delta: &[u8],
    peer_id: u64,
    provenance_bytes: &Option<Vec<u8>>,
    constraint_version_required: u64,
) -> crate::Result<PhysicalPlan> {
    let surrogate = assign_or_zero(ctx, collection, document_id.as_bytes())?;
    let provenance = decode_sync_engines::decode_provenance(provenance_bytes)?;
    Ok(PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        delta: delta.to_vec(),
        peer_id,
        mutation_id: 0,
        surrogate,
        provenance,
        constraint_version_required,
    }))
}

/// Per-collection Loro doc import — no surrogate, no provenance. Every
/// replica applies the same snapshot via the same idempotent Loro merge,
/// converging deterministically.
pub(super) fn import_collection(tenant_id: u64, collection: &str, bytes: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
        tenant_id,
        collection: collection.to_owned(),
        bytes: bytes.to_vec(),
    })
}

pub(super) fn constraint_change(
    collection: &str,
    op: &ConstraintChangeOp,
    constraint_version: u64,
    constraints: &[Vec<u8>],
) -> PhysicalPlan {
    match op {
        ConstraintChangeOp::Set => PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection: collection.to_owned(),
            constraint_version,
            constraints: constraints.to_vec(),
        }),
        ConstraintChangeOp::Drop => PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection: collection.to_owned(),
            constraint_version,
        }),
    }
}
