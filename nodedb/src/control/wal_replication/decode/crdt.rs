// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Crdt`.

use super::super::decode_sync_engines;
use super::super::types::ConstraintChangeOp;
use super::ctx::{DecodeCtx, assign_or_zero};
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::CrdtOp;

pub(super) struct ApplyArgs<'a> {
    pub(super) collection: &'a str,
    pub(super) document_id: &'a str,
    pub(super) delta: &'a [u8],
    pub(super) peer_id: u64,
    pub(super) provenance_bytes: &'a Option<Vec<u8>>,
    pub(super) constraint_version_required: u64,
    pub(super) expected_frontier_digest: Option<[u8; 32]>,
    pub(super) auth_user_id: u64,
    pub(super) auth_device_id: u64,
    pub(super) auth_seq_no: u64,
    pub(super) delta_signature: [u8; 32],
    pub(super) signing_required: bool,
    pub(super) authenticated: bool,
}

pub(super) fn apply(ctx: &DecodeCtx, args: ApplyArgs<'_>) -> crate::Result<PhysicalPlan> {
    let ApplyArgs {
        collection,
        document_id,
        delta,
        peer_id,
        provenance_bytes,
        constraint_version_required,
        expected_frontier_digest,
        auth_user_id,
        auth_device_id,
        auth_seq_no,
        delta_signature,
        signing_required,
        authenticated,
    } = args;
    let surrogate = assign_or_zero(ctx, collection, document_id.as_bytes())?;
    let provenance = decode_sync_engines::decode_provenance(provenance_bytes)?;
    if authenticated {
        let provenance = provenance.ok_or_else(|| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: "authenticated CRDT replay is missing sync provenance".into(),
        })?;
        Ok(PhysicalPlan::Crdt(CrdtOp::ApplyAuthenticated {
            collection: collection.to_owned(),
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            mutation_id: 0,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
        }))
    } else {
        Ok(PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: collection.to_owned(),
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            mutation_id: 0,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
        }))
    }
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

/// Narrow a wire `u64` list position/index to the `usize` the live
/// `execute_crdt_list_*` handlers take. `usize::try_from` (never `as`): a
/// value that doesn't fit `usize` on this platform is a corrupt/incompatible
/// wire payload, not a value to silently truncate and replay at the wrong
/// position.
fn list_index(field: &str, value: u64) -> crate::Result<usize> {
    usize::try_from(value).map_err(|_| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("CrdtList {field}={value} does not fit usize on this platform"),
    })
}

/// Reconstruct `CrdtOp::ListInsert` from its wire intent. No assigner call:
/// the live dispatch handler
/// (`data/executor/dispatch/crdt.rs::CrdtOp::ListInsert`) ignores the
/// `surrogate` field entirely, so `Surrogate::ZERO` carries no
/// replay-relevant information here.
pub(super) fn list_insert(
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: u64,
    fields_json: &str,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Crdt(CrdtOp::ListInsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: list_index("index", index)?,
        fields_json: fields_json.to_owned(),
        surrogate: nodedb_types::Surrogate::ZERO,
    }))
}

/// Reconstruct `CrdtOp::ListDelete` from its wire intent. See
/// [`list_insert`] for the surrogate note.
pub(super) fn list_delete(
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: u64,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Crdt(CrdtOp::ListDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: list_index("index", index)?,
        surrogate: nodedb_types::Surrogate::ZERO,
    }))
}

/// Reconstruct `CrdtOp::ListMove` from its wire intent. `from_index` and
/// `to_index` are narrowed independently so a value that fits one but not
/// the other still surfaces as a typed decode error rather than silently
/// substituting. See [`list_insert`] for the surrogate note.
pub(super) fn list_move(
    collection: &str,
    document_id: &str,
    list_path: &str,
    from_index: u64,
    to_index: u64,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Crdt(CrdtOp::ListMove {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        from_index: list_index("from_index", from_index)?,
        to_index: list_index("to_index", to_index)?,
        surrogate: nodedb_types::Surrogate::ZERO,
    }))
}

/// Reconstruct `CrdtOp::DocUpsert` from its wire intent. Unlike the block-list
/// ops, the row's own top-level `surrogate` is carried across the wire and
/// rebuilt via `Surrogate::new` — the live dispatch handler uses it to gate +
/// key the sparse-store materialization.
pub(super) fn doc_upsert(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    fields_json: &str,
    partial: bool,
) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::DocUpsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        fields_json: fields_json.to_owned(),
        surrogate: nodedb_types::Surrogate::new(surrogate),
        partial,
        returning: None,
        rls_filters: Vec::new(),
    })
}

/// Reconstruct `CrdtOp::DocDelete` from its wire intent. See [`doc_upsert`]
/// for the surrogate note.
pub(super) fn doc_delete(collection: &str, document_id: &str, surrogate: u32) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::DocDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate: nodedb_types::Surrogate::new(surrogate),
        returning: None,
        rls_filters: Vec::new(),
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
