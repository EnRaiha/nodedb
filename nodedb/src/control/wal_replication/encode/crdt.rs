// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Crdt` variants into `ReplicatedWrite`.

use super::super::types::{ConstraintChangeOp, ReplicatedWrite};
use nodedb_physical::physical_plan::CrdtOp;

/// Encode a `CrdtOp` write variant into its `ReplicatedWrite` wire shape.
///
/// Exhaustive over `CrdtOp` (not a catch-all): a new variant forces an
/// explicit decision here instead of silently falling through, mirroring
/// `vector::encode`'s exhaustiveness guarantee.
///
/// `SetConstraints` / `DropConstraints` encode to `ReplicatedWrite::
/// ConstraintChange` so a constraint change installs on every follower's CRDT
/// validator immediately on the write, closing the window in which the leader
/// enforces a constraint its followers do not. The `constraint_reconcile`
/// bootstrap path stays the catch-up safety net for a lagging/new replica;
/// both are fenced idempotent by the monotonic `constraint_version`.
///
/// Returns `None` for the read-only / DDL-observability variants (`Read`,
/// `ReadConstraints`, `SetPolicy`, `GetPolicy`, `ReadAtVersion`,
/// `GetVersionVector`, `ExportDelta`, `CompactAtVersion`) and for
/// `RestoreToVersion`. `RestoreToVersion` is deliberately not encoded here: the
/// restore path replicates its effect as a forward delta wrapped in
/// `CrdtOp::Apply`, which then follows the normal apply replication route.
/// Encoding the restore op directly would double-apply the change and is
/// non-deterministic across replicas.
pub(super) fn encode(op: &CrdtOp) -> Option<ReplicatedWrite> {
    Some(match op {
        CrdtOp::Apply {
            collection,
            document_id,
            delta,
            peer_id,
            mutation_id: _,
            surrogate: _,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
        } => apply(
            collection,
            document_id,
            delta,
            *peer_id,
            super::entry::encode_provenance(provenance),
            *constraint_version_required,
            *expected_frontier_digest,
        ),
        CrdtOp::ApplyAuthenticated {
            collection,
            document_id,
            delta,
            peer_id,
            mutation_id: _,
            surrogate: _,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
        } => ReplicatedWrite::CrdtApplyAuthenticated {
            collection: collection.clone(),
            document_id: document_id.clone(),
            delta: delta.clone(),
            peer_id: *peer_id,
            provenance: super::entry::encode_provenance(&Some(provenance.clone())),
            constraint_version_required: *constraint_version_required,
            expected_frontier_digest: *expected_frontier_digest,
            auth_user_id: *auth_user_id,
            auth_device_id: *auth_device_id,
            auth_seq_no: *auth_seq_no,
            delta_signature: *delta_signature,
            signing_required: *signing_required,
        },
        CrdtOp::ImportSnapshot {
            tenant_id,
            collection,
            bytes,
        } => import_snapshot(*tenant_id, collection, bytes),
        CrdtOp::ListInsert {
            collection,
            document_id,
            list_path,
            index,
            fields_json,
            surrogate: _,
        } => list_insert(collection, document_id, list_path, *index, fields_json),
        CrdtOp::ListDelete {
            collection,
            document_id,
            list_path,
            index,
            surrogate: _,
        } => list_delete(collection, document_id, list_path, *index),
        CrdtOp::ListMove {
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
            surrogate: _,
        } => list_move(collection, document_id, list_path, *from_index, *to_index),
        CrdtOp::DocUpsert {
            collection,
            document_id,
            fields_json,
            surrogate,
            partial,
            returning: _,
            rls_filters: _,
        } => doc_upsert(
            collection,
            document_id,
            surrogate.as_u32(),
            fields_json,
            *partial,
        ),
        CrdtOp::DocDelete {
            collection,
            document_id,
            surrogate,
            returning: _,
            rls_filters: _,
        } => doc_delete(collection, document_id, surrogate.as_u32()),
        CrdtOp::SetConstraints {
            collection,
            constraint_version,
            constraints,
        } => set_constraints(collection, *constraint_version, constraints),
        CrdtOp::DropConstraints {
            collection,
            constraint_version,
        } => drop_constraints(collection, *constraint_version),
        CrdtOp::Read { .. }
        | CrdtOp::PreviewApply { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::CompactAtVersion { .. }
        | CrdtOp::RestoreToVersion { .. } => return None,
    })
}

/// Encode `SetConstraints` as a `ConstraintChange` install. The full constraint
/// blob set is carried verbatim; the apply path fences on `constraint_version`.
pub(super) fn set_constraints(
    collection: &str,
    constraint_version: u64,
    constraints: &[Vec<u8>],
) -> ReplicatedWrite {
    ReplicatedWrite::ConstraintChange {
        collection: collection.to_owned(),
        op: ConstraintChangeOp::Set,
        constraint_version,
        constraints: constraints.to_vec(),
    }
}

/// Encode `DropConstraints` as a `ConstraintChange` removal — no blobs, fenced
/// by `constraint_version` exactly as the install is.
pub(super) fn drop_constraints(collection: &str, constraint_version: u64) -> ReplicatedWrite {
    ReplicatedWrite::ConstraintChange {
        collection: collection.to_owned(),
        op: ConstraintChangeOp::Drop,
        constraint_version,
        constraints: Vec::new(),
    }
}

pub(super) fn apply(
    collection: &str,
    document_id: &str,
    delta: &[u8],
    peer_id: u64,
    provenance: Option<Vec<u8>>,
    constraint_version_required: u64,
    expected_frontier_digest: Option<[u8; 32]>,
) -> ReplicatedWrite {
    match expected_frontier_digest {
        Some(expected_frontier_digest) => ReplicatedWrite::CrdtApplyFenced {
            collection: collection.to_owned(),
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
        },
        None => ReplicatedWrite::CrdtApply {
            collection: collection.to_owned(),
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            provenance,
            constraint_version_required,
        },
    }
}

pub(super) fn import_snapshot(tenant_id: u64, collection: &str, bytes: &[u8]) -> ReplicatedWrite {
    ReplicatedWrite::CrdtImportCollection {
        tenant_id,
        collection: collection.to_owned(),
        bytes: bytes.to_vec(),
    }
}

/// `index` is the Data Plane's `usize` list position, widened losslessly to
/// the wire's `u64` (every supported target's `usize` fits in `u64`).
pub(super) fn list_insert(
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
    fields_json: &str,
) -> ReplicatedWrite {
    ReplicatedWrite::CrdtListInsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: index as u64,
        fields_json: fields_json.to_owned(),
    }
}

/// See [`list_insert`] for the `index` widening note.
pub(super) fn list_delete(
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
) -> ReplicatedWrite {
    ReplicatedWrite::CrdtListDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: index as u64,
    }
}

/// See [`list_insert`] for the `index` widening note (applies to both
/// `from_index` and `to_index` here).
pub(super) fn list_move(
    collection: &str,
    document_id: &str,
    list_path: &str,
    from_index: usize,
    to_index: usize,
) -> ReplicatedWrite {
    ReplicatedWrite::CrdtListMove {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        from_index: from_index as u64,
        to_index: to_index as u64,
    }
}

pub(super) fn doc_upsert(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    fields_json: &str,
    partial: bool,
) -> ReplicatedWrite {
    ReplicatedWrite::CrdtDocUpsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
        fields_json: fields_json.to_owned(),
        partial,
    }
}

pub(super) fn doc_delete(collection: &str, document_id: &str, surrogate: u32) -> ReplicatedWrite {
    ReplicatedWrite::CrdtDocDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
    }
}
