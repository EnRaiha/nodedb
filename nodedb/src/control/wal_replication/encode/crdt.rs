// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Crdt` variants into `ReplicatedWrite`.

use super::super::types::ReplicatedWrite;

pub(super) fn apply(
    collection: &str,
    document_id: &str,
    delta: &[u8],
    peer_id: u64,
    provenance: Option<Vec<u8>>,
    constraint_version_required: u64,
) -> ReplicatedWrite {
    ReplicatedWrite::CrdtApply {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        delta: delta.to_vec(),
        peer_id,
        provenance,
        constraint_version_required,
    }
}

pub(super) fn import_snapshot(tenant_id: u64, collection: &str, bytes: &[u8]) -> ReplicatedWrite {
    ReplicatedWrite::CrdtImportCollection {
        tenant_id,
        collection: collection.to_owned(),
        bytes: bytes.to_vec(),
    }
}
