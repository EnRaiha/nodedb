// SPDX-License-Identifier: BUSL-1.1

//! Entry point: encode a decided [`ReplicableWrite`] into a `ReplicatedEntry`
//! for Raft proposal, plus the shared provenance-encoding helper.
//!
//! `to_replicated_entry` is the single oracle deciding which `PhysicalPlan`
//! variants are proposed over Raft; exhaustive so a new variant is a compile error.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::replicable_write::ReplicableWrite;
use super::super::types::ReplicatedEntry;
use super::{
    crdt, entry_array, entry_columnar_family, entry_document, entry_graph, entry_kv, vector,
};
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId, VShardId};

/// Serialize optional sync provenance into the cross-node wire shape.
/// `.expect()`, not `.ok()`: losing provenance on a follower would defeat the
/// idempotency gate and risk double-apply.
pub(super) fn encode_provenance(
    provenance: &Option<nodedb_types::sync::wire::SyncProvenance>,
) -> Option<Vec<u8>> {
    provenance
        .as_ref()
        .map(|p| zerompk::to_msgpack_vec(p).expect("SyncProvenance serialization is infallible"))
}

/// Serialize an optional RETURNING projection spec, same infallible-encode
/// contract as `encode_provenance`: failure must fail loud, not silently
/// degrade a RETURNING request to an empty result.
pub(super) fn encode_returning(
    returning: &Option<nodedb_physical::physical_plan::ReturningSpec>,
) -> Option<Vec<u8>> {
    returning
        .as_ref()
        .map(|r| zerompk::to_msgpack_vec(r).expect("ReturningSpec serialization is infallible"))
}

/// Encode a [`ReplicableWrite`] into a `ReplicatedEntry`, or `Ok(None)` for a
/// non-replicated plan. Live-predicate refusal lives in
/// `ReplicableWrite::decide_for_replication` — nothing to re-check here.
pub fn to_replicated_entry(
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    write: &ReplicableWrite<'_>,
) -> crate::Result<Option<ReplicatedEntry>> {
    let plan = write.plan();
    let encoded = match plan {
        PhysicalPlan::Document(op) => entry_document::document_write(op),
        // Fallible: a governed predicate DML refuses rather than encode it bare.
        PhysicalPlan::Kv(op) => entry_kv::kv_write(op)?,
        // Exhaustive over their op enums — see their module docs.
        PhysicalPlan::Vector(op) => vector::encode(op),
        PhysicalPlan::Crdt(op) => crdt::encode(op),
        PhysicalPlan::Graph(op) => entry_graph::graph_write(op),
        // Fallible for the same reason as the Kv arm above.
        PhysicalPlan::Columnar(op) => entry_columnar_family::columnar_write(op)?,
        PhysicalPlan::Timeseries(op) => entry_columnar_family::timeseries_write(op),
        PhysicalPlan::Text(op) => entry_columnar_family::text_write(op),
        PhysicalPlan::Spatial(op) => entry_columnar_family::spatial_write(op),
        PhysicalPlan::Array(op) => entry_array::array_write(op),
        // Cluster-fanned array ops execute entirely on the Control Plane (`ArrayCoordinator`).
        PhysicalPlan::ClusterArray(_) => None,
        // Reads / query operators / metadata ops are never replicated writes.
        PhysicalPlan::Query(_) => None,
        PhysicalPlan::Meta(_) | PhysicalPlan::ClusterEvent(_) => None,
    };

    Ok(encoded.map(|write| {
        ReplicatedEntry::new(
            tenant_id.as_u64(),
            database_id.as_u64(),
            vshard_id.as_u32(),
            write,
        )
    }))
}
