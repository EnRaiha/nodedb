// SPDX-License-Identifier: BUSL-1.1

//! Entry point: encode a write-side `PhysicalPlan` into a `ReplicatedEntry`
//! for Raft proposal, plus the shared provenance-encoding helper.
//!
//! `to_replicated_entry` is the single oracle deciding which `PhysicalPlan`
//! variants are proposed over Raft. The top-level match is exhaustive over
//! every `PhysicalPlan` variant, and each engine's per-op classification is
//! delegated to an equally-exhaustive `*_write` helper (or, for Vector/Crdt,
//! the pre-existing `vector::encode` / `crdt::encode`). A new variant anywhere
//! in this tree is a compile error here — never a silent omission that would
//! leave a new write un-replicated. Mirrors the technique used by
//! `plan_vshard` (`control/cluster/calvin/scheduler/driver/core/routing.rs`)
//! and `is_write_plan` (`control/planner/calvin/write_class.rs`).

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedEntry;
use super::{
    crdt, entry_array, entry_columnar_family, entry_document, entry_graph, entry_kv, vector,
};
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId, VShardId};

/// Serialize optional sync provenance into the cross-node wire shape.
///
/// `SyncProvenance` is a plain POD struct (producer_id / epoch / stream_id /
/// seq); its msgpack encoding is infallible — the same contract the
/// `geometry_bytes` encoding relies on. We `.expect()` rather than silently
/// dropping provenance with `.ok()`: losing provenance on a follower would
/// defeat the idempotency gate and risk double-apply, so a (theoretical)
/// encode failure must fail loud, not replicate `None`.
pub(super) fn encode_provenance(
    provenance: &Option<nodedb_types::sync::wire::SyncProvenance>,
) -> Option<Vec<u8>> {
    provenance
        .as_ref()
        .map(|p| zerompk::to_msgpack_vec(p).expect("SyncProvenance serialization is infallible"))
}

pub fn to_replicated_entry(
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
) -> Option<ReplicatedEntry> {
    let write = match plan {
        PhysicalPlan::Document(op) => entry_document::document_write(op),
        PhysicalPlan::Kv(op) => entry_kv::kv_write(op),
        // `vector::encode` / `crdt::encode` are exhaustive over their op enums
        // (each returns `None` for reads and still-unencoded variants) — see
        // their module docs.
        PhysicalPlan::Vector(op) => vector::encode(op),
        PhysicalPlan::Crdt(op) => crdt::encode(op),
        PhysicalPlan::Graph(op) => entry_graph::graph_write(op),
        PhysicalPlan::Columnar(op) => entry_columnar_family::columnar_write(op),
        PhysicalPlan::Timeseries(op) => entry_columnar_family::timeseries_write(op),
        PhysicalPlan::Text(op) => entry_columnar_family::text_write(op),
        PhysicalPlan::Spatial(op) => entry_columnar_family::spatial_write(op),
        PhysicalPlan::Array(op) => entry_array::array_write(op),
        // Cluster-fanned-out array ops execute entirely on the Control Plane
        // (`ArrayCoordinator`); they are never a single-shard Raft proposal
        // from here.
        PhysicalPlan::ClusterArray(_) => None,
        // Reads / query operators / metadata ops are never replicated writes.
        PhysicalPlan::Query(_) => None,
        PhysicalPlan::Meta(_) | PhysicalPlan::ClusterEvent(_) => None,
    };

    write.map(|write| {
        ReplicatedEntry::new(
            tenant_id.as_u64(),
            database_id.as_u64(),
            vshard_id.as_u32(),
            write,
        )
    })
}
