// SPDX-License-Identifier: BUSL-1.1

//! Point-write lock-key extraction for the write-admission fast path.
//!
//! [`plan_lock_keys`] maps a physical plan to the exact deterministic lock keys
//! the fast path must hold to serialize the write against a pending
//! cross-shard commit. It returns `Some` ONLY for uncontended-eligible POINT
//! writes that home to a single vShard and carry a single stable identity;
//! every predicate / bulk / multi-home write (and every non-write) returns
//! `None`, which the gate routes to the deterministic Calvin scheduler instead.
//!
//! The single-home requirement is enforced structurally: a plan is eligible
//! only when [`plan_vshard`] resolves it to exactly one vShard. A cross-home
//! graph edge resolves to two vShards and therefore falls through to the
//! scheduler — it cannot be expressed as one `(vShard, keys)` pair.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::calvin::scheduler::driver::core::routing::{PlanRouting, plan_vshard};
use crate::control::cluster::calvin::scheduler::lock_manager::LockKey;
use crate::types::VShardId;
use nodedb_physical::physical_plan::{DocumentOp, GraphOp, KvOp, VectorOp};

/// The vShard and exact lock-key set a POINT write must hold on the fast path.
///
/// Returns `None` — routing the write to the deterministic Calvin scheduler —
/// for any plan that is not a single-home, single-identity point write:
/// predicate / bulk writes, multi-home graph edges, and non-writes.
pub(crate) fn plan_lock_keys(plan: &PhysicalPlan) -> Option<(VShardId, BTreeSet<LockKey>)> {
    // Point writes home to exactly one vShard. `plan_vshard` returns two vShards
    // for a cross-home graph edge; such an edge has no single `(vShard, keys)`
    // representation, so it is ineligible for the fast path. Any non-`Vshards`
    // routing (control-plane-only, non-write, or a known-unroutable gap) is
    // likewise ineligible — the scheduler / gate above this handles those.
    let vshard = match plan_vshard(plan) {
        PlanRouting::Vshards(v) => match v.as_slice() {
            [v] => *v,
            _ => return None,
        },
        PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => {
            return None;
        }
    };
    let key = point_lock_key(plan)?;
    let mut keys = BTreeSet::new();
    keys.insert(key);
    Some((vshard, keys))
}

/// The single deterministic lock key identifying a point write, or `None` when
/// the plan is not a single-identity point write.
///
/// The outer match is exhaustive over `PhysicalPlan` so a new engine variant
/// forces a decision here rather than silently taking the fast path.
fn point_lock_key(plan: &PhysicalPlan) -> Option<LockKey> {
    match plan {
        PhysicalPlan::Document(op) => document_point_key(op),
        PhysicalPlan::Kv(op) => kv_point_key(op),
        PhysicalPlan::Vector(op) => vector_point_key(op),
        PhysicalPlan::Graph(op) => graph_point_key(op),
        // Append-only, bulk, index-overlay, read and meta engines never carry a
        // single-identity point write: they route to the deterministic
        // scheduler, which owns their ordering.
        PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_) => None,
    }
}

/// Document-engine point-write key: the row surrogate. Predicate / multi-row
/// document writes have no single point identity and route to the scheduler.
fn document_point_key(op: &DocumentOp) -> Option<LockKey> {
    match op {
        DocumentOp::PointPut {
            collection,
            surrogate,
            ..
        }
        | DocumentOp::PointInsert {
            collection,
            surrogate,
            ..
        }
        | DocumentOp::PointDelete {
            collection,
            surrogate,
            ..
        }
        | DocumentOp::PointUpdate {
            collection,
            surrogate,
            ..
        } => Some(LockKey::Surrogate {
            collection: Arc::from(collection.as_str()),
            surrogate: surrogate.as_u32(),
        }),
        DocumentOp::BatchInsert { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::UpdateFromJoin { .. } => None,
        _ => None,
    }
}

/// KV-engine point-write key: the single raw byte key. A single-key delete is a
/// point write; multi-key delete and batch put have no single identity.
fn kv_point_key(op: &KvOp) -> Option<LockKey> {
    match op {
        KvOp::Put {
            collection, key, ..
        }
        | KvOp::Insert {
            collection, key, ..
        }
        | KvOp::InsertIfAbsent {
            collection, key, ..
        }
        | KvOp::InsertOnConflictUpdate {
            collection, key, ..
        } => Some(LockKey::Kv {
            collection: Arc::from(collection.as_str()),
            key: Arc::from(key.as_slice()),
        }),
        KvOp::Delete { collection, keys } => match keys.as_slice() {
            [k] => Some(LockKey::Kv {
                collection: Arc::from(collection.as_str()),
                key: Arc::from(k.as_slice()),
            }),
            _ => None,
        },
        KvOp::BatchPut { .. } => None,
        _ => None,
    }
}

/// Vector-engine point-write key: the row surrogate. Batch, node-id delete,
/// sparse and multi-vector writes lack a single stable surrogate identity.
fn vector_point_key(op: &VectorOp) -> Option<LockKey> {
    match op {
        VectorOp::Insert {
            collection,
            surrogate,
            ..
        }
        | VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            ..
        } => Some(LockKey::Surrogate {
            collection: Arc::from(collection.as_str()),
            surrogate: surrogate.as_u32(),
        }),
        VectorOp::BatchInsert { .. }
        | VectorOp::Delete { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. } => None,
        _ => None,
    }
}

/// Graph-engine point-write key: the directed edge identity. Single-home is
/// already guaranteed by [`plan_lock_keys`] (two-vShard edges never reach here).
fn graph_point_key(op: &GraphOp) -> Option<LockKey> {
    match op {
        GraphOp::EdgePut {
            collection,
            src_surrogate,
            dst_surrogate,
            ..
        }
        | GraphOp::EdgeDelete {
            collection,
            src_surrogate,
            dst_surrogate,
            ..
        } => Some(LockKey::Edge {
            collection: Arc::from(collection.as_str()),
            src: src_surrogate.as_u32(),
            dst: dst_surrogate.as_u32(),
        }),
        _ => None,
    }
}
