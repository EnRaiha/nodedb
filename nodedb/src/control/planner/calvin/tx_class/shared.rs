// SPDX-License-Identifier: BUSL-1.1

//! Write-set-identity extractors shared by the static and dependent `TxClass`
//! builders.

use crate::control::server::shared::session::read_set::{ReadKey, ReadSetEntry};
use nodedb_cluster::calvin::types::{ReadKeyIdent, VersionedReadEntry, VersionedReadSet};
use nodedb_physical::physical_plan::{
    DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};

/// Map the neutral session read-set into the replicated, LSN-versioned
/// [`VersionedReadSet`] carried on the `TxClass`.
///
/// Each [`ReadSetEntry`] becomes one [`VersionedReadEntry`], preserving the
/// engine, collection, per-shard `read_lsn`, and the point/predicate
/// distinction. The entry's `(database_id, tenant_id)` scope is not re-carried
/// per entry: the enclosing `TxClass` already scopes the tenant.
///
/// Own-overlay (read-your-own-write) exclusion is a capture-time concern (a
/// read satisfied by the txn's own staged writes is never recorded, and a
/// mixed committed-base + staged read records only the committed portion) — it
/// cannot be reconstructed here from key identity alone, so this mapping is a
/// faithful 1:1 projection of whatever the session captured.
pub(super) fn versioned_reads_from(reads: &[ReadSetEntry]) -> VersionedReadSet {
    VersionedReadSet::new(
        reads
            .iter()
            .map(|entry| VersionedReadEntry {
                engine: entry.engine,
                collection: entry.collection.clone(),
                key: match &entry.key {
                    ReadKey::Point { repr } => ReadKeyIdent::Point(repr.clone()),
                    ReadKey::Predicate => ReadKeyIdent::Predicate,
                },
                read_lsn: entry.read_lsn,
            })
            .collect(),
    )
}

/// Extract `(collection, raw byte keys)` from a KV write plan, or `None` for a
/// KV op with no statically-known point keys (e.g. `BatchPut`).
pub(super) fn kv_write_keys(op: &KvOp) -> Option<(String, Vec<Vec<u8>>)> {
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
        } => Some((collection.clone(), vec![key.clone()])),
        KvOp::Delete { collection, keys } => Some((collection.clone(), keys.clone())),
        _ => None,
    }
}

/// Extract `(collection, surrogates)` from a Vector write plan, or `None` for a
/// Vector op with no statically-known surrogate identity (e.g. node-id delete).
pub(super) fn vector_write_surrogates(op: &VectorOp) -> Option<(String, Vec<u32>)> {
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
        } => Some((collection.clone(), vec![surrogate.as_u32()])),
        VectorOp::BatchInsert {
            collection,
            surrogates,
            ..
        } => Some((
            collection.clone(),
            surrogates.iter().map(|s| s.as_u32()).collect(),
        )),
        _ => None,
    }
}

/// Extract the collection name from a write plan.
pub(crate) fn collection_name_from_plan(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut { collection, .. }
            | DocumentOp::PointInsert { collection, .. }
            | DocumentOp::PointDelete { collection, .. }
            | DocumentOp::PointUpdate { collection, .. }
            | DocumentOp::BatchInsert { collection, .. }
            | DocumentOp::Upsert { collection, .. }
            | DocumentOp::BulkUpdate { collection, .. }
            | DocumentOp::BulkDelete { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Kv(
            KvOp::Put { collection, .. }
            | KvOp::Insert { collection, .. }
            | KvOp::InsertIfAbsent { collection, .. }
            | KvOp::InsertOnConflictUpdate { collection, .. }
            | KvOp::Delete { collection, .. }
            | KvOp::BatchPut { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Vector(
            VectorOp::Insert { collection, .. }
            | VectorOp::BatchInsert { collection, .. }
            | VectorOp::Delete { collection, .. }
            | VectorOp::DeleteBySurrogate { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Graph(
            GraphOp::EdgePut { collection, .. } | GraphOp::EdgeDelete { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => collection.clone(),
        _ => String::new(),
    }
}

/// Extract a surrogate from a write plan (returns 0 when unavailable).
pub(super) fn surrogate_from_plan(plan: &PhysicalPlan) -> u32 {
    match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut { surrogate, .. }
            | DocumentOp::PointInsert { surrogate, .. }
            | DocumentOp::PointDelete { surrogate, .. }
            | DocumentOp::PointUpdate { surrogate, .. },
        ) => surrogate.as_u32(),
        _ => 0,
    }
}
