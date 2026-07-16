// SPDX-License-Identifier: BUSL-1.1

//! Write-set-identity extractors shared by the static and dependent `TxClass`
//! builders.

use crate::control::server::shared::session::read_set::{ReadKey, ReadSetEntry};
use nodedb_cluster::calvin::types::{
    EngineKeySet, EngineTag, ReadKeyIdent, ReadWriteSet, SortedVec, VersionedReadEntry,
    VersionedReadSet,
};
use nodedb_physical::physical_plan::{
    DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};

/// Map the neutral session read-set into the replicated, LSN-versioned
/// [`VersionedReadSet`] carried on the `TxClass`.
///
/// Each [`ReadSetEntry`] becomes one [`VersionedReadEntry`], preserving the
/// engine, collection, per-collection `read_version_lsn` (the read collection's
/// write floor, a WAL LSN — the sound cross-shard OCC comparand, not the
/// core-global `read_lsn`), and the point/predicate
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
                    ReadKey::IndexEq { field, value } => ReadKeyIdent::IndexEq {
                        field: field.clone(),
                        value: value.clone(),
                    },
                    ReadKey::IndexRange { field, lo, hi } => ReadKeyIdent::IndexRange {
                        field: field.clone(),
                        lo: lo.clone(),
                        hi: hi.clone(),
                    },
                },
                read_lsn: entry.read_version_lsn,
            })
            .collect(),
    )
}

/// Build the routing/identity `read_set` for a Calvin `TxClass` from the
/// neutral session read-set.
///
/// This populates the key-IDENTITY set used for participant derivation and
/// routing — NOT the LSN-versioned OCC validation set (`versioned_reads`, built
/// separately by [`versioned_reads_from`]).
///
/// Each [`ReadSetEntry`] carries only `(engine, collection)` plus a
/// point/predicate marker — no surrogate or byte-key identity — so it maps to a
/// COLLECTION-homed [`EngineKeySet`] with an empty key vector, and its
/// participating vShard is derived from the collection name. This intentionally
/// over-approximates participants at collection granularity. For a graph/edge
/// read it is REQUIRED and SAFE: a `ReadSetEntry` has no endpoint homes, so an
/// `EngineKeySet::Edge` (which routes by key-hashed endpoint homes) cannot be
/// built — collection-homing adds participant shards (more validation) and never
/// drops one. A read with no extractable collection contributes no participant.
pub(super) fn read_set_from(reads: &[ReadSetEntry]) -> ReadWriteSet {
    use std::collections::BTreeSet;

    // Dedup by (engine-variant, collection): identity is empty, so many reads on
    // one collection collapse to a single keyset, keeping the read_set that
    // rides the Raft log compact. Vector and KV reads keep their engine variant;
    // every other engine (Document plus the graph / FTS / columnar / ... overlays
    // and column-family engines) routes by collection name and is collection-homed
    // via a Document keyset.
    let mut vector_colls: BTreeSet<String> = BTreeSet::new();
    let mut kv_colls: BTreeSet<String> = BTreeSet::new();
    let mut doc_colls: BTreeSet<String> = BTreeSet::new();
    for entry in reads {
        if entry.collection.is_empty() {
            continue;
        }
        match entry.engine {
            EngineTag::Vector => {
                vector_colls.insert(entry.collection.clone());
            }
            EngineTag::Kv => {
                kv_colls.insert(entry.collection.clone());
            }
            EngineTag::Document
            | EngineTag::Graph
            | EngineTag::Text
            | EngineTag::Columnar
            | EngineTag::Timeseries
            | EngineTag::Spatial
            | EngineTag::Crdt
            | EngineTag::Query
            | EngineTag::Meta
            | EngineTag::Array
            | EngineTag::ClusterArray => {
                doc_colls.insert(entry.collection.clone());
            }
        }
    }
    let mut sets: Vec<EngineKeySet> = Vec::new();
    for collection in vector_colls {
        sets.push(EngineKeySet::Vector {
            collection,
            surrogates: SortedVec::new(vec![]),
        });
    }
    for collection in kv_colls {
        sets.push(EngineKeySet::Kv {
            collection,
            keys: SortedVec::new(vec![]),
        });
    }
    for collection in doc_colls {
        sets.push(EngineKeySet::Document {
            collection,
            surrogates: SortedVec::new(vec![]),
        });
    }
    ReadWriteSet::new(sets)
}

/// Extract `(collection, raw byte keys)` from a KV write plan, or `None` for a
/// KV op with no statically-known point keys (e.g. `BatchPut`). Single-key
/// read-modify-write ops (`Incr`/`IncrFloat`/`Cas`/`GetSet`/`FieldSet`) key on
/// the same `(collection, key)` pair as `Put`/`Insert`, so they sequence
/// identically to the write-admission gate's `kv_point_key`.
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
        }
        | KvOp::Incr {
            collection, key, ..
        }
        | KvOp::IncrFloat {
            collection, key, ..
        }
        | KvOp::Cas {
            collection, key, ..
        }
        | KvOp::GetSet {
            collection, key, ..
        }
        | KvOp::FieldSet {
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
            | KvOp::BatchPut { collection, .. }
            | KvOp::Incr { collection, .. }
            | KvOp::IncrFloat { collection, .. }
            | KvOp::Cas { collection, .. }
            | KvOp::GetSet { collection, .. }
            | KvOp::FieldSet { collection, .. },
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
            | DocumentOp::PointUpdate { surrogate, .. }
            | DocumentOp::Upsert { surrogate, .. },
        ) => surrogate.as_u32(),
        _ => 0,
    }
}

/// Lockstep proof that the write-admission gate and the Calvin scheduler
/// derive IDENTICAL lock keys for the same op. `plan_lock_keys` (gate side)
/// and `kv_write_keys`/`surrogate_from_plan` -> `EngineKeySet` (scheduler
/// side, via `build_single_vshard_tx_class`) must never diverge — if they
/// did, a gate-fenced write and a sequenced txn would lock different keys
/// and the write-ordering fix this module exists for would be void.
#[cfg(test)]
mod lockstep_tests {
    use super::*;
    use crate::control::cluster::calvin::scheduler::lock_manager::LockKey;
    use crate::control::planner::calvin::tx_class::static_builder::build_single_vshard_tx_class;
    use crate::control::server::shared::write_admission::lock_keys::plan_lock_keys;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_cluster::calvin::types::EngineKeySet;
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
    use nodedb_types::Surrogate;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn task(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    /// Mirrors the scheduler driver's `expand_rw_set` `EngineKeySet` ->
    /// `LockKey` mapping. This half is fixed, engine-tag-driven translation
    /// that does not vary per op; the property under test is whether the
    /// extractor threads the SAME `(collection, key/surrogate)` the gate's
    /// `plan_lock_keys` uses, not this mapping itself.
    fn scheduler_lock_keys(sets: &[EngineKeySet]) -> BTreeSet<LockKey> {
        let mut keys = BTreeSet::new();
        for ks in sets {
            match ks {
                EngineKeySet::Document {
                    collection,
                    surrogates,
                }
                | EngineKeySet::Vector {
                    collection,
                    surrogates,
                } => {
                    let coll: Arc<str> = Arc::from(collection.as_str());
                    for &surrogate in surrogates.iter() {
                        keys.insert(LockKey::Surrogate {
                            collection: Arc::clone(&coll),
                            surrogate,
                        });
                    }
                }
                EngineKeySet::Kv {
                    collection,
                    keys: kv_keys,
                } => {
                    let coll: Arc<str> = Arc::from(collection.as_str());
                    for k in kv_keys.iter() {
                        keys.insert(LockKey::Kv {
                            collection: Arc::clone(&coll),
                            key: Arc::from(k.as_slice()),
                        });
                    }
                }
                EngineKeySet::Edge {
                    collection, edges, ..
                } => {
                    let coll: Arc<str> = Arc::from(collection.as_str());
                    for &(src, dst) in edges.iter() {
                        keys.insert(LockKey::Edge {
                            collection: Arc::clone(&coll),
                            src,
                            dst,
                        });
                    }
                }
            }
        }
        keys
    }

    fn assert_gate_matches_scheduler(plan: PhysicalPlan) {
        let t = task(plan);
        let (_, gate_keys) =
            plan_lock_keys(&t.plan).expect("op must be fast-path eligible for this test");
        let tx = build_single_vshard_tx_class(&[t], TenantId::new(1), &[])
            .expect("valid single-vshard TxClass");
        let scheduler_keys = scheduler_lock_keys(&tx.write_set.0);
        assert_eq!(
            gate_keys, scheduler_keys,
            "gate and scheduler must lock the identical key set"
        );
    }

    #[test]
    fn kv_incr_gate_key_matches_scheduler_key() {
        assert_gate_matches_scheduler(PhysicalPlan::Kv(KvOp::Incr {
            collection: "counters".to_owned(),
            key: b"ctr".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: Surrogate::new(3),
        }));
    }

    #[test]
    fn kv_cas_gate_key_matches_scheduler_key() {
        assert_gate_matches_scheduler(PhysicalPlan::Kv(KvOp::Cas {
            collection: "counters".to_owned(),
            key: b"ctr".to_vec(),
            expected: vec![],
            new_value: vec![],
            surrogate: Surrogate::new(3),
        }));
    }

    #[test]
    fn document_upsert_gate_key_matches_scheduler_key() {
        assert_gate_matches_scheduler(PhysicalPlan::Document(DocumentOp::Upsert {
            collection: "docs".to_owned(),
            document_id: "d1".to_owned(),
            value: vec![],
            on_conflict_updates: vec![],
            surrogate: Surrogate::new(9),
        }));
    }
}
