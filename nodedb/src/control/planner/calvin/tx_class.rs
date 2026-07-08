// SPDX-License-Identifier: BUSL-1.1

//! `TxClass` construction for Calvin dispatch.
//!
//! Builds the replicated transaction descriptor (`TxClass`) from a physical
//! task slice: the per-engine write set (`EngineKeySet` — document / vector
//! surrogates, KV raw keys, graph-edge identity + routing homes) plus the
//! msgpack-encoded plans. Two builders:
//!
//! - [`build_static_tx_class`] — every write key is known upfront.
//! - [`build_dependent_tx_class`] — the OLLP collection's write set comes from
//!   reconnaissance-predicted surrogates; all other tasks use static extraction.

use crate::Error;
use crate::control::planner::calvin::dispatch::is_write_plan;
use crate::types::VShardId;
use nodedb_cluster::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
use nodedb_physical::physical_plan::{
    DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::TenantId;

/// Build a `TxClass` from a static write task slice.
///
/// Extracts each write task's deterministic identity into the matching
/// `EngineKeySet` (document / vector surrogates, KV raw keys, graph-edge
/// pairs), constructs the `ReadWriteSet`, msgpack-encodes plans into `Vec<u8>`,
/// and calls `TxClass::new`.
///
/// Returns `Err(SequencerUnavailable)` if msgpack encoding of plans fails.
pub fn build_static_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
) -> crate::Result<TxClass> {
    use std::collections::HashMap;

    // Collect surrogates per collection for non-edge write tasks.
    let mut doc_surrogates: HashMap<String, Vec<u32>> = HashMap::new();
    // Collect edge identity (surrogate pairs) and routing homes
    // (from_key of src/dst string keys) per collection for graph edges.
    let mut edge_pairs: HashMap<String, Vec<(u32, u32)>> = HashMap::new();
    let mut edge_homes: HashMap<String, Vec<u32>> = HashMap::new();
    // KV writes are keyed by raw bytes and Vector writes by surrogate — each
    // needs its own EngineKeySet rather than the generic document-surrogate
    // bucket (which would mis-key them and break lock-conflict detection).
    let mut kv_keys: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
    let mut vector_surrogates: HashMap<String, Vec<u32>> = HashMap::new();

    for task in tasks {
        if !is_write_plan(&task.plan) {
            continue;
        }
        // Graph edges route by from_key(src)/from_key(dst), not by collection.
        // EdgePut and EdgeDelete share identity fields so both produce an
        // `EngineKeySet::Edge` — a cross-shard delete dual-homes (and locks)
        // exactly like the matching insert.
        if let PhysicalPlan::Graph(
            GraphOp::EdgePut {
                collection,
                src_id,
                dst_id,
                src_surrogate,
                dst_surrogate,
                ..
            }
            | GraphOp::EdgeDelete {
                collection,
                src_id,
                dst_id,
                src_surrogate,
                dst_surrogate,
                ..
            },
        ) = &task.plan
        {
            edge_pairs
                .entry(collection.clone())
                .or_default()
                .push((src_surrogate.as_u32(), dst_surrogate.as_u32()));
            let homes = edge_homes.entry(collection.clone()).or_default();
            homes.push(VShardId::from_key(src_id.as_bytes()).as_u32());
            homes.push(VShardId::from_key(dst_id.as_bytes()).as_u32());
            continue;
        }
        // KV and Vector writes carry their own key representation.
        match &task.plan {
            PhysicalPlan::Kv(op) => {
                if let Some((coll, keys)) = kv_write_keys(op) {
                    kv_keys.entry(coll).or_default().extend(keys);
                    continue;
                }
            }
            PhysicalPlan::Vector(op) => {
                if let Some((coll, surrs)) = vector_write_surrogates(op) {
                    vector_surrogates.entry(coll).or_default().extend(surrs);
                    continue;
                }
            }
            _ => {}
        }
        // Document engine (and any other statically-keyed write reaching the
        // multishard path): bucket by surrogate.
        let collection = collection_name_from_plan(&task.plan);
        let surrogate = surrogate_from_plan(&task.plan);
        doc_surrogates
            .entry(collection)
            .or_default()
            .push(surrogate);
    }

    // Build write set — one EngineKeySet per collection, sorted for
    // determinism.
    let mut write_sets: Vec<EngineKeySet> = doc_surrogates
        .into_iter()
        .map(|(collection, surrogates)| EngineKeySet::Document {
            collection,
            surrogates: SortedVec::new(surrogates),
        })
        .collect();
    // Emit one Edge keyset per collection, carrying surrogate-pair identity
    // (for locking) and from_key routing homes (for participating vShards).
    for (collection, pairs) in edge_pairs {
        // `edge_pairs` and `edge_homes` are populated in lockstep in the loop
        // above, so a collection in one is always in the other. Treat a missing
        // homes entry as a hard error rather than silently emitting an Edge
        // keyset with empty `home_vshards` (which would drop Calvin participant
        // shards and misroute the cross-shard write with no diagnostic).
        let homes = edge_homes.remove(&collection).ok_or_else(|| Error::Internal {
            detail: format!(
                "build_static_tx_class invariant violated: no edge_homes for collection {collection}"
            ),
        })?;
        write_sets.push(EngineKeySet::Edge {
            collection,
            edges: SortedVec::new(pairs),
            home_vshards: SortedVec::new(homes),
        });
    }
    // Emit one Kv keyset per collection (raw byte keys) and one Vector keyset
    // per collection (surrogates), so KV and Vector writes lock on their real
    // identity rather than a bogus document surrogate.
    for (collection, keys) in kv_keys {
        write_sets.push(EngineKeySet::Kv {
            collection,
            keys: SortedVec::new(keys),
        });
    }
    for (collection, surrogates) in vector_surrogates {
        write_sets.push(EngineKeySet::Vector {
            collection,
            surrogates: SortedVec::new(surrogates),
        });
    }
    // Sort by collection name for determinism.
    write_sets.sort_by(|a, b| a.collection().cmp(b.collection()));

    let write_set = ReadWriteSet::new(write_sets);
    let read_set = ReadWriteSet::new(vec![]);

    // Encode all plans as msgpack bytes.
    let plans: Vec<&PhysicalPlan> = tasks.iter().map(|t| &t.plan).collect();
    let plans_bytes = zerompk::to_msgpack_vec(&plans).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode PhysicalPlan vec for Calvin TxClass: {e}"),
    })?;

    TxClass::new(read_set, write_set, plans_bytes, tenant_id, None).map_err(|e| Error::BadRequest {
        detail: format!("invalid TxClass: {e}"),
    })
}

/// Extract `(collection, raw byte keys)` from a KV write plan, or `None` for a
/// KV op with no statically-known point keys (e.g. `BatchPut`).
fn kv_write_keys(op: &KvOp) -> Option<(String, Vec<Vec<u8>>)> {
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
fn vector_write_surrogates(op: &VectorOp) -> Option<(String, Vec<u32>)> {
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

/// Build a `TxClass` for a dependent-read (OLLP) transaction.
///
/// For `BulkUpdate`/`BulkDelete` plans that have `ollp_predicted_surrogates`
/// set, the OLLP collection's write set is built from `predicted_surrogates`.
/// All other tasks in the batch are included using static surrogate extraction,
/// exactly as `build_static_tx_class` does. This ensures multi-shard Calvin
/// txns that contain an OLLP bulk operation alongside static-key writes still
/// produce a valid multi-vshard `TxClass`.
///
/// Returns `Err` if encoding fails or the resulting TxClass is invalid.
pub fn build_dependent_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    collection: &str,
    predicted_surrogates: &[u32],
) -> crate::Result<TxClass> {
    use std::collections::BTreeMap;

    // Accumulate per-collection surrogate sets. The OLLP collection uses the
    // predicted surrogates; all other tasks use static key extraction.
    let mut doc_surrogates: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    // Graph edges (appended implicit-edge deletes) route by from_key(src)/
    // from_key(dst), NOT by collection — mirror `build_static_tx_class`'s edge
    // handling so an `EdgeDelete` appended to a dependent txn is classified as
    // an `EngineKeySet::Edge` (and dual-homed/locked) rather than misrouted as a
    // document write via `surrogate_from_plan`.
    let mut edge_pairs: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
    let mut edge_homes: BTreeMap<String, Vec<u32>> = BTreeMap::new();

    // Seed with the OLLP collection's predicted surrogates.
    doc_surrogates
        .entry(collection.to_owned())
        .or_default()
        .extend_from_slice(predicted_surrogates);

    // Add static surrogates for all non-OLLP tasks.
    for task in tasks {
        // Edges first: collect surrogate-pair identity + from_key routing homes,
        // then skip the doc-surrogate path. EdgePut/EdgeDelete share identity
        // fields so both produce an `EngineKeySet::Edge`.
        if let PhysicalPlan::Graph(
            GraphOp::EdgePut {
                collection: edge_coll,
                src_id,
                dst_id,
                src_surrogate,
                dst_surrogate,
                ..
            }
            | GraphOp::EdgeDelete {
                collection: edge_coll,
                src_id,
                dst_id,
                src_surrogate,
                dst_surrogate,
                ..
            },
        ) = &task.plan
        {
            edge_pairs
                .entry(edge_coll.clone())
                .or_default()
                .push((src_surrogate.as_u32(), dst_surrogate.as_u32()));
            let homes = edge_homes.entry(edge_coll.clone()).or_default();
            homes.push(VShardId::from_key(src_id.as_bytes()).as_u32());
            homes.push(VShardId::from_key(dst_id.as_bytes()).as_u32());
            continue;
        }

        let coll = collection_name_from_plan(&task.plan);
        if coll.is_empty() || coll == collection {
            continue;
        }
        let surrogate = surrogate_from_plan(&task.plan);
        doc_surrogates.entry(coll).or_default().push(surrogate);
    }

    let mut write_sets: Vec<EngineKeySet> = doc_surrogates
        .into_iter()
        .map(|(coll, surrogates)| EngineKeySet::Document {
            collection: coll,
            surrogates: SortedVec::new(surrogates),
        })
        .collect();
    // Emit one Edge keyset per edge collection, with the SAME missing-homes-is-
    // hard-error guard `build_static_tx_class` uses: `edge_pairs` and
    // `edge_homes` are populated in lockstep, so a missing homes entry is an
    // invariant violation, not an empty-participant write.
    for (edge_coll, pairs) in edge_pairs {
        let homes = edge_homes.remove(&edge_coll).ok_or_else(|| Error::Internal {
            detail: format!(
                "build_dependent_tx_class invariant violated: no edge_homes for collection {edge_coll}"
            ),
        })?;
        write_sets.push(EngineKeySet::Edge {
            collection: edge_coll,
            edges: SortedVec::new(pairs),
            home_vshards: SortedVec::new(homes),
        });
    }
    write_sets.sort_by(|a, b| a.collection().cmp(b.collection()));

    let write_set = ReadWriteSet::new(write_sets);
    let read_set = ReadWriteSet::new(vec![]);

    let plans: Vec<&PhysicalPlan> = tasks.iter().map(|t| &t.plan).collect();
    let plans_bytes = zerompk::to_msgpack_vec(&plans).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode PhysicalPlan vec for Calvin dependent TxClass: {e}"),
    })?;

    TxClass::new(read_set, write_set, plans_bytes, tenant_id, None).map_err(|e| Error::BadRequest {
        detail: format!("invalid dependent TxClass: {e}"),
    })
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
fn surrogate_from_plan(plan: &PhysicalPlan) -> u32 {
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
