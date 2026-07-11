// SPDX-License-Identifier: BUSL-1.1

//! `TxClass` construction for a static write task slice (every write key
//! known upfront).

use crate::Error;
use crate::control::planner::calvin::dispatch::is_write_plan;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::types::VShardId;
use nodedb_cluster::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::TenantId;

use super::shared::{
    collection_name_from_plan, kv_write_keys, surrogate_from_plan, vector_write_surrogates,
    versioned_reads_from,
};

/// Build a **multi-vshard** `TxClass` from a static write task slice.
///
/// Extracts each write task's deterministic identity into the matching
/// `EngineKeySet` (document / vector surrogates, KV raw keys, graph-edge
/// pairs), constructs the `ReadWriteSet`, msgpack-encodes plans into `Vec<u8>`,
/// and calls `TxClass::new`. A write set that collapses to a single vshard is
/// rejected (`SingleVshardTxn`) — that shape indicates a misrouted multi-shard
/// dispatch. For the legitimate contended-single-vshard point-write path, use
/// [`build_single_vshard_tx_class`].
///
/// `reads` is the neutral session read-set captured during the transaction;
/// it is projected onto the `TxClass`'s LSN-versioned `versioned_reads` field.
/// Autocommit and pure-write paths pass an empty slice.
///
/// Returns `Err(SequencerUnavailable)` if msgpack encoding of plans fails.
pub fn build_static_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    reads: &[ReadSetEntry],
) -> crate::Result<TxClass> {
    build_static_tx_class_impl(tasks, tenant_id, reads, false)
}

/// Build a `TxClass` from a static write task slice that is permitted to resolve
/// to a **single vshard**.
///
/// Used only by the contended point-write routing path
/// (`route_write_to_calvin`): the write-admission gate returned
/// `RouteToCalvin` because a pending commit holds the write's
/// key, so the write must sequence through the deterministic scheduler to
/// serialize on the SAME shared per-vShard `LockManager`. Identical extraction
/// to [`build_static_tx_class`]; only the participant floor differs.
pub fn build_single_vshard_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    reads: &[ReadSetEntry],
) -> crate::Result<TxClass> {
    build_static_tx_class_impl(tasks, tenant_id, reads, true)
}

/// Shared body for the static builders. `allow_single_vshard` selects between
/// [`TxClass::new`] (multi-vshard, `>=2` floor) and
/// [`TxClass::new_single_vshard`] (single-vshard opt-in).
fn build_static_tx_class_impl(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    reads: &[ReadSetEntry],
    allow_single_vshard: bool,
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

    let versioned_reads = versioned_reads_from(reads);

    let result = if allow_single_vshard {
        TxClass::new_single_vshard(
            read_set,
            write_set,
            plans_bytes,
            tenant_id,
            None,
            versioned_reads,
        )
    } else {
        TxClass::new(
            read_set,
            write_set,
            plans_bytes,
            tenant_id,
            None,
            versioned_reads,
        )
    };
    result.map_err(|e| Error::BadRequest {
        detail: format!("invalid TxClass: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::read_set::{EngineTag, ReadKey};
    use crate::types::{DatabaseId, KeyRepr, Lsn};
    use nodedb_cluster::calvin::types::ReadKeyIdent;
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::Surrogate;

    /// Find two collection names whose default-database vShards differ, so the
    /// built `TxClass` spans ≥2 vShards (required by `TxClass::new`).
    pub(super) fn two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..1024 {
            let name = format!("coll_{i}");
            let v = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
            match &first {
                Some((fname, fv)) if *fv != v => return (fname.clone(), name),
                Some(_) => {}
                None => first = Some((name, v)),
            }
        }
        panic!("could not find two distinct-vShard collections in 1024 tries");
    }

    pub(super) fn point_insert_task(collection: &str, surrogate: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: collection.to_owned(),
                document_id: "d1".to_owned(),
                surrogate: Surrogate::new(surrogate),
                value: vec![],
                if_absent: false,
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    fn read_entry(collection: &str, key: ReadKey, read_lsn: u64) -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Document,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: collection.to_owned(),
            key,
            read_lsn: Lsn::new(read_lsn),
        }
    }

    #[test]
    fn build_static_carries_versioned_reads_and_keeps_write_derived_vshards() {
        let (col_a, col_b) = two_distinct_collections();
        let tasks = vec![point_insert_task(&col_a, 1), point_insert_task(&col_b, 2)];

        // Synthetic read-set: one point read (surrogate identity) at LSN 7 and
        // one collection-scoped predicate read at LSN 11.
        let reads = vec![
            read_entry(
                "read_col",
                ReadKey::Point {
                    repr: KeyRepr::Surrogate(42),
                },
                7,
            ),
            read_entry("scan_col", ReadKey::Predicate, 11),
        ];

        let tx = build_static_tx_class(&tasks, TenantId::new(1), &reads)
            .expect("valid multi-vShard TxClass");

        // Versioned reads are carried on the new field, faithfully mapped.
        assert_eq!(tx.versioned_reads.len(), 2);
        let point = tx
            .versioned_reads
            .iter()
            .find(|e| matches!(e.key, ReadKeyIdent::Point(_)))
            .expect("point entry present");
        assert_eq!(point.read_lsn, Lsn::new(7));
        assert_eq!(point.engine, EngineTag::Document);
        assert_eq!(point.key, ReadKeyIdent::Point(KeyRepr::Surrogate(42)));

        let predicate = tx
            .versioned_reads
            .iter()
            .find(|e| matches!(e.key, ReadKeyIdent::Predicate))
            .expect("predicate entry present");
        assert_eq!(predicate.read_lsn, Lsn::new(11));

        // participating_vshards stays WRITE-derived: exactly the two write
        // collections' vShards, unaffected by the read-set.
        let expected = tx.write_set.participating_vshards();
        assert_eq!(tx.participating_vshards(), expected.as_slice());
        assert_eq!(tx.participating_vshards().len(), 2);
    }

    #[test]
    fn empty_read_set_yields_empty_versioned_reads() {
        let (col_a, col_b) = two_distinct_collections();
        let tasks = vec![point_insert_task(&col_a, 1), point_insert_task(&col_b, 2)];
        let tx = build_static_tx_class(&tasks, TenantId::new(1), &[])
            .expect("valid multi-vShard TxClass");
        assert!(tx.versioned_reads.is_empty());
    }

    #[test]
    fn single_point_write_strict_rejects_but_single_vshard_builder_accepts() {
        // One point-write task → one collection → one vshard. This is exactly the
        // shape the contended point-write routing path builds.
        let tasks = vec![point_insert_task("users", 7)];
        let want_vshard =
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, "users").as_u32();

        // Strict builder rejects the single-vshard write set.
        let strict = build_static_tx_class(&tasks, TenantId::new(1), &[]);
        assert!(
            matches!(strict, Err(crate::Error::BadRequest { .. })),
            "strict builder must reject single-vshard write set"
        );

        // Single-vshard builder accepts it, with exactly one participating vshard.
        let tx = build_single_vshard_tx_class(&tasks, TenantId::new(1), &[])
            .expect("single-vshard TxClass accepted");
        assert_eq!(tx.participating_vshards().len(), 1);
        assert_eq!(tx.participating_vshards()[0].as_u32(), want_vshard);
    }
}
