// SPDX-License-Identifier: BUSL-1.1

//! Read-set projections and node lock pairs shared by the static and
//! dependent `TxClass` builders. The write keys live in
//! [`super::write_keys`].

use crate::control::server::shared::session::read_set::{ReadKey, ReadSetEntry};
use nodedb_cluster::calvin::types::{
    EngineKeySet, EngineTag, ReadKeyIdent, ReadWriteSet, SortedVec, VersionedReadEntry,
    VersionedReadSet,
};

/// Map the neutral session read-set into the replicated, LSN-versioned
/// [`VersionedReadSet`] carried on the `TxClass`.
///
/// Each [`ReadSetEntry`] becomes one [`VersionedReadEntry`], preserving
/// engine, collection, `read_version_lsn` (the collection's write floor —
/// the sound OCC comparand, not the core-global `read_lsn`), and the
/// point/predicate distinction. Own-overlay exclusion already happened at
/// capture time, so this is a faithful 1:1 projection.
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
                home_vshard: entry.home.map(|home| home.as_u32()),
                served_by: entry.home_node,
            })
            .collect(),
    )
}

/// Build the routing/identity `read_set` for a Calvin `TxClass` from the
/// neutral session read-set — the key-IDENTITY set for participant/routing,
/// not the LSN-versioned OCC set ([`versioned_reads_from`]).
///
/// A [`ReadSetEntry`] carries only `(engine, collection)`, no key identity,
/// so an unhomed entry maps to a COLLECTION-homed [`EngineKeySet`] with an
/// empty key vector. That over-approximates participants: more validation,
/// never a dropped participant. A homed entry (one vShard of a cross-shard
/// graph read) maps to an `EngineKeySet::Edge` with no edges and its home in
/// `home_vshards`, so the vShard it read participates and validates it.
pub(super) fn read_set_from(reads: &[ReadSetEntry]) -> ReadWriteSet {
    use std::collections::BTreeSet;

    // Dedup by (engine-variant, collection): many reads on one collection
    // collapse to a single keyset, keeping the Raft-log read_set compact.
    // Vector/KV keep their engine variant; every other engine routes by
    // collection name via a Document keyset.
    let mut vector_colls: BTreeSet<String> = BTreeSet::new();
    let mut kv_colls: BTreeSet<String> = BTreeSet::new();
    let mut doc_colls: BTreeSet<String> = BTreeSet::new();
    // Homed reads (cross-shard graph reads) participate on their home vShards,
    // not on the collection's vShard. They key no edge, so they lock nothing.
    let mut homed: std::collections::BTreeMap<String, BTreeSet<u32>> =
        std::collections::BTreeMap::new();
    for entry in reads {
        if let Some(home) = entry.home {
            homed
                .entry(entry.collection.clone())
                .or_default()
                .insert(home.as_u32());
            continue;
        }
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
    for (collection, homes) in homed {
        sets.push(EngineKeySet::Edge {
            collection,
            edges: SortedVec::new(vec![]),
            home_vshards: SortedVec::new(homes.into_iter().collect()),
        });
    }
    ReadWriteSet::new(sets)
}

/// The lock pair of node `node` within one edge collection.
///
/// Every edge write takes it for both endpoints, and a node delete's guard
/// takes it for its node, so a guard and every write of an edge on its node
/// run in sequence order on the node's key home. A real edge never locks a
/// pair with a zero destination surrogate, so the pair never aliases an
/// edge. Two node names that hash alike only serialize together.
pub(crate) fn node_lock_pair(node: &str) -> (u32, u32) {
    let hash = crate::util::fnv1a_hash(node.as_bytes());
    ((hash ^ (hash >> 32)) as u32, 0)
}

/// Lockstep proof that the write-admission gate and the Calvin scheduler
/// derive IDENTICAL lock keys for the same op — if they diverged, a
/// gate-fenced write and a sequenced txn will lock different keys.
#[cfg(test)]
mod lockstep_tests {
    use super::*;
    use crate::control::cluster::calvin::scheduler::lock_manager::LockKey;
    use crate::control::planner::calvin::tx_class::static_builder::build_single_vshard_tx_class;
    use crate::control::server::shared::write_admission::lock_keys::plan_lock_keys;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_cluster::calvin::types::EngineKeySet;
    use nodedb_physical::physical_plan::{DocumentOp, GraphOp, KvOp, PhysicalPlan};
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

    /// Mirrors the scheduler's `expand_rw_set` `EngineKeySet` → `LockKey`
    /// mapping (fixed translation). What's under test is whether the
    /// extractor threads the same `(collection, key/surrogate)` as the
    /// gate's `plan_lock_keys`, not this mapping.
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
                EngineKeySet::Array { collection, .. } => {
                    keys.insert(LockKey::Surrogate {
                        collection: Arc::from(collection.as_str()),
                        surrogate:
                            crate::control::planner::calvin::tx_class::write_keys::COLLECTION_KEY,
                    });
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
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "counters",
            ),
            key: b"ctr".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: Surrogate::new(3),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
        }));
    }

    #[test]
    fn kv_cas_gate_key_matches_scheduler_key() {
        assert_gate_matches_scheduler(PhysicalPlan::Kv(KvOp::Cas {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "counters",
            ),
            key: b"ctr".to_vec(),
            expected: vec![],
            new_value: vec![],
            surrogate: Surrogate::new(3),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        }));
    }

    #[test]
    fn document_upsert_gate_key_matches_scheduler_key() {
        assert_gate_matches_scheduler(PhysicalPlan::Document(DocumentOp::Upsert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".to_owned(),
            value: vec![],
            on_conflict_updates: vec![],
            surrogate: Surrogate::new(9),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }));
    }

    /// A bound point delete on the fast path locks its surrogate and its row
    /// id, exactly as the scheduler does, so an unbound delete sequenced
    /// through Calvin orders against it by the row id. The fence and the
    /// keyed order lock still name the row by its surrogate key alone.
    #[test]
    fn document_point_delete_gate_keys_match_scheduler_keys_with_row_id() {
        use crate::control::server::shared::write_admission::lock_keys::plan_row_key;
        let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".to_owned(),
            surrogate: Some(Surrogate::new(9)),
            pk_bytes: b"d1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        });
        let (_, gate_keys) = plan_lock_keys(&plan).expect("a bound point delete is fast-path");
        assert!(gate_keys.contains(&LockKey::Kv {
            collection: Arc::from("docs"),
            key: Arc::from(b"d1".as_slice()),
        }));
        assert_eq!(
            plan_row_key(&plan),
            Some(LockKey::Surrogate {
                collection: Arc::from("docs"),
                surrogate: 9,
            })
        );
        assert_gate_matches_scheduler(plan);
    }

    /// A single-home edge write on the fast path locks its edge and both
    /// endpoints' node lock pairs, exactly as the scheduler does, so a node
    /// delete's guard orders against it either way.
    #[test]
    fn edge_put_gate_keys_match_scheduler_keys_with_node_locks() {
        let src = "n0";
        let dst = (1u32..)
            .map(|i| format!("n{i}"))
            .find(|name| {
                crate::types::VShardId::from_key(name.as_bytes())
                    == crate::types::VShardId::from_key(src.as_bytes())
            })
            .expect("some node shares the source's key home");
        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "g",
            ),
            src_id: src.to_owned(),
            label: "L".to_owned(),
            dst_id: dst.clone(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        });
        let (_, gate_keys) = plan_lock_keys(&plan).expect("a single-home edge is fast-path");
        for node in [src, dst.as_str()] {
            let (lock_src, lock_dst) = node_lock_pair(node);
            assert!(gate_keys.contains(&LockKey::Edge {
                collection: Arc::from("g"),
                src: lock_src,
                dst: lock_dst,
            }));
        }
        assert_gate_matches_scheduler(plan);
    }
}

/// The participant set and the routing oracle must agree about where a plan
/// lives: the write keys' collections feed the participant list, and the
/// scheduler's `plan_vshard` oracle decides who gets the plan. A
/// disagreement enlists a shard, hands it nothing, and aborts far from the
/// cause. These tests pin the agreement directly.
#[cfg(test)]
mod routing_agreement_tests {
    use crate::control::planner::calvin::tx_class::static_builder::build_static_tx_class;
    use crate::control::planner::calvin::tx_class::write_keys::{WriteKeys, add_plan_write_keys};
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_cluster::calvin::types::{EngineKeySet, SortedVec};
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
    use nodedb_types::Surrogate;

    const TENANT: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    /// The binding's source and its balance target. Asserted to hash apart
    /// by [`the_fixture_spans_two_vshards`] — a co-resident pair will never
    /// produce the two-task plan this file is about.
    const SOURCE: &str = "route_entries";
    const TARGET: &str = "route_accounts";

    /// Build a task homed the way PRODUCTION homes it, not by asking the
    /// write keys, which makes the agreement true by construction.
    fn task(plan: PhysicalPlan, vshard_id: VShardId) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TENANT,
            vshard_id,
            database_id: DB,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    /// The pair a cross-shard materialized-sum statement produces: the source
    /// write, and the balance task homed by the same function
    /// `append_cross_shard_balance_tasks` homes it with.
    fn statement_tasks() -> Vec<PhysicalTask> {
        vec![
            task(
                source_write(),
                nodedb_types::CollectionKey::from_bare(DB, SOURCE).vshard(),
            ),
            task(balance_write(), crate::query::sum_target_vshard(DB, TARGET)),
        ]
    }

    fn source_write() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: nodedb_types::QualifiedCollection::new(DB, SOURCE),
            document_id: "e1".to_owned(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(11),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn balance_write() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: nodedb_types::QualifiedCollection::new(DB, TARGET),
            document_id: "0000010f".to_owned(),
            surrogate: Surrogate::new(271),
            column: "balance".to_owned(),
            delta: "25".to_owned(),
            join_column: "account_id".to_owned(),
            join_value: "acc-1".to_owned(),
            declared_primary_key: None,
        })
    }

    #[test]
    fn the_fixture_spans_two_vshards() {
        assert_ne!(
            nodedb_types::CollectionKey::from_bare(DB, SOURCE).vshard(),
            nodedb_types::CollectionKey::from_bare(DB, TARGET).vshard(),
            "the balance-pairing case only exists when source and target hash apart"
        );
    }

    /// A balance write locks the TARGET row of the TARGET collection it
    /// names: not an empty collection on vShard 0, and not the collection
    /// key every balance write will share.
    #[test]
    fn a_balance_write_locks_the_target_row() {
        let mut keys = WriteKeys::default();
        add_plan_write_keys(&mut keys, &balance_write()).expect("a balance write has keys");
        assert_eq!(
            keys.into_key_sets(),
            vec![EngineKeySet::Document {
                collection: TARGET.to_owned(),
                surrogates: SortedVec::new(vec![271]),
            }]
        );
    }

    /// The pair enlists exactly the two shards that hold work, no third.
    #[test]
    fn the_pair_enlists_only_the_shards_that_hold_work() {
        let tasks = statement_tasks();
        let tx = build_static_tx_class(&tasks, TENANT, &[]).expect("build the transaction class");

        let mut expected = vec![
            nodedb_types::CollectionKey::from_bare(DB, SOURCE).vshard(),
            nodedb_types::CollectionKey::from_bare(DB, TARGET).vshard(),
        ];
        expected.sort_by_key(|v| v.as_u32());
        assert_eq!(
            tx.participating_vshards(),
            expected.as_slice(),
            "every enlisted shard must be one the routing oracle sends a plan to"
        );
    }

    /// Every task's own home agrees with the participant the class enlists for
    /// it. Stated over the task list rather than over one op, so a future write
    /// shape appended alongside a source write is covered by the same rule.
    #[test]
    fn every_task_homes_on_a_shard_the_class_enlists() {
        let tasks = statement_tasks();
        let tx = build_static_tx_class(&tasks, TENANT, &[]).expect("build the transaction class");
        for task in &tasks {
            assert!(
                tx.participating_vshards().contains(&task.vshard_id),
                "task homed on {:?} is not enlisted; it would be dispatched to a shard \
                 that never voted",
                task.vshard_id
            );
        }
    }
}
