// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the Calvin dependent-read path.
//!
//! Uses mock channels to simulate the passive → active read result flow
//! without a real cluster.

use std::collections::BTreeMap;
use std::time::Duration;

use nodedb::control::cluster::calvin::scheduler::driver::barrier::{
    PendingDependentBarrier, ReadResultEvent,
};
use nodedb::control::cluster::calvin::scheduler::lock_manager::TxnId;
use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
use nodedb_types::{TenantId, Value};

use std::collections::BTreeSet;
use std::time::Instant;

use nodedb_cluster::calvin::types::{
    DependentReadSpec, EngineKeySet, PassiveReadKey, ReadWriteSet, SequencedTxn, SortedVec,
    TxClass, VersionedReadSet,
};
use nodedb_types::id::VShardId;

fn two_distinct_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("col_{i}");
        let vshard =
            VShardId::from_collection_in_database(nodedb::types::DatabaseId::DEFAULT, &name)
                .as_u32();
        if let Some((ref fname, fv)) = first {
            if fv != vshard {
                return (fname.clone(), name);
            }
        } else {
            first = Some((name, vshard));
        }
    }
    panic!("could not find two distinct-vshard collections in 512 tries");
}

fn make_dependent_txn(passive_vshard: u32) -> SequencedTxn {
    let (col_a, col_b) = two_distinct_collections();
    let write_set = ReadWriteSet::new(vec![
        EngineKeySet::Document {
            collection: col_a.clone(),
            surrogates: SortedVec::new(vec![1]),
        },
        EngineKeySet::Document {
            collection: col_b.clone(),
            surrogates: SortedVec::new(vec![2]),
        },
    ]);
    let spec = DependentReadSpec {
        passive_reads: {
            let mut m = BTreeMap::new();
            m.insert(
                passive_vshard,
                vec![PassiveReadKey {
                    engine_key: EngineKeySet::Document {
                        collection: "passive_col".to_owned(),
                        surrogates: SortedVec::new(vec![42u32]),
                    },
                }],
            );
            m
        },
    };
    let tx_class = TxClass::new_dependent(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![],
        TenantId::new(1),
        spec,
        VersionedReadSet::default(),
    )
    .expect("valid dependent TxClass");
    SequencedTxn {
        epoch: 1,
        position: 0,
        tx_class,
        epoch_system_ms: 0,
        epoch_vshard_txn_count: 0,
        lock_owner: None,
    }
}

#[test]
fn dependent_barrier_completes_after_passive_delivers() {
    let txn = make_dependent_txn(7);
    let passive_vshard = 7u32;

    let mut waiting_for = BTreeSet::new();
    waiting_for.insert(passive_vshard);

    let timeout_at = Instant::now() + Duration::from_secs(30);

    let mut barrier = PendingDependentBarrier {
        txn: txn.clone(),
        lock_owner: TxnId::new(txn.epoch, txn.position),
        waiting_for,
        received: BTreeMap::new(),
        timeout_at,
    };

    assert!(!barrier.is_complete());

    // Simulate the passive vshard delivering its read result.
    let key_id = PassiveReadKeyId {
        collection: "passive_col".to_owned(),
        surrogate: 42,
    };
    let values = vec![(key_id.clone(), Value::Integer(100))];

    barrier.waiting_for.remove(&passive_vshard);
    barrier.received.insert(passive_vshard, values);

    assert!(barrier.is_complete(), "barrier should be complete now");

    let injected = barrier.assemble_injected_reads();
    assert_eq!(injected.len(), 1);
    assert_eq!(injected.get(&key_id), Some(&Value::Integer(100)));
}

#[test]
fn dependent_barrier_not_complete_with_multiple_passives() {
    let (col_a, col_b) = two_distinct_collections();
    let write_set = ReadWriteSet::new(vec![
        EngineKeySet::Document {
            collection: col_a.clone(),
            surrogates: SortedVec::new(vec![1]),
        },
        EngineKeySet::Document {
            collection: col_b.clone(),
            surrogates: SortedVec::new(vec![2]),
        },
    ]);

    let spec = DependentReadSpec {
        passive_reads: {
            let mut m = BTreeMap::new();
            m.insert(
                10u32,
                vec![PassiveReadKey {
                    engine_key: EngineKeySet::Document {
                        collection: "coll_10".to_owned(),
                        surrogates: SortedVec::new(vec![1u32]),
                    },
                }],
            );
            m.insert(
                20u32,
                vec![PassiveReadKey {
                    engine_key: EngineKeySet::Document {
                        collection: "coll_20".to_owned(),
                        surrogates: SortedVec::new(vec![2u32]),
                    },
                }],
            );
            m
        },
    };

    let tx_class = TxClass::new_dependent(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![],
        TenantId::new(1),
        spec,
        VersionedReadSet::default(),
    )
    .expect("valid");

    let mut waiting_for = BTreeSet::new();
    waiting_for.insert(10u32);
    waiting_for.insert(20u32);

    let mut barrier = PendingDependentBarrier {
        txn: SequencedTxn {
            epoch: 1,
            position: 0,
            tx_class,
            epoch_system_ms: 0,
            epoch_vshard_txn_count: 0,
            lock_owner: None,
        },
        lock_owner: TxnId::new(1, 0),
        waiting_for,
        received: BTreeMap::new(),
        timeout_at: Instant::now() + Duration::from_secs(30),
    };

    // Only one passive delivers — barrier should still be incomplete.
    barrier.waiting_for.remove(&10u32);
    barrier.received.insert(
        10u32,
        vec![(
            PassiveReadKeyId {
                collection: "coll_10".to_owned(),
                surrogate: 1,
            },
            Value::Integer(42),
        )],
    );

    assert!(!barrier.is_complete(), "still waiting for vshard 20");

    // Both deliver — now complete.
    barrier.waiting_for.remove(&20u32);
    barrier.received.insert(
        20u32,
        vec![(
            PassiveReadKeyId {
                collection: "coll_20".to_owned(),
                surrogate: 2,
            },
            Value::Integer(99),
        )],
    );

    assert!(barrier.is_complete());
    let injected = barrier.assemble_injected_reads();
    assert_eq!(injected.len(), 2);
}

#[test]
fn dependent_barrier_timeout_detected() {
    let txn = make_dependent_txn(7);
    let lock_owner = TxnId::new(txn.epoch, txn.position);
    let barrier = PendingDependentBarrier {
        txn,
        lock_owner,
        waiting_for: {
            let mut s = BTreeSet::new();
            s.insert(7u32);
            s
        },
        received: BTreeMap::new(),
        // Timeout already in the past.
        timeout_at: Instant::now() - Duration::from_millis(1),
    };

    assert!(
        barrier.is_timed_out(),
        "barrier should report timeout when timeout_at is in the past"
    );
}

/// Happy-path test: passive delivers, active assembles injected_reads correctly.
#[test]
fn read_result_event_assembles_correctly() {
    let event = ReadResultEvent {
        epoch: 5,
        position: 2,
        passive_vshard: 17,
        tenant_id: TenantId::new(3),
        values: vec![(
            PassiveReadKeyId {
                collection: "coll".to_owned(),
                surrogate: 100,
            },
            Value::Float(std::f64::consts::PI),
        )],
    };

    assert_eq!(event.epoch, 5);
    assert_eq!(event.passive_vshard, 17);
    assert_eq!(event.values.len(), 1);
    assert_eq!(event.values[0].0.collection, "coll");
}
