// SPDX-License-Identifier: BUSL-1.1

//! Expansion and decoding helpers for the Calvin scheduler driver.

use std::collections::BTreeSet;
use std::sync::Arc;

use nodedb_cluster::calvin::types::{EngineKeySet, LockKeyWire, SequencedTxn, TxnIdWire};

use crate::control::cluster::calvin::scheduler::lock_manager::{LockKey, TxnId};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::wire as plan_wire;

impl From<TxnIdWire> for TxnId {
    fn from(wire: TxnIdWire) -> Self {
        TxnId::new(wire.epoch, wire.position)
    }
}

/// Decode a [`LockKeyWire`] transport twin into the real scheduler-side
/// [`LockKey`]. Mirrors the field shapes of [`expand_rw_set`]'s per-engine key
/// construction (interned `Arc<str>` / `Arc<[u8]>` for cheap clones).
pub(super) fn decode_lock_key(wire: &LockKeyWire) -> LockKey {
    match wire {
        LockKeyWire::Surrogate {
            collection,
            surrogate,
        } => LockKey::Surrogate {
            collection: Arc::from(collection.as_str()),
            surrogate: *surrogate,
        },
        LockKeyWire::Kv { collection, key } => LockKey::Kv {
            collection: Arc::from(collection.as_str()),
            key: Arc::from(key.as_slice()),
        },
        LockKeyWire::Edge {
            collection,
            src,
            dst,
        } => LockKey::Edge {
            collection: Arc::from(collection.as_str()),
            src: *src,
            dst: *dst,
        },
    }
}

/// Expand the read_set ∪ write_set of a sequenced transaction into a
/// `BTreeSet<LockKey>`.
///
/// All keys in both the read set and write set are acquired as exclusive
/// (write) locks.
pub(super) fn expand_rw_set(txn: &SequencedTxn) -> BTreeSet<LockKey> {
    let mut keys = BTreeSet::new();
    let add_key_set = |keys: &mut BTreeSet<LockKey>, ks: &EngineKeySet| match ks {
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
    };

    for ks in &txn.tx_class.read_set.0 {
        add_key_set(&mut keys, ks);
    }
    for ks in &txn.tx_class.write_set.0 {
        add_key_set(&mut keys, ks);
    }

    keys
}

/// Decode a `Vec<PhysicalPlan>` from the opaque plan bytes stored in a
/// `TxClass`.
pub(super) fn decode_plans(plan_bytes: &[u8]) -> crate::Result<Vec<PhysicalPlan>> {
    plan_wire::decode_batch(plan_bytes).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("plan decode: {e}"),
    })
}
