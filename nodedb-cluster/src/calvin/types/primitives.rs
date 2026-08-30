// SPDX-License-Identifier: BUSL-1.1

//! Primitive Calvin type definitions.
//!
//! [`SortedVec`], [`EngineKeySet`], and [`PassiveReadKey`] live in
//! `nodedb-types` so the physical-plan IR can reference them without
//! pulling in the distributed scheduler. [`DependentReadSpec`] stays
//! here because it is scheduler-internal.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use nodedb_types::calvin::{
    EngineKeySet, EngineTag, PassiveReadKey, ReadKeyIdent, SortedVec, VersionedReadEntry,
    VersionedReadSet,
};

/// Describes the passive-read participants for a dependent-read Calvin txn.
///
/// Each entry maps a vshard id to the keys that vshard must read and broadcast
/// to all active participants before any writes can proceed.
///
/// `BTreeMap` is mandatory here: the sequencer and scheduler must iterate
/// vshards in a deterministic order (determinism contract).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DependentReadSpec {
    /// Passive participants: vshard → keys to read.
    pub passive_reads: BTreeMap<u32, Vec<PassiveReadKey>>,
}

impl DependentReadSpec {
    /// Total estimated serialized bytes across all passive read keys.
    ///
    /// Used by the sequencer admission check to enforce
    /// `max_dependent_read_bytes_per_txn`.  This is an O(1)-per-key
    /// estimate, not an exact serialized size.
    pub fn total_bytes(&self) -> usize {
        self.passive_reads
            .values()
            .flat_map(|ks| ks.iter())
            .map(|k| k.engine_key.serialized_size_hint())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_set(collection: &str, surrogates: Vec<u32>) -> EngineKeySet {
        EngineKeySet::Document {
            collection: collection.to_owned(),
            surrogates: SortedVec::new(surrogates),
        }
    }

    fn vec_set(collection: &str, surrogates: Vec<u32>) -> EngineKeySet {
        EngineKeySet::Vector {
            collection: collection.to_owned(),
            surrogates: SortedVec::new(surrogates),
        }
    }

    fn kv_set(collection: &str, keys: Vec<Vec<u8>>) -> EngineKeySet {
        EngineKeySet::Kv {
            collection: collection.to_owned(),
            keys: SortedVec::new(keys),
        }
    }

    fn edge_set(collection: &str, edges: Vec<(u32, u32)>) -> EngineKeySet {
        EngineKeySet::Edge {
            collection: collection.to_owned(),
            edges: SortedVec::new(edges),
            home_vshards: SortedVec::new(Vec::new()),
        }
    }

    // ── SortedVec ─────────────────────────────────────────────────────────────

    #[test]
    fn sorted_vec_sort_and_dedup() {
        let v: SortedVec<u32> = SortedVec::new(vec![5, 1, 3, 1, 2, 5]);
        assert_eq!(v.as_slice(), &[1, 2, 3, 5]);
    }

    #[test]
    fn sorted_vec_already_sorted() {
        let v: SortedVec<u32> = SortedVec::new(vec![1, 2, 3]);
        assert_eq!(v.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn sorted_vec_empty() {
        let v: SortedVec<u32> = SortedVec::new(vec![]);
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn sorted_vec_bytes_deterministic_regardless_of_insertion_order() {
        let a: SortedVec<u32> = SortedVec::new(vec![3, 1, 4, 1, 5]);
        let b: SortedVec<u32> = SortedVec::new(vec![5, 4, 3, 1, 1]);
        let a_bytes = sonic_rs::to_vec(&a).unwrap();
        let b_bytes = sonic_rs::to_vec(&b).unwrap();
        assert_eq!(a_bytes, b_bytes);
    }

    // ── EngineKeySet ──────────────────────────────────────────────────────────

    #[test]
    fn engine_key_set_collection_name() {
        let d = doc_set("users", vec![1]);
        assert_eq!(d.collection(), "users");

        let v = vec_set("embeddings", vec![2]);
        assert_eq!(v.collection(), "embeddings");

        let k = kv_set("sessions", vec![b"key1".to_vec()]);
        assert_eq!(k.collection(), "sessions");

        let e = edge_set("follows", vec![(1, 2)]);
        assert_eq!(e.collection(), "follows");
    }

    #[test]
    fn engine_key_set_is_empty() {
        assert!(doc_set("users", vec![]).is_empty());
        assert!(!doc_set("users", vec![1]).is_empty());
    }

    // ── DependentReadSpec ─────────────────────────────────────────────────────

    #[test]
    fn dependent_read_spec_msgpack_roundtrip() {
        let spec = DependentReadSpec {
            passive_reads: {
                let mut m = BTreeMap::new();
                m.insert(
                    1u32,
                    vec![PassiveReadKey {
                        engine_key: doc_set("users", vec![10, 20]),
                    }],
                );
                m.insert(
                    2u32,
                    vec![PassiveReadKey {
                        engine_key: kv_set("sessions", vec![b"abc".to_vec()]),
                    }],
                );
                m
            },
        };
        let bytes = zerompk::to_msgpack_vec(&spec).unwrap();
        let decoded: DependentReadSpec = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(spec.passive_reads.len(), decoded.passive_reads.len());
        assert_eq!(spec.passive_reads.get(&1), decoded.passive_reads.get(&1));
    }
}
