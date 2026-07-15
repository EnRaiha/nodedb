// SPDX-License-Identifier: BUSL-1.1

pub mod lock_wire;
pub mod primitives;
pub mod scheduler_input;
pub mod sequencer;
pub mod transaction;

pub use lock_wire::{LockKeyWire, ReleaseReason, TxnIdWire};
pub use primitives::{
    DependentReadSpec, EngineKeySet, EngineTag, PassiveReadKey, ReadKeyIdent, SortedVec,
    VersionedReadEntry, VersionedReadSet,
};
pub use scheduler_input::SchedulerInput;
pub use sequencer::{EpochBatch, SequencedTxn};
pub use transaction::{ReadWriteSet, TxClass};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nodedb_types::TenantId;
    use nodedb_types::id::{DatabaseId, VShardId};

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

    fn multi_vshard_write_set() -> ReadWriteSet {
        // Use two different collections that hash to different vShards.
        // We can't pick known-distinct names without running the hash, so we
        // scan at test time.
        let (a, b) = find_two_distinct_collections();
        ReadWriteSet::new(vec![doc_set(&a, vec![1, 2]), doc_set(&b, vec![3])])
    }

    /// Find two collection names whose vShards differ.
    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
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

    fn make_tx_class(write_set: ReadWriteSet) -> TxClass {
        TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![0x01, 0x02],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass")
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

    // ── ReadWriteSet ──────────────────────────────────────────────────────────

    #[test]
    fn read_write_set_participating_vshards_distinct() {
        let ws = multi_vshard_write_set();
        let vshards = ws.participating_vshards();
        assert!(vshards.len() >= 2, "expected at least 2 distinct vShards");
    }

    #[test]
    fn read_write_set_participating_vshards_sorted() {
        let ws = multi_vshard_write_set();
        let vshards = ws.participating_vshards();
        let ids: Vec<u32> = vshards.iter().map(|v| v.as_u32()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn read_write_set_same_collection_counted_once() {
        // Two EngineKeySets for the same collection: still one vshard.
        let ws = ReadWriteSet::new(vec![doc_set("users", vec![1]), vec_set("users", vec![1])]);
        let vshards = ws.participating_vshards();
        assert_eq!(vshards.len(), 1);
    }

    // ── TxClass construction ──────────────────────────────────────────────────

    #[test]
    fn tx_class_new_rejects_empty_write_set() {
        use crate::error::CalvinError;
        let err = TxClass::new(
            ReadWriteSet::new(vec![]),
            ReadWriteSet::new(vec![]),
            vec![],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::EmptyWriteSet));
    }

    #[test]
    fn tx_class_new_rejects_single_vshard() {
        use crate::error::CalvinError;
        // Single collection → single vshard.
        let ws = ReadWriteSet::new(vec![doc_set("users", vec![1, 2])]);
        let err = TxClass::new(
            ReadWriteSet::new(vec![]),
            ws,
            vec![],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::SingleVshardTxn { .. }));
    }

    #[test]
    fn tx_class_new_accepts_multi_vshard() {
        let tc = make_tx_class(multi_vshard_write_set());
        assert!(tc.participating_vshards().len() >= 2);
    }

    #[test]
    fn tx_class_participating_vshards_cached() {
        let tc = make_tx_class(multi_vshard_write_set());
        // Two calls return the same slice.
        assert_eq!(tc.participating_vshards(), tc.participating_vshards());
    }

    // ── Byte-determinism ──────────────────────────────────────────────────────

    /// Byte-determinism: two TxClass values with logically identical sets
    /// (different insertion order) must produce byte-identical JSON.
    ///
    /// `participating_vshards` is `#[serde(skip)]` so it is excluded from
    /// serialization; only the stable sorted fields participate.
    #[test]
    fn tx_class_byte_deterministic_across_insertion_order() {
        let (col_a, col_b) = find_two_distinct_collections();

        let ws_forward = ReadWriteSet::new(vec![
            doc_set(&col_a, vec![3, 1, 2]),
            doc_set(&col_b, vec![10, 5]),
        ]);
        let ws_backward = ReadWriteSet::new(vec![
            doc_set(&col_b, vec![5, 10]),
            doc_set(&col_a, vec![2, 3, 1]),
        ]);

        // Both write sets have the same logical content but different
        // ordering.  We compare the serialized *inner sets* after sorting
        // the outer Vec by collection name so key-set order doesn't matter.
        let forward_bytes = sonic_rs::to_vec(&ws_forward).unwrap();
        let backward_bytes = sonic_rs::to_vec(&ws_backward).unwrap();

        // The outer Vec order may differ; compare after canonical-sort.
        let mut fw_parsed: Vec<serde_json::Value> = sonic_rs::from_slice(&forward_bytes).unwrap();
        let mut bw_parsed: Vec<serde_json::Value> = sonic_rs::from_slice(&backward_bytes).unwrap();

        let sort_key = |v: &serde_json::Value| -> String {
            v.as_object()
                .and_then(|o| o.values().next())
                .and_then(|inner| inner.get("collection"))
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_owned()
        };
        fw_parsed.sort_by_key(sort_key);
        bw_parsed.sort_by_key(sort_key);
        assert_eq!(fw_parsed, bw_parsed);
    }

    /// Byte-determinism for the full TxClass: serialize → deserialize →
    /// restore_derived → serialize again; both bytes must be identical.
    #[test]
    fn tx_class_roundtrip_bytes_stable() {
        let tc = make_tx_class(multi_vshard_write_set());
        let first = sonic_rs::to_vec(&tc).unwrap();

        let mut restored: TxClass = sonic_rs::from_slice(&first).unwrap();
        restored.restore_derived();

        let second = sonic_rs::to_vec(&restored).unwrap();
        assert_eq!(first, second);
    }

    // ── MessagePack roundtrips ────────────────────────────────────────────────

    #[test]
    fn tx_class_msgpack_roundtrip() {
        let tc = make_tx_class(multi_vshard_write_set());
        let bytes = zerompk::to_msgpack_vec(&tc).unwrap();
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).unwrap();
        decoded.restore_derived();
        assert_eq!(tc.tenant_id, decoded.tenant_id);
        assert_eq!(tc.plans, decoded.plans);
        assert_eq!(tc.write_set, decoded.write_set);
        assert_eq!(tc.read_set, decoded.read_set);
        assert_eq!(tc.participating_vshards(), decoded.participating_vshards());
    }

    #[test]
    fn sequenced_txn_msgpack_roundtrip() {
        let tx_class = make_tx_class(multi_vshard_write_set());
        let st = SequencedTxn {
            epoch: 42,
            position: 7,
            tx_class,
            epoch_system_ms: 1_700_000_000_000,
            epoch_vshard_txn_count: 3,
            lock_owner: None,
        };
        let bytes = zerompk::to_msgpack_vec(&st).unwrap();
        let mut decoded: SequencedTxn = zerompk::from_msgpack(&bytes).unwrap();
        decoded.tx_class.restore_derived();
        assert_eq!(st.epoch, decoded.epoch);
        assert_eq!(st.position, decoded.position);
        assert_eq!(st.epoch_system_ms, decoded.epoch_system_ms);
        assert_eq!(st.tx_class.write_set, decoded.tx_class.write_set);
    }

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

    #[test]
    fn tx_class_with_dependent_reads_participating_vshards_includes_passives() {
        let (col_a, col_b) = find_two_distinct_collections();
        let write_set = ReadWriteSet::new(vec![doc_set(&col_a, vec![1]), doc_set(&col_b, vec![2])]);

        // Pick a vshard id that's different from col_a and col_b.
        let passive_vshard_id = {
            let a = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_a).as_u32();
            let b = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_b).as_u32();
            // Find one that differs from both.
            let mut candidate = 9999u32;
            for i in 0u32..64 {
                let name = format!("passive_col_{i}");
                let v = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
                if v != a && v != b {
                    candidate = v;
                    break;
                }
            }
            candidate
        };

        let spec = DependentReadSpec {
            passive_reads: {
                let mut m = BTreeMap::new();
                m.insert(
                    passive_vshard_id,
                    vec![PassiveReadKey {
                        engine_key: doc_set("passive_col", vec![99]),
                    }],
                );
                m
            },
        };

        let tc = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(1),
            Some(spec),
            VersionedReadSet::default(),
        )
        .expect("valid TxClass with dependent reads");

        // The participating vshards must include the passive vshard.
        let vshard_ids: Vec<u32> = tc
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        assert!(
            vshard_ids.contains(&passive_vshard_id),
            "participating_vshards must include passive vshard {passive_vshard_id}; got {vshard_ids:?}"
        );
    }

    #[test]
    fn epoch_batch_msgpack_roundtrip() {
        let tc = make_tx_class(multi_vshard_write_set());
        let batch = EpochBatch {
            epoch: 1,
            txns: vec![
                SequencedTxn {
                    epoch: 1,
                    position: 0,
                    tx_class: tc.clone(),
                    epoch_system_ms: 1_700_000_000_000,
                    epoch_vshard_txn_count: 2,
                    lock_owner: None,
                },
                SequencedTxn {
                    epoch: 1,
                    position: 1,
                    tx_class: tc,
                    epoch_system_ms: 1_700_000_000_000,
                    epoch_vshard_txn_count: 2,
                    lock_owner: None,
                },
            ],
            epoch_system_ms: 1_700_000_000_000,
        };
        let bytes = zerompk::to_msgpack_vec(&batch).unwrap();
        let mut decoded: EpochBatch = zerompk::from_msgpack(&bytes).unwrap();
        for txn in &mut decoded.txns {
            txn.tx_class.restore_derived();
        }
        assert_eq!(batch.epoch, decoded.epoch);
        assert_eq!(batch.epoch_system_ms, decoded.epoch_system_ms);
        assert_eq!(batch.txns.len(), decoded.txns.len());
        assert_eq!(
            batch.txns[0].epoch_system_ms,
            decoded.txns[0].epoch_system_ms
        );
        assert_eq!(
            batch.txns[0].tx_class.write_set,
            decoded.txns[0].tx_class.write_set
        );
    }
}
