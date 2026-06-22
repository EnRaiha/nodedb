// SPDX-License-Identifier: BUSL-1.1

//! The canonical wire-type for entries proposed to the sequencer Raft group.
//!
//! [`SequencerEntry`] mirrors the [`crate::metadata_group::entry::MetadataEntry`]
//! pattern: a group-local typed enum with zerompk derives. Future variants can
//! be added here without coupling to the metadata group's apply path.

use serde::{Deserialize, Serialize};

use crate::calvin::types::EpochBatch;

/// An entry in the replicated sequencer log.
///
/// Every epoch batch committed by the sequencer is encoded as one of these
/// variants, proposed to the sequencer Raft group, and applied on every replica
/// by [`super::state_machine::SequencerStateMachine`].
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
pub enum SequencerEntry {
    /// A validated epoch batch ready for global-order assignment.
    ///
    /// The state machine fans the batch out to per-vshard output channels after
    /// applying it to local state.
    EpochBatch { batch: EpochBatch },
    /// Completion acknowledgement from one participating vshard.
    CompletionAck {
        epoch: u64,
        position: u32,
        vshard_id: u32,
    },
    /// OLLP predicate-mismatch signal. Proposed by the per-vShard scheduler when
    /// the active executor returns `OllpRetryRequired`. Applied on ALL sequencer-group
    /// replicas so every node's `CalvinCompletionRegistry` fires `note_ollp_mismatch`,
    /// waking the coordinator's retry-loop waiter wherever it is (including remote
    /// nodes). Mirrors `CompletionAck` — no `vshard_id` because mismatch is keyed
    /// only by `(epoch, position)` like the registry's `TxnId`.
    OllpMismatch { epoch: u64, position: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::types::{EngineKeySet, ReadWriteSet, SequencedTxn, SortedVec, TxClass};
    use nodedb_types::{
        TenantId,
        id::{DatabaseId, VShardId},
    };

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

    fn make_epoch_batch() -> EpochBatch {
        let (col_a, col_b) = find_two_distinct_collections();
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a,
                surrogates: SortedVec::new(vec![1, 2]),
            },
            EngineKeySet::Document {
                collection: col_b,
                surrogates: SortedVec::new(vec![3]),
            },
        ]);
        let tx_class = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![0xAB],
            TenantId::new(1),
            None,
        )
        .expect("valid TxClass");

        EpochBatch {
            epoch: 7,
            txns: vec![SequencedTxn {
                epoch: 7,
                position: 0,
                tx_class,
                epoch_system_ms: 1_700_000_000_000,
            }],
            epoch_system_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn epoch_batch_msgpack_roundtrip() {
        let entry = SequencerEntry::EpochBatch {
            batch: make_epoch_batch(),
        };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        match (entry, decoded) {
            (SequencerEntry::EpochBatch { batch: a }, SequencerEntry::EpochBatch { batch: b }) => {
                assert_eq!(a.epoch, b.epoch);
                assert_eq!(a.txns.len(), b.txns.len());
                assert_eq!(a.txns[0].position, b.txns[0].position);
            }
            _ => panic!("decoded wrong sequencer entry variant"),
        }
    }

    #[test]
    fn epoch_batch_roundtrip_preserves_txn_count() {
        let batch = make_epoch_batch();
        let entry = SequencerEntry::EpochBatch { batch };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        let SequencerEntry::EpochBatch { batch } = decoded else {
            panic!("decoded wrong sequencer entry variant");
        };
        assert_eq!(batch.txns.len(), 1);
        assert_eq!(batch.epoch, 7);
    }
}
