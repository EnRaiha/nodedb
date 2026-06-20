// SPDX-License-Identifier: BUSL-1.1

//! Pre-validation pass for a candidate epoch batch.
//!
//! The validator detects intra-batch write-set conflicts and produces a
//! deterministically-ordered list of admitted and rejected transactions. No
//! cross-epoch read-set checks are performed here; cross-epoch read-after-write
//! conflict detection is the scheduler's responsibility.
//!
//! # Ordering
//!
//! Transactions are ordered by `(inbox_seq, tenant_id, hash(plans))`.
//! `inbox_seq` is assigned by the sequencer leader at admit time and is the
//! primary tiebreaker. `xxh3_64` (from `xxhash-rust`) is used for `hash(plans)`
//! because it is byte-stable across processes and architectures.
//!
//! # Conflict detection
//!
//! A flat list of `(engine_discriminant, collection, key_bytes)` is built from
//! each transaction's write set. The list is sorted lexicographically. Adjacent
//! entries with identical `(discriminant, collection, key_bytes)` from different
//! txn indices indicate a conflict; the later txn (higher sort key, i.e. higher
//! `inbox_seq`) is rejected.

use std::cmp::Ordering;

use xxhash_rust::xxh3::xxh3_64;

use crate::calvin::sequencer::error::SequencerError;
use crate::calvin::sequencer::inbox::{AdmittedTx, RejectedTx};
use crate::calvin::sequencer::metrics::ConflictKey;
use crate::calvin::types::{EngineKeySet, SequencedTxn};

/// A flat write-set entry used during conflict detection.
#[derive(Debug)]
struct WriteEntry {
    /// Discriminant tag for the engine variant (Document=0, Vector=1, Kv=2, Edge=3).
    discriminant: u8,
    /// Static engine name used when building conflict context keys.
    engine_name: &'static str,
    /// Collection name.
    collection: String,
    /// Serialized key bytes.
    key_bytes: Vec<u8>,
    /// Index into the sorted-batch `Vec<AdmittedTx>`.
    txn_index: usize,
}

impl WriteEntry {
    fn sort_key(&self) -> (&u8, &str, &[u8]) {
        (
            &self.discriminant,
            self.collection.as_str(),
            &self.key_bytes,
        )
    }
}

/// Build the flat write-entry list for one transaction.
fn flatten_write_set(tx: &AdmittedTx, txn_index: usize) -> Vec<WriteEntry> {
    let mut out = Vec::new();
    for key_set in &tx.tx_class.write_set.0 {
        match key_set {
            EngineKeySet::Document {
                collection,
                surrogates,
            } => {
                for &s in surrogates.as_slice() {
                    out.push(WriteEntry {
                        discriminant: 0,
                        engine_name: "document",
                        collection: collection.clone(),
                        key_bytes: s.to_le_bytes().to_vec(),
                        txn_index,
                    });
                }
            }
            EngineKeySet::Vector {
                collection,
                surrogates,
            } => {
                for &s in surrogates.as_slice() {
                    out.push(WriteEntry {
                        discriminant: 1,
                        engine_name: "vector",
                        collection: collection.clone(),
                        key_bytes: s.to_le_bytes().to_vec(),
                        txn_index,
                    });
                }
            }
            EngineKeySet::Kv { collection, keys } => {
                for k in keys.as_slice() {
                    out.push(WriteEntry {
                        discriminant: 2,
                        engine_name: "kv",
                        collection: collection.clone(),
                        key_bytes: k.clone(),
                        txn_index,
                    });
                }
            }
            EngineKeySet::Edge {
                collection, edges, ..
            } => {
                for &(src, dst) in edges.as_slice() {
                    let mut key_bytes = src.to_le_bytes().to_vec();
                    key_bytes.extend_from_slice(&dst.to_le_bytes());
                    out.push(WriteEntry {
                        discriminant: 3,
                        engine_name: "edge",
                        collection: collection.clone(),
                        key_bytes,
                        txn_index,
                    });
                }
            }
        }
    }
    out
}

/// Sort key for admitted transactions: `(inbox_seq, tenant_id, hash(plans))`.
fn admitted_sort_key(tx: &AdmittedTx) -> (u64, u64, u64) {
    let plan_hash = xxh3_64(&tx.tx_class.plans);
    (tx.inbox_seq, tx.tx_class.tenant_id.as_u64(), plan_hash)
}

/// Validate a candidate batch of admitted transactions.
///
/// Returns `(Vec<SequencedTxn>, Vec<RejectedTx>)`:
/// - `SequencedTxn.position` is 0-based among the admitted transactions only.
/// - `RejectedTx.reason` is `SequencerError::Conflict { position_admitted }`.
///
/// The function is pure — no I/O, no global state, deterministic.
pub fn validate_batch_with_assignments(
    epoch: u64,
    mut candidates: Vec<AdmittedTx>,
) -> (Vec<(u64, SequencedTxn)>, Vec<RejectedTx>) {
    if candidates.is_empty() {
        return (vec![], vec![]);
    }

    // Step 1: sort by (inbox_seq, tenant_id, hash(plans)).
    candidates.sort_by_key(admitted_sort_key);

    // Step 2: build flat write-entry list.
    let mut flat: Vec<WriteEntry> = Vec::new();
    for (i, tx) in candidates.iter().enumerate() {
        flat.extend(flatten_write_set(tx, i));
    }

    // Step 3: sort flat list lexicographically.
    flat.sort_by(|a, b| match a.discriminant.cmp(&b.discriminant) {
        Ordering::Equal => match a.collection.cmp(&b.collection) {
            Ordering::Equal => a.key_bytes.cmp(&b.key_bytes),
            other => other,
        },
        other => other,
    });

    // Step 4: detect conflicts — adjacent duplicate keys from different txns.
    let n = candidates.len();
    let mut rejected = vec![false; n];
    // determinism: scratch map, not iterated for output
    let mut admitted_position_for: std::collections::HashMap<usize, u32> =
        std::collections::HashMap::new();

    let mut i = 0;
    while i < flat.len() {
        // Collect run of entries with same (discriminant, collection, key_bytes).
        let mut j = i + 1;
        while j < flat.len() && flat[j].sort_key() == flat[i].sort_key() {
            j += 1;
        }
        // flat[i..j] all have the same key. If more than one distinct txn_index
        // appears, all but the lowest-indexed are rejected.
        let min_txn = flat[i..j].iter().map(|e| e.txn_index).min().unwrap_or(i);
        for entry in &flat[i..j] {
            if entry.txn_index != min_txn {
                rejected[entry.txn_index] = true;
                // Record the admitted position of the winner if not yet set.
                admitted_position_for.entry(entry.txn_index).or_insert(0);
            }
        }
        i = j;
    }

    // Step 5: build output. First pass: assign positions to admitted txns.
    let mut position_map = vec![0u32; n];
    let mut next_position: u32 = 0;
    for (idx, is_rejected) in rejected.iter().enumerate() {
        if !*is_rejected {
            position_map[idx] = next_position;
            next_position += 1;
        }
    }

    // Now build admitted/rejected output.
    let mut admitted_out: Vec<(u64, SequencedTxn)> = Vec::new();
    let mut rejected_out: Vec<RejectedTx> = Vec::new();

    for (idx, tx) in candidates.into_iter().enumerate() {
        if rejected[idx] {
            // Find the winning position: the admitted txn that holds the conflicting key.
            // We scan flat to find the min txn_index for any key this txn writes.
            let (winner_position, conflict_context) =
                find_winner_position_and_context(&flat, idx, &position_map, &tx);
            rejected_out.push(RejectedTx {
                admitted: tx,
                reason: SequencerError::Conflict {
                    position_admitted: winner_position,
                },
                conflict_context,
            });
        } else {
            let inbox_seq = tx.inbox_seq;
            admitted_out.push((
                inbox_seq,
                SequencedTxn {
                    epoch,
                    position: position_map[idx],
                    tx_class: tx.tx_class,
                    // epoch_system_ms is filled in by the service tick() when the
                    // EpochBatch is constructed; 0 is a safe placeholder here.
                    epoch_system_ms: 0,
                },
            ));
        }
    }

    (admitted_out, rejected_out)
}

pub fn validate_batch(
    epoch: u64,
    candidates: Vec<AdmittedTx>,
) -> (Vec<SequencedTxn>, Vec<RejectedTx>) {
    let (admitted, rejected) = validate_batch_with_assignments(epoch, candidates);
    (admitted.into_iter().map(|(_, txn)| txn).collect(), rejected)
}

/// Find the position of the admitted (winning) txn that conflicts with
/// `loser_idx`, and build the [`ConflictKey`] for the first conflicting write
/// entry found.
///
/// Returns `(winner_position, Some(ConflictKey))`. The `ConflictKey` uses the
/// tenant from `loser_tx`, the engine from the `WriteEntry`, and the collection
/// from that same entry — matching what the metrics layer needs for per-context
/// hotness tracking.
fn find_winner_position_and_context(
    flat: &[WriteEntry],
    loser_idx: usize,
    position_map: &[u32],
    loser_tx: &AdmittedTx,
) -> (u32, Option<ConflictKey>) {
    for entry in flat.iter().filter(|e| e.txn_index == loser_idx) {
        let min_idx = flat
            .iter()
            .filter(|e| e.sort_key() == entry.sort_key() && e.txn_index != loser_idx)
            .map(|e| e.txn_index)
            .min();
        if let Some(winner) = min_idx {
            let ctx = ConflictKey {
                tenant: loser_tx.tx_class.tenant_id.as_u64(),
                engine: entry.engine_name,
                collection: entry.collection.clone(),
            };
            return (position_map[winner], Some(ctx));
        }
    }
    (0, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::sequencer::inbox::AdmittedTx;
    use crate::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
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

    fn make_tx(
        inbox_seq: u64,
        col_a: &str,
        surrogates_a: Vec<u32>,
        col_b: &str,
        surrogates_b: Vec<u32>,
    ) -> AdmittedTx {
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a.to_owned(),
                surrogates: SortedVec::new(surrogates_a),
            },
            EngineKeySet::Document {
                collection: col_b.to_owned(),
                surrogates: SortedVec::new(surrogates_b),
            },
        ]);
        let tx_class = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![inbox_seq as u8],
            TenantId::new(1),
            None,
        )
        .expect("valid TxClass");
        AdmittedTx {
            inbox_seq,
            tx_class,
        }
    }

    #[test]
    fn empty_input_produces_empty_output() {
        let (admitted, rejected) = validate_batch(1, vec![]);
        assert!(admitted.is_empty());
        assert!(rejected.is_empty());
    }

    #[test]
    fn single_txn_admitted_at_position_zero() {
        let (col_a, col_b) = find_two_distinct_collections();
        let tx = make_tx(0, &col_a, vec![1], &col_b, vec![2]);
        let (admitted, rejected) = validate_batch(1, vec![tx]);
        assert_eq!(admitted.len(), 1);
        assert!(rejected.is_empty());
        assert_eq!(admitted[0].position, 0);
        assert_eq!(admitted[0].epoch, 1);
    }

    #[test]
    fn two_non_conflicting_txns_both_admitted_in_inbox_seq_order() {
        let (col_a, col_b) = find_two_distinct_collections();
        let tx0 = make_tx(0, &col_a, vec![1], &col_b, vec![10]);
        let tx1 = make_tx(1, &col_a, vec![2], &col_b, vec![20]);
        let (admitted, rejected) = validate_batch(2, vec![tx0, tx1]);
        assert_eq!(admitted.len(), 2);
        assert!(rejected.is_empty());
        // Position 0 should have the lower inbox_seq.
        assert_eq!(admitted[0].position, 0);
        assert_eq!(admitted[1].position, 1);
    }

    #[test]
    fn two_conflicting_txns_first_admitted_second_rejected() {
        let (col_a, col_b) = find_two_distinct_collections();
        // Both txns write to surrogate 42 in col_a.
        let tx0 = make_tx(0, &col_a, vec![42], &col_b, vec![1]);
        let tx1 = make_tx(1, &col_a, vec![42], &col_b, vec![2]);
        let (admitted, rejected) = validate_batch(3, vec![tx0, tx1]);
        assert_eq!(admitted.len(), 1);
        assert_eq!(rejected.len(), 1);
        assert_eq!(admitted[0].position, 0);
        assert!(matches!(
            rejected[0].reason,
            SequencerError::Conflict { .. }
        ));
    }

    #[test]
    fn deterministic_ordering_across_repeated_runs() {
        let (col_a, col_b) = find_two_distinct_collections();
        let tx0 = make_tx(0, &col_a, vec![1], &col_b, vec![10]);
        let tx1 = make_tx(1, &col_a, vec![1], &col_b, vec![20]);

        let (admitted1, rejected1) = validate_batch(1, vec![tx0.clone(), tx1.clone()]);
        let (admitted2, rejected2) = validate_batch(1, vec![tx0, tx1]);

        assert_eq!(admitted1.len(), admitted2.len());
        assert_eq!(rejected1.len(), rejected2.len());
        for (a, b) in admitted1.iter().zip(admitted2.iter()) {
            assert_eq!(a.position, b.position);
        }
    }

    #[test]
    fn rejected_txn_carries_conflict_error_with_winner_position() {
        let (col_a, col_b) = find_two_distinct_collections();
        let tx0 = make_tx(0, &col_a, vec![99], &col_b, vec![1]);
        let tx1 = make_tx(1, &col_a, vec![99], &col_b, vec![2]);
        let (_admitted, rejected) = validate_batch(5, vec![tx0, tx1]);
        assert_eq!(rejected.len(), 1);
        // Winner is at position 0.
        assert_eq!(
            rejected[0].reason,
            SequencerError::Conflict {
                position_admitted: 0
            }
        );
    }

    #[test]
    fn conflict_fairness_metric_keyed() {
        // Verify that RejectedTx.conflict_context is populated with the
        // correct tenant, engine name, and collection when a conflict occurs.
        let (col_a, col_b) = find_two_distinct_collections();
        // Both transactions write to surrogate 7 in col_a (Document engine).
        let tx0 = make_tx(0, &col_a, vec![7], &col_b, vec![1]);
        let tx1 = make_tx(1, &col_a, vec![7], &col_b, vec![2]);

        // tx1 uses tenant_id 1 (from make_tx helper).
        let (_admitted, rejected) = validate_batch(10, vec![tx0, tx1]);
        assert_eq!(rejected.len(), 1);

        let ctx = rejected[0]
            .conflict_context
            .as_ref()
            .expect("conflict_context must be Some for a Conflict rejection");

        assert_eq!(ctx.tenant, 1, "tenant should match tx tenant_id");
        assert_eq!(
            ctx.engine, "document",
            "engine should be 'document' for Document key set"
        );
        assert_eq!(
            ctx.collection, col_a,
            "collection should be the conflicting collection"
        );
    }
}
