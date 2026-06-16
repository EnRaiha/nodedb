// SPDX-License-Identifier: BUSL-1.1

//! Starvation property test for the Calvin sequencer inbox.
//!
//! Eight OS threads each submit transactions in a tight loop for 200 ms. All
//! eight threads conflict on the same surrogate so every epoch produces at most
//! one admitted transaction from the batch. A drain loop on the main thread
//! calls `drain_into_capped` + `validate_batch` every ~5 ms.
//!
//! The test asserts that no single thread is completely starved: the worst
//! thread's success count must be at least `1 / (8 * 4)` = 3.125 % of the
//! total successes. This property holds because:
//! - `inbox_seq` is assigned by a shared `AtomicU64::fetch_add`, so each
//!   thread gets a strictly increasing, fairly interleaved sequence number
//!   across the submission window.
//! - The validator sorts by `inbox_seq` first, so no thread can monopolise the
//!   winning position across many epochs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nodedb_cluster::calvin::sequencer::config::SequencerConfig;
use nodedb_cluster::calvin::sequencer::inbox::{Inbox, new_inbox};
use nodedb_cluster::calvin::sequencer::validator::validate_batch;
use nodedb_cluster::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
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

/// Build a TxClass that writes surrogate `surr` in `col_a` and a unique non-
/// conflicting key in `col_b`. All 8 threads use the same `surr` in `col_a` so
/// they conflict deterministically; the distinct surrogates in `col_b` ensure
/// each transaction has a different write set (and therefore a different hash)
/// for tie-breaking.
fn make_conflicting_tx(col_a: &str, col_b: &str, unique_surr: u32, tenant: u64) -> TxClass {
    let write_set = ReadWriteSet::new(vec![
        EngineKeySet::Document {
            collection: col_a.to_owned(),
            surrogates: SortedVec::new(vec![1]), // hot surrogate — all threads conflict here
        },
        EngineKeySet::Document {
            collection: col_b.to_owned(),
            surrogates: SortedVec::new(vec![unique_surr]), // per-thread unique
        },
    ]);
    TxClass::new(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![unique_surr as u8],
        TenantId::new(tenant),
        None,
    )
    .expect("valid TxClass")
}

#[tokio::test]
async fn starvation_property() {
    let n_threads: u64 = 8;
    let run_duration = Duration::from_millis(200);

    let config = SequencerConfig {
        inbox_capacity: 4096,
        tenant_inbox_quota: 512,
        max_txns_per_epoch: 128,
        max_bytes_per_epoch: 1 << 20,
        ..SequencerConfig::default()
    };

    let (inbox, mut receiver) = new_inbox(config.inbox_capacity, &config);

    // Per-thread success counter (indexed by thread id 0..8).
    let success_counts: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));

    let (col_a, col_b) = find_two_distinct_collections();

    // Spawn 8 OS threads. Each submits as fast as it can for `run_duration`.
    let mut handles = Vec::new();
    for tid in 0..n_threads {
        let inbox_clone: Inbox = inbox.clone();
        let col_a = col_a.clone();
        let col_b = col_b.clone();
        let counts = Arc::clone(&success_counts);
        let handle = std::thread::spawn(move || {
            let deadline = Instant::now() + run_duration;
            let mut local_ok: u64 = 0;
            // Use a unique surrogate per thread so write sets differ (affects
            // the xxh3 plan hash used as a tie-breaker in the validator).
            let unique_surr = (tid + 100) as u32;
            while Instant::now() < deadline {
                let tx = make_conflicting_tx(&col_a, &col_b, unique_surr, tid + 1);
                if inbox_clone.submit(tx).is_ok() {
                    local_ok += 1;
                }
                // Brief yield to avoid saturating the inbox atomics benchmark-
                // style; real clients would have natural async gaps.
                std::hint::spin_loop();
            }
            let mut map = counts.lock().unwrap_or_else(|p| p.into_inner());
            *map.entry(tid).or_insert(0) += local_ok;
        });
        handles.push(handle);
    }

    // Drain loop: every ~5 ms drain the inbox and run validate_batch.
    let epoch_duration = Duration::from_millis(5);
    let total_deadline = Instant::now() + run_duration + Duration::from_millis(50);
    let mut epoch: u64 = 0;
    let mut admitted_per_thread: HashMap<u64, u64> = HashMap::new();

    while Instant::now() < total_deadline {
        let start = Instant::now();
        let mut candidates = Vec::new();
        receiver.drain_into_capped(
            &mut candidates,
            config.max_txns_per_epoch,
            config.max_bytes_per_epoch,
        );

        if !candidates.is_empty() {
            let (admitted, _rejected) = validate_batch(epoch, candidates);
            epoch += 1;
            for seq_txn in admitted {
                let tid = seq_txn.tx_class.tenant_id.as_u64() - 1;
                *admitted_per_thread.entry(tid).or_insert(0) += 1;
            }
        }

        let elapsed = start.elapsed();
        if elapsed < epoch_duration {
            std::thread::sleep(epoch_duration - elapsed);
        }
    }

    // Join all producer threads.
    for h in handles {
        h.join().expect("producer thread panicked");
    }

    // Drain any remaining items (producers may have outlasted the drain loop).
    let mut tail = Vec::new();
    receiver.drain_into_capped(&mut tail, usize::MAX, usize::MAX);
    if !tail.is_empty() {
        let (admitted, _) = validate_batch(epoch, tail);
        for seq_txn in admitted {
            let tid = seq_txn.tx_class.tenant_id.as_u64() - 1;
            *admitted_per_thread.entry(tid).or_insert(0) += 1;
        }
    }

    let total_admitted: u64 = admitted_per_thread.values().sum();
    assert!(
        total_admitted > 0,
        "at least some transactions must be admitted"
    );

    // Starvation property: the worst thread must have >= 1/(8*4) of total admissions.
    let threshold_num = 1u64;
    let threshold_den = n_threads * 4; // 3.125% of total

    // Multiply to avoid float: worst >= total / threshold_den.
    let min_acceptable = (total_admitted / threshold_den).max(1);

    for tid in 0..n_threads {
        let count = *admitted_per_thread.get(&tid).unwrap_or(&0);
        assert!(
            count * threshold_den >= threshold_num * total_admitted || count >= min_acceptable,
            "thread {tid} admitted {count} txns out of {total_admitted} total; \
             expected at least {min_acceptable} (>= {:.1}%); \
             per-thread admitted: {admitted_per_thread:?}",
            100.0 * threshold_num as f64 / threshold_den as f64,
        );
    }
}
