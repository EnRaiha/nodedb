// SPDX-License-Identifier: BUSL-1.1

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::control::cluster::calvin::scheduler::lock_manager::{
    AcquireOutcome, LockKey, LockManager, TxnId,
};

#[test]
fn two_non_conflicting_both_dispatch_immediately() {
    let mut lm = LockManager::new();

    let txn1 = TxnId::new(1, 0);
    let txn2 = TxnId::new(1, 1);

    let keys1: BTreeSet<LockKey> = [LockKey::Surrogate {
        collection: Arc::from("coll"),
        surrogate: 1,
    }]
    .into();
    let keys2: BTreeSet<LockKey> = [LockKey::Surrogate {
        collection: Arc::from("coll"),
        surrogate: 2,
    }]
    .into();

    let o1 = lm.acquire(txn1, keys1);
    let o2 = lm.acquire(txn2, keys2);

    assert_eq!(o1, AcquireOutcome::Ready, "txn1 should be ready");
    assert_eq!(
        o2,
        AcquireOutcome::Ready,
        "txn2 should be ready (disjoint keys)"
    );
}

#[test]
fn two_conflicting_second_dispatches_after_first_completes() {
    let mut lm = LockManager::new();

    let txn1 = TxnId::new(1, 0);
    let txn2 = TxnId::new(1, 1);
    let shared_key: BTreeSet<LockKey> = [LockKey::Surrogate {
        collection: Arc::from("coll"),
        surrogate: 42,
    }]
    .into();

    let o1 = lm.acquire(txn1, shared_key.clone());
    assert_eq!(o1, AcquireOutcome::Ready);

    let o2 = lm.acquire(txn2, shared_key.clone());
    assert_eq!(o2, AcquireOutcome::Blocked);

    let unblocked = lm.release(txn1);
    assert!(unblocked.contains(&txn2));

    assert!(lm.is_ready(txn2, &shared_key));
}

#[test]
fn many_mixed_deterministic_dispatch_order() {
    let mut lm = LockManager::new();
    let mut dispatched: Vec<TxnId> = Vec::new();

    let pairs = [(2, 0), (1, 1), (3, 0), (1, 0), (2, 1)];
    for (epoch, pos) in pairs {
        let tid = TxnId::new(epoch, pos);
        let keys: BTreeSet<LockKey> = [LockKey::Surrogate {
            collection: Arc::from(format!("c_{epoch}_{pos}")),
            surrogate: epoch as u32 * 10 + pos,
        }]
        .into();
        let outcome = lm.acquire(tid, keys);
        if outcome == AcquireOutcome::Ready {
            dispatched.push(tid);
        }
    }

    assert_eq!(
        dispatched.len(),
        5,
        "all non-conflicting txns should be ready"
    );

    let mut expected = pairs.map(|(e, p)| TxnId::new(e, p)).to_vec();
    expected.sort();
    let mut sorted_dispatched = dispatched.clone();
    sorted_dispatched.sort();
    assert_eq!(sorted_dispatched, expected);
}

#[test]
fn cross_epoch_raw_blocks_correctly() {
    let mut lm = LockManager::new();

    let txn_n = TxnId::new(1, 0);
    let txn_n1 = TxnId::new(2, 0);

    let key_k: BTreeSet<LockKey> = [LockKey::Surrogate {
        collection: Arc::from("orders"),
        surrogate: 100,
    }]
    .into();

    let o1 = lm.acquire(txn_n, key_k.clone());
    assert_eq!(o1, AcquireOutcome::Ready);

    let o2 = lm.acquire(txn_n1, key_k.clone());
    assert_eq!(o2, AcquireOutcome::Blocked);

    let unblocked = lm.release(txn_n);
    assert!(unblocked.contains(&txn_n1));
    assert!(lm.is_ready(txn_n1, &key_k));
}

// ── Catch-up drain + in-flight guard (sequencer fan-out reliability) ────────

use std::collections::HashMap;
use std::sync::Mutex;

use nodedb_cluster::MultiRaft;
use nodedb_cluster::RoutingTable;
use nodedb_cluster::calvin::types::{
    EngineKeySet, ReadWriteSet, SchedulerInput, SequencedTxn, SortedVec, TxClass, VersionedReadSet,
};
use nodedb_cluster::calvin::{CalvinCompletionRegistry, SequencerStateMachine};
use nodedb_types::TenantId;

use super::scheduler::{Scheduler, SchedulerParams};
use crate::bridge::dispatch::Dispatcher;
use crate::control::cluster::calvin::scheduler::SchedulerConfig;
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
use crate::control::state::SharedState;
use crate::wal::WalManager;

/// Build a minimally-wired `Scheduler` for driver-level unit tests. The Data
/// Plane is NOT started — these tests exercise only the Control-Plane routing /
/// guard logic that never dispatches, so no core loop is needed. The returned
/// `TempDir` must be kept alive for the scheduler's lifetime (backs the WAL and
/// Raft storage).
fn build_test_scheduler(vshard_id: u32) -> (Scheduler, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).unwrap();

    let rt = RoutingTable::uniform(1, &[1], 1);
    let multi_raft = Arc::new(Mutex::new(MultiRaft::new(1, rt, dir.path().to_path_buf())));

    let registry = CalvinCompletionRegistry::new_detached();
    let sequencer_state_machine = Arc::new(Mutex::new(SequencerStateMachine::new(
        HashMap::new(),
        Arc::clone(&registry),
    )));

    let (_tx, receiver) = tokio::sync::mpsc::channel(16);
    let (_rr_tx, read_result_rx) = tokio::sync::mpsc::channel(16);
    let (_prom_tx, promotion_rx) = tokio::sync::mpsc::unbounded_channel();
    let (_v_tx, verdict_rx) = tokio::sync::mpsc::channel(16);

    let lock_manager = Arc::new(Mutex::new(LockManager::new()));

    let scheduler = Scheduler::new(SchedulerParams {
        vshard_id,
        receiver,
        shared,
        multi_raft,
        sequencer_state_machine,
        fully_applied_epoch: 0,
        applied_tail: BTreeSet::new(),
        rebuild_target_epoch: 0,
        config: SchedulerConfig::default(),
        metrics: SchedulerMetrics::new(),
        read_result_rx,
        lock_manager,
        promotion_rx,
        registry,
        verdict_rx,
    });
    (scheduler, dir)
}

/// Build a static-write `SequencedTxn` at `(epoch, position)`.
fn make_sequenced_txn(epoch: u64, position: u32) -> SequencedTxn {
    let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
        collection: "test_coll".to_string(),
        surrogates: SortedVec::new(vec![1]),
    }]);
    let tx_class = TxClass::new_single_vshard(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![],
        TenantId::new(1),
        None,
        VersionedReadSet::default(),
    )
    .expect("valid TxClass");
    SequencedTxn {
        epoch,
        position,
        tx_class,
        epoch_system_ms: 1_700_000_000_000,
        epoch_vshard_txn_count: 1,
        lock_owner: None,
    }
}

#[tokio::test]
async fn in_flight_guard_skips_replayed_txn_already_in_flight() {
    let (mut scheduler, _dir) = build_test_scheduler(0);
    let txn = make_sequenced_txn(5, 0);
    let txn_id = TxnId::new(5, 0);

    // Stand in for the LIVE delivery: the txn is already in-flight (here,
    // blocked on locks — keyed by its lock_owner == apply-slot). Inserting the
    // map entry directly avoids driving the full dispatch machinery.
    scheduler.blocked.insert(
        txn_id,
        super::super::types::BlockedTxn {
            txn: txn.clone(),
            keys: BTreeSet::new(),
            blocked_at: std::time::Instant::now(),
        },
    );

    let dispatched_before = scheduler.metrics.dispatch_count.load(Ordering::Relaxed);

    // Now REPLAY the same (epoch, position) through the live processing path —
    // exactly what `drain_catch_up` does for a dropped-then-recovered input that
    // overlaps an already-in-flight live one. The in-flight guard must turn it
    // into a no-op: no second dispatch, no duplicate in-flight entry.
    scheduler.process_scheduler_input(SchedulerInput::Txn(txn));

    let dispatched_after = scheduler.metrics.dispatch_count.load(Ordering::Relaxed);
    assert_eq!(
        dispatched_before, dispatched_after,
        "in-flight guard must prevent a second dispatch of an already-in-flight txn"
    );
    assert_eq!(
        scheduler.blocked.len(),
        1,
        "guard must not add a duplicate in-flight entry"
    );
    assert!(
        scheduler.pending.is_empty(),
        "guard must not have dispatched (no pending entry created)"
    );
}

#[tokio::test]
async fn drain_catch_up_is_noop_when_no_drop_recorded() {
    // Fresh sequencer state machine: no fan-out was ever dropped, so
    // `take_catch_up_from` returns `None` and the drain returns O(1) without
    // touching MultiRaft, replaying anything, or hitting the compacted path.
    let (mut scheduler, _dir) = build_test_scheduler(0);

    scheduler.drain_catch_up();

    assert_eq!(
        scheduler.metrics.catch_up_replayed.load(Ordering::Relaxed),
        0,
        "no inputs should be replayed when no drop was recorded"
    );
    assert_eq!(
        scheduler
            .metrics
            .catch_up_log_compacted
            .load(Ordering::Relaxed),
        0,
        "the compacted path must not be reached on the no-drop common case"
    );
}
