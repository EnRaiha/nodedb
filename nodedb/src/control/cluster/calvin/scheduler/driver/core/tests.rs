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
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;
use nodedb_types::TenantId;

use super::super::types::{CommitState, PendingTxn};
use super::scheduler::{Scheduler, SchedulerParams};
use crate::bridge::dispatch::Dispatcher;
use crate::bridge::envelope::{Payload, Response, Status};
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
use crate::control::cluster::calvin::scheduler::{
    AppliedGate, NOT_YET_APPLIED_EPOCH, SchedulerConfig,
};
use crate::control::state::SharedState;
use crate::types::{Lsn, RequestId};
use crate::wal::WalManager;

/// Build a minimally-wired `Scheduler` for driver-level unit tests. The Data
/// Plane is NOT started — tests exercise Control-Plane routing, guards, and
/// request dispatch only, so no core loop is needed. The returned `TempDir`
/// must be kept alive for the scheduler's lifetime (backs the WAL and
/// Raft storage).
fn build_test_scheduler(vshard_id: u32) -> (Scheduler, tempfile::TempDir) {
    let registry = CalvinCompletionRegistry::new_detached();
    let (scheduler, dir, _data_side) = build_test_scheduler_with_data_side(vshard_id, registry);
    (scheduler, dir)
}

/// Same minimal scheduler fixture, retaining its Data-Plane request receiver
/// for tests that must observe scheduler dispatches.
fn build_test_scheduler_with_data_side(
    vshard_id: u32,
    registry: Arc<CalvinCompletionRegistry>,
) -> (
    Scheduler,
    tempfile::TempDir,
    crate::bridge::dispatch::CoreChannelDataSide,
) {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
    let (dispatcher, mut data_sides) = Dispatcher::new(1, 64);
    let data_side = data_sides
        .pop()
        .expect("one configured core has one data side");
    let shared = SharedState::new(dispatcher, wal).unwrap();

    let rt = RoutingTable::uniform(1, &[1], 1);
    let multi_raft = Arc::new(Mutex::new(MultiRaft::new(1, rt, dir.path().to_path_buf())));

    let sequencer_state_machine = Arc::new(Mutex::new(SequencerStateMachine::new(
        HashMap::new(),
        Arc::clone(&registry),
    )));

    let (_tx, receiver) = tokio::sync::mpsc::channel(16);
    let (_rr_tx, read_result_rx) = tokio::sync::mpsc::channel(16);
    let (_prom_tx, promotion_rx) = tokio::sync::mpsc::unbounded_channel();
    let (verdict_tx, verdict_rx) = tokio::sync::mpsc::channel(16);
    registry.register_verdict_signal_sender(vshard_id, verdict_tx);

    let lock_manager = Arc::new(Mutex::new(LockManager::new()));

    let scheduler = Scheduler::new(SchedulerParams {
        vshard_id,
        receiver,
        shared,
        multi_raft,
        sequencer_state_machine,
        // A freshly-built scheduler has applied nothing, so its watermark is the
        // not-yet-applied sentinel (matching `read_applied_recovery` for a clean
        // node). Hardcoding `0` here would instead claim epoch 0 is fully applied,
        // making the exactly-once gate (`AppliedGate::is_applied`) short-circuit
        // every epoch-0 replay before it reaches the lock table — silently
        // defeating the end-to-end drain tests below.
        fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
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
    (scheduler, dir, data_side)
}

/// Build a static-write `SequencedTxn` at `(epoch, position)`.
fn staged_pending(txn: SequencedTxn, txn_id: TxnId) -> PendingTxn {
    PendingTxn {
        txn,
        lock_owner: txn_id,
        dispatch_time: std::time::Instant::now(),
        has_primary_write: true,
        has_returning: false,
        change_sets: Vec::new(),
        commit_state: Some(CommitState::Staged),
        verdict_deadline: None,
    }
}

fn staged_response(status: Status, read_set_valid: Option<bool>) -> Response {
    Response {
        request_id: RequestId::new(1),
        status,
        attempt: 1,
        partial: false,
        payload: Payload::empty(),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        read_set_valid,
        read_version_lsn: Lsn::ZERO,
        write_set: Vec::new(),
    }
}

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
            // no-determinism: test-only blocked_at timestamp for a fabricated BlockedTxn fixture.
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

// ── END-TO-END catch-up drain (real committed sequencer Raft log) ───────────
//
// The three tests above cover the pieces in isolation (replay decode, the
// in-flight guard, the no-drop fast path). The two tests below close the loop:
// they stand up a REAL single-node sequencer Raft group, COMMIT epoch batches
// to it, force the live fan-out to actually DROP under a full channel, then run
// the real `drain_catch_up` — which reads the genuine committed log via
// `read_committed_entries`, decodes it through `replay_epochs_for_vshard`, and
// feeds each input through `process_scheduler_input` — and assert the dropped
// input's effect lands in the scheduler's lock table. This proves the whole
// mechanism closes the fan-out gap, not just each half.

use std::time::{Duration, Instant};

use nodedb_cluster::calvin::types::EpochBatch;
use nodedb_cluster::calvin::{SEQUENCER_GROUP_ID, SequencerEntry};
use nodedb_types::id::{DatabaseId, VShardId};

/// Ensure the sequencer Raft group exists on this scheduler's `MultiRaft` and
/// that this single node is its leader, so proposals commit immediately.
fn ensure_sequencer_leader(scheduler: &Scheduler) {
    let mut mr = scheduler
        .multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if !mr.contains_group(SEQUENCER_GROUP_ID) {
        mr.add_group(SEQUENCER_GROUP_ID, vec![]).unwrap();
    }
    // Force the election timeout to fire on the next tick so the single voter
    // campaigns and wins immediately (majority = itself).
    if let Some(node) = mr.groups_mut().get_mut(&SEQUENCER_GROUP_ID) {
        // no-determinism: test-only forced election deadline so the single voter campaigns immediately.
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
    }
    for _ in 0..20 {
        mr.tick().unwrap();
        if mr.is_group_leader(SEQUENCER_GROUP_ID) {
            return;
        }
    }
    panic!("sequencer group did not reach single-node leadership");
}

/// Encode `batch` and propose it to the committed sequencer Raft log, returning
/// its committed Raft index and the encoded bytes (reused to drive `apply`, so
/// the index handed to `apply` is the SAME real committed index the drain will
/// read back). Single-voter groups commit on propose.
fn commit_epoch_batch(scheduler: &Scheduler, batch: EpochBatch) -> (u64, Vec<u8>) {
    let bytes =
        zerompk::to_msgpack_vec(&SequencerEntry::EpochBatch { batch }).expect("encode epoch batch");
    let index = {
        let mut mr = scheduler
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        mr.propose_to_group(SEQUENCER_GROUP_ID, bytes.clone())
            .expect("propose epoch batch to sequencer group")
    };
    (index, bytes)
}

/// A single-position `EpochBatch` at `epoch` carrying `txn`.
fn make_batch(epoch: u64, txn: &SequencedTxn) -> EpochBatch {
    EpochBatch {
        epoch,
        txns: vec![txn.clone()],
        epoch_system_ms: txn.epoch_system_ms,
    }
}

/// Register a capacity-1, pre-filled (hence permanently Full) fan-out channel
/// for `vshard` on the shared sequencer state machine, then `apply` the
/// committed entry `bytes` (Raft index `index`). The live `try_send` fan-out
/// hits `Full` and DROPS, recording the catch-up index — the exact drop the
/// drain must repair. The pre-fill payload keeps the receiver end (`_full_rx`)
/// alive so the channel reports `Full` (not `Closed`).
fn apply_with_full_channel(
    scheduler: &Scheduler,
    vshard: u32,
    index: u64,
    bytes: &[u8],
    fill: &SequencedTxn,
) {
    let (full_tx, full_rx) = tokio::sync::mpsc::channel(1);
    full_tx
        .try_send(SchedulerInput::Txn(fill.clone()))
        .expect("pre-fill the capacity-1 channel");
    // Keep the receiver alive for the duration of `apply` so the sender reports
    // Full rather than Closed (either records a catch-up index, but Full is the
    // backpressure case this test targets).
    let _full_rx = full_rx;
    let mut sm = scheduler
        .sequencer_state_machine
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    sm.set_vshard_sender(vshard, full_tx);
    sm.apply(index, bytes);
}

/// END-TO-END: a fan-out drop under a full channel, then a real `drain_catch_up`
/// that reads the committed sequencer Raft log, replays the dropped input, and
/// applies it to this scheduler's lock table.
///
/// This is the whole-mechanism proof the piece-wise unit tests cannot give: the
/// drain reads a GENUINE committed Raft entry (not a hand-built log slice) and
/// the replayed input mutates the REAL lock table. A conflicting holder is
/// pre-seeded so the replayed txn blocks in the lock table — landing observably
/// in `blocked` without a Data-Plane dispatch (no executor runs in this harness).
#[tokio::test]
async fn drain_replays_dropped_input_into_lock_table_end_to_end() {
    // Use the vShard that "test_coll" hashes to, so the batch's fan-out targets —
    // and its replay decodes for — this scheduler's vShard.
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, "test_coll").as_u32();
    let (mut scheduler, _dir) = build_test_scheduler(vshard);
    ensure_sequencer_leader(&scheduler);

    let txn = make_sequenced_txn(0, 0);
    let (committed_index, bytes) = commit_epoch_batch(&scheduler, make_batch(0, &txn));

    // Drive the real live drop: apply the committed entry against a full channel.
    apply_with_full_channel(&scheduler, vshard, committed_index, &bytes, &txn);
    {
        let sm = scheduler
            .sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        // Non-consuming proof the drop happened (leaves catch_up_from intact for
        // the drain to TAKE).
        assert!(
            sm.metrics.txns_dropped_backpressure.load(Ordering::Relaxed) >= 1,
            "apply on a full channel must drop and bookkeep the catch-up index"
        );
    }

    // Pre-seed a conflicting exclusive holder so the replayed txn BLOCKS (and so
    // never dispatches to a Data Plane that is not running in this harness).
    let keys = super::super::helpers::expand_rw_set(&txn);
    assert!(!keys.is_empty(), "txn must expand to at least one lock key");
    let sentinel = TxnId::new(u64::MAX, 0);
    {
        let mut lm = scheduler
            .lock_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(lm.acquire(sentinel, keys.clone()), AcquireOutcome::Ready);
    }

    let replayed_before = scheduler.metrics.catch_up_replayed.load(Ordering::Relaxed);
    let dispatched_before = scheduler.metrics.dispatch_count.load(Ordering::Relaxed);
    assert!(scheduler.blocked.is_empty());

    // THE mechanism under test: read the committed log, replay, re-apply.
    scheduler.drain_catch_up();

    // Exactly the one dropped input was replayed.
    assert_eq!(
        scheduler.metrics.catch_up_replayed.load(Ordering::Relaxed),
        replayed_before + 1,
        "drain must replay exactly the one dropped input from the committed log"
    );
    // The replayed input reached the lock table and queued behind the conflicting
    // holder — proving the dropped input was read from the REAL committed Raft
    // log, decoded, and re-applied through the live `process_scheduler_input`.
    let lock_owner = TxnId::new(0, 0);
    assert!(
        scheduler.blocked.contains_key(&lock_owner),
        "the replayed txn must have acquired-and-blocked in the lock table"
    );
    // Blocked never dispatches, so the whole path touched no executor.
    assert_eq!(
        scheduler.metrics.dispatch_count.load(Ordering::Relaxed),
        dispatched_before,
        "a blocked replayed txn must not dispatch"
    );
    // The catch-up entry was consumed exactly once (TAKE semantics).
    {
        let sm = scheduler
            .sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(
            sm.take_catch_up_from(vshard),
            None,
            "the drain must have TAKEn the catch-up index"
        );
    }
}

/// END-TO-END: when the replay range re-covers an input already delivered live
/// and still in-flight, the in-flight guard turns the overlapping replay into a
/// no-op — the input is not processed (or dispatched) twice — while a genuinely
/// dropped earlier input in the same range IS applied.
///
/// This exercises the guard's real end-to-end behavior: the drain replays from
/// the earliest dropped index forward (`[idx0 ..= idx1]`), which unavoidably
/// re-covers a later, non-dropped input. Epoch 0 (dropped) must be applied;
/// epoch 1 (delivered live, in-flight) must be skipped.
#[tokio::test]
async fn drain_skips_in_flight_overlap_no_double_dispatch_end_to_end() {
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, "test_coll").as_u32();
    let (mut scheduler, _dir) = build_test_scheduler(vshard);
    ensure_sequencer_leader(&scheduler);

    let txn0 = make_sequenced_txn(0, 0);
    let txn1 = make_sequenced_txn(1, 0);
    let (idx0, bytes0) = commit_epoch_batch(&scheduler, make_batch(0, &txn0));
    let (idx1, bytes1) = commit_epoch_batch(&scheduler, make_batch(1, &txn1));
    assert!(idx1 > idx0, "second batch commits at a later Raft index");

    // A single conflicting holder on the shared key (both txns write test_coll
    // surrogate 1) so every txn BLOCKS rather than dispatching to a Data Plane
    // that is not running.
    let keys = super::super::helpers::expand_rw_set(&txn0);
    let sentinel = TxnId::new(u64::MAX, 0);
    {
        let mut lm = scheduler
            .lock_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(lm.acquire(sentinel, keys.clone()), AcquireOutcome::Ready);
    }

    // Deliver epoch 1 LIVE: it acquires, blocks behind the sentinel, and is now
    // in-flight (its (epoch, position) sits in `blocked`). It was NOT dropped.
    scheduler.process_scheduler_input(SchedulerInput::Txn(txn1.clone()));
    let live_owner = TxnId::new(1, 0);
    assert!(
        scheduler.blocked.contains_key(&live_owner),
        "live epoch-1 txn must be in-flight (blocked) before the drain"
    );

    // Drop BOTH committed entries through the full-channel fan-out so the drain's
    // replay range spans `[idx0 ..= idx1]` (min-collapse keeps idx0; last
    // committed index advances to idx1), re-covering the live epoch-1 input.
    apply_with_full_channel(&scheduler, vshard, idx0, &bytes0, &txn0);
    apply_with_full_channel(&scheduler, vshard, idx1, &bytes1, &txn1);

    let dispatched_before = scheduler.metrics.dispatch_count.load(Ordering::Relaxed);
    let blocked_before = scheduler.blocked.len();
    assert_eq!(blocked_before, 1, "only the live epoch-1 txn is in-flight");

    scheduler.drain_catch_up();

    // Epoch 0 (genuinely dropped, new to this scheduler) was applied → it now
    // blocks behind the sentinel too.
    let dropped_owner = TxnId::new(0, 0);
    assert!(
        scheduler.blocked.contains_key(&dropped_owner),
        "the genuinely-dropped epoch-0 input must be applied by the drain"
    );
    // Epoch 1 was already in-flight; the guard must have skipped its replay — no
    // duplicate entry, no second dispatch. Exactly the two distinct owners remain.
    assert_eq!(
        scheduler.blocked.len(),
        2,
        "the in-flight overlap must not create a duplicate blocked entry"
    );
    assert!(scheduler.blocked.contains_key(&live_owner));
    assert_eq!(
        scheduler.metrics.dispatch_count.load(Ordering::Relaxed),
        dispatched_before,
        "the guarded overlap must not cause a second dispatch"
    );
}

// ── `is_caught_up` sentinel handling ─────────────────────────────────────────

/// A freshly-recovered scheduler (`fully_applied_epoch` still the
/// `NOT_YET_APPLIED_EPOCH` sentinel) with a REAL, non-zero rebuild target must
/// NOT report caught-up. Naively comparing `u64::MAX >= rebuild_target_epoch`
/// (the bug) would say "caught up" before a single epoch was re-applied.
#[tokio::test]
async fn is_caught_up_false_when_fully_applied_is_sentinel_and_target_is_real() {
    let (mut scheduler, _dir) = build_test_scheduler(0);
    scheduler.rebuild_target_epoch = 5;
    assert_eq!(
        scheduler.applied.fully_applied_epoch(),
        NOT_YET_APPLIED_EPOCH
    );

    assert!(
        !scheduler.is_caught_up(),
        "sentinel fully_applied_epoch with a real rebuild target must not be caught up"
    );
}

/// Once the applied watermark advances to (or past) a real rebuild target,
/// the scheduler correctly reports caught-up.
#[tokio::test]
async fn is_caught_up_true_once_fully_applied_reaches_target() {
    let (mut scheduler, _dir) = build_test_scheduler(0);
    scheduler.rebuild_target_epoch = 5;

    scheduler.applied = AppliedGate::new(5, BTreeSet::new());
    assert!(
        scheduler.is_caught_up(),
        "fully_applied_epoch == rebuild_target_epoch must be caught up"
    );

    scheduler.applied = AppliedGate::new(7, BTreeSet::new());
    assert!(
        scheduler.is_caught_up(),
        "fully_applied_epoch > rebuild_target_epoch must be caught up"
    );
}

/// A greenfield node with NO Calvin history at all: `read_applied_recovery`
/// seeds `max_applied_epoch` (hence `rebuild_target_epoch`) to
/// `NOT_YET_APPLIED_EPOCH` too (see `recovery.rs`'s
/// `greenfield_returns_sentinel_and_empty_tail` test) — this is distinct from
/// a real target of epoch 0 (which would report `max_applied_epoch == 0`).
/// With nothing to rebuild, the scheduler is trivially caught up even though
/// `fully_applied_epoch` is still the sentinel.
#[tokio::test]
async fn is_caught_up_true_when_no_rebuild_target_exists() {
    let (mut scheduler, _dir) = build_test_scheduler(0);
    // `build_test_scheduler` defaults `rebuild_target_epoch` to `0` (a REAL
    // target) for its own catch-up-drain tests; set it to the sentinel here to
    // model the actual greenfield-recovery value.
    scheduler.rebuild_target_epoch = NOT_YET_APPLIED_EPOCH;
    assert_eq!(
        scheduler.applied.fully_applied_epoch(),
        NOT_YET_APPLIED_EPOCH
    );

    assert!(
        scheduler.is_caught_up(),
        "no rebuild target (greenfield node) must report caught-up"
    );
}

/// A false vote from either participant makes the only global verdict abort;
/// applying that durable verdict broadcasts the abort to every parked local
/// participant. The scheduler's `resume_on_verdict(false)` then dispatches a
/// drop, never a resolve/flush, on each recipient.
#[tokio::test]
async fn two_participant_false_vote_broadcasts_global_abort_to_every_scheduler() {
    let registry = CalvinCompletionRegistry::new_detached();
    let txn = nodedb_cluster::calvin::TxnId::new(14, 2);
    let txn_id = TxnId::new(14, 2);
    let (mut first_scheduler, _first_dir, mut first_data) =
        build_test_scheduler_with_data_side(7, Arc::clone(&registry));
    let (mut second_scheduler, _second_dir, mut second_data) =
        build_test_scheduler_with_data_side(9, Arc::clone(&registry));
    first_scheduler
        .pending
        .insert(txn_id, staged_pending(make_sequenced_txn(14, 2), txn_id));
    second_scheduler
        .pending
        .insert(txn_id, staged_pending(make_sequenced_txn(14, 2), txn_id));

    // Local staging votes only park their own staged slices; neither the
    // affirmative nor the failed participant may resolve or drop unilaterally.
    first_scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Ok, Some(true)));
    second_scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Error, None));
    for (scheduler, data_side) in [
        (&first_scheduler, &mut first_data),
        (&second_scheduler, &mut second_data),
    ] {
        assert!(matches!(
            scheduler
                .pending
                .get(&txn_id)
                .and_then(|pending| pending.commit_state),
            Some(CommitState::AwaitingVerdict)
        ));
        assert!(data_side.request_rx.try_pop().is_err());
    }

    // Model the replicated vote entries and their resulting durable verdict.
    // The shared registry sends each scheduler's actual registered channel.
    registry.seed_expected(txn, 2);
    registry.note_vote(txn, 7, true);
    assert!(registry.drain_unproposed_verdicts().is_empty());
    registry.note_vote(txn, 9, false);
    assert_eq!(registry.drain_unproposed_verdicts(), vec![(txn, false)]);
    registry.note_verdict(txn, false);
    assert_eq!(registry.verdict(txn), Some(false));

    let first_signal = first_scheduler
        .verdict_rx
        .try_recv()
        .expect("registry must signal the first registered scheduler");
    let second_signal = second_scheduler
        .verdict_rx
        .try_recv()
        .expect("registry must signal the second registered scheduler");
    first_scheduler.handle_verdict_signal(first_signal);
    second_scheduler.handle_verdict_signal(second_signal);

    for (scheduler, data_side) in [
        (&first_scheduler, &mut first_data),
        (&second_scheduler, &mut second_data),
    ] {
        assert!(matches!(
            scheduler
                .pending
                .get(&txn_id)
                .and_then(|pending| pending.commit_state),
            Some(CommitState::AwaitingResolve {
                committed: false,
                redo_lsn: None
            })
        ));
        let request = data_side
            .request_rx
            .try_pop()
            .expect("global abort must dispatch a drop to every participant");
        assert!(matches!(
            request.inner.plan,
            PhysicalPlan::Meta(MetaOp::CalvinDrop {
                epoch: 14,
                position: 2
            })
        ));
        assert!(data_side.request_rx.try_pop().is_err());
    }
}
