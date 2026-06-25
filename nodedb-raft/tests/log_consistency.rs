// SPDX-License-Identifier: BUSL-1.1

//! Log-consistency invariants exercised at the crate boundary.
//!
//! Inline unit tests in `log.rs` cover the basic shape (empty, append,
//! range, single conflict-at-tail, snapshot compaction). These tests
//! drive the multi-step paths that are easy to get wrong: matching
//! prefixes followed by conflicting suffixes, idempotent re-delivery
//! of the same batch, AE prev-log mismatch and the resulting backtrack
//! hint, and append/AE behavior straddling a snapshot boundary.

use std::time::Duration;

use nodedb_raft::{
    AppendEntriesRequest, LogEntry, RaftLog, RaftNode,
    node::config::RaftConfig,
    storage::{LogStorage, MemStorage},
};

fn entry(term: u64, index: u64) -> LogEntry {
    LogEntry {
        term,
        index,
        data: vec![],
    }
}

fn entry_with(term: u64, index: u64, data: &[u8]) -> LogEntry {
    LogEntry {
        term,
        index,
        data: data.to_vec(),
    }
}

fn follower_config() -> RaftConfig {
    RaftConfig {
        node_id: 1,
        group_id: 1,
        peers: vec![2, 3],
        learners: vec![],
        observers: vec![],
        starts_as_learner: false,
        starts_as_observer: false,
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
        log_compaction_threshold: None,
    }
}

/// A leader sends a batch where the prefix matches the follower's log
/// (same term) but the suffix conflicts (different term). Only the
/// conflicting suffix should be truncated; the matching prefix must
/// be left intact and not duplicated.
#[test]
fn matching_prefix_then_conflicting_suffix_truncates_only_suffix() {
    let mut log = RaftLog::new(MemStorage::new());
    log.append(entry_with(1, 1, b"a")).unwrap();
    log.append(entry_with(1, 2, b"b")).unwrap();
    log.append(entry_with(1, 3, b"c")).unwrap();

    // Leader retransmits index 2 (matching) and overwrites 3 with new term.
    let batch = vec![
        entry_with(1, 2, b"b"),
        entry_with(2, 3, b"c2"),
        entry_with(2, 4, b"d"),
    ];
    log.append_entries(1, &batch).unwrap();

    assert_eq!(log.last_index(), 4);
    assert_eq!(log.term_at(2), Some(1), "matching prefix preserved");
    assert_eq!(log.entry_at(2).unwrap().data, b"b");
    assert_eq!(log.term_at(3), Some(2), "conflict overwritten");
    assert_eq!(log.entry_at(3).unwrap().data, b"c2");
    assert_eq!(log.term_at(4), Some(2));
}

/// Re-delivering the exact same batch (e.g. retried AE) must be a
/// no-op: no truncation, no duplication, no spurious appends.
#[test]
fn idempotent_reapply_of_same_batch() {
    let mut log = RaftLog::new(MemStorage::new());
    let batch = vec![entry(1, 1), entry(1, 2), entry(1, 3)];
    log.append_entries(0, &batch).unwrap();
    let snapshot_after_first = log.last_index();

    // Replay.
    log.append_entries(0, &batch).unwrap();
    assert_eq!(log.last_index(), snapshot_after_first);
    assert_eq!(log.entry_at(1).unwrap().term, 1);
    assert_eq!(log.entry_at(2).unwrap().term, 1);
    assert_eq!(log.entry_at(3).unwrap().term, 1);

    // Storage shouldn't have grown beyond the original three entries.
    let persisted = log.storage().load_entries_after(0).unwrap();
    assert_eq!(persisted.len(), 3);
}

/// AE with `prev_log_index` past the follower's tail must be rejected,
/// and the response's `last_log_index` hint must let the leader skip
/// directly to a useful retry point (no decrement-by-one walk needed).
#[test]
fn ae_prev_log_mismatch_returns_backtrack_hint() {
    let mut node = RaftNode::new(follower_config(), MemStorage::new());

    // Seed the follower with one entry at term 1, index 1.
    let bootstrap = AppendEntriesRequest {
        term: 1,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry_with(1, 1, b"x")],
        leader_commit: 0,
        group_id: 1,
    };
    let resp = node.handle_append_entries(&bootstrap);
    assert!(resp.success);

    // Leader thinks we have entries through index 5; we only have 1.
    let stale = AppendEntriesRequest {
        term: 1,
        leader_id: 2,
        prev_log_index: 5,
        prev_log_term: 1,
        entries: vec![entry_with(1, 6, b"y")],
        leader_commit: 0,
        group_id: 1,
    };
    let resp = node.handle_append_entries(&stale);
    assert!(!resp.success, "AE with bad prev must be rejected");
    assert_eq!(
        resp.last_log_index, 1,
        "rejection must surface follower's true tail for fast backtrack"
    );
}

/// AE with matching prev_log_index but mismatched prev_log_term must
/// also be rejected — same backtrack-hint contract.
#[test]
fn ae_prev_log_term_mismatch_returns_backtrack_hint() {
    let mut node = RaftNode::new(follower_config(), MemStorage::new());

    let bootstrap = AppendEntriesRequest {
        term: 5,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry_with(2, 1, b"a"), entry_with(2, 2, b"b")],
        leader_commit: 0,
        group_id: 1,
    };
    assert!(node.handle_append_entries(&bootstrap).success);

    // Same prev_log_index = 2, but claim it was term 4 (we have term 2).
    let bad_term = AppendEntriesRequest {
        term: 5,
        leader_id: 2,
        prev_log_index: 2,
        prev_log_term: 4,
        entries: vec![entry_with(5, 3, b"c")],
        leader_commit: 0,
        group_id: 1,
    };
    let resp = node.handle_append_entries(&bad_term);
    assert!(!resp.success);
    assert_eq!(resp.last_log_index, 2);
}

/// After applying a snapshot at index N, queries for indices <= N must
/// reflect compaction (term_at returns the snapshot term at the
/// boundary, None below; entry_at returns None at and below; range
/// queries below the snapshot return LogCompacted).
#[test]
fn append_then_snapshot_then_appendentries_post_boundary() {
    let mut log = RaftLog::new(MemStorage::new());
    for i in 1..=5 {
        log.append(entry(1, i)).unwrap();
    }
    log.apply_snapshot(3, 1);

    // Boundary semantics.
    assert_eq!(log.snapshot_index(), 3);
    assert_eq!(log.snapshot_term(), 1);
    assert_eq!(log.term_at(3), Some(1), "boundary term still queryable");
    assert!(
        log.entry_at(3).is_none(),
        "boundary index has no live entry — covered by snapshot"
    );
    assert!(log.entry_at(2).is_none(), "below-snapshot is compacted");
    assert!(
        log.entries_range(2, 5).is_err(),
        "range crossing into compacted region is an error, not silent truncation"
    );

    // Post-boundary entries still queryable.
    assert!(log.entry_at(4).is_some());
    assert!(log.entry_at(5).is_some());

    // A leader can still extend the log past the existing tail after
    // the snapshot — entries at indices > last_index must append.
    log.append_entries(5, &[entry(2, 6), entry(2, 7)]).unwrap();
    assert_eq!(log.last_index(), 7);
    assert_eq!(log.term_at(7), Some(2));
}

/// MemStorage must reflect truncation through the LogStorage trait so
/// that a restart sees the post-truncation state, not the pre-truncation
/// one. This is the persistence side of the conflict-detection path.
#[test]
fn truncation_propagates_to_storage() {
    let mut log = RaftLog::new(MemStorage::new());
    log.append(entry(1, 1)).unwrap();
    log.append(entry(1, 2)).unwrap();
    log.append(entry(1, 3)).unwrap();

    // Conflict at index 2 truncates 2 and 3, replaces with new entries.
    log.append_entries(1, &[entry(2, 2), entry(2, 3), entry(2, 4)])
        .unwrap();

    let persisted = log.storage().load_entries_after(0).unwrap();
    assert_eq!(persisted.len(), 4, "storage tracks truncation + reappend");
    let terms: Vec<u64> = persisted.iter().map(|e| e.term).collect();
    assert_eq!(terms, vec![1, 2, 2, 2]);
}

/// Heartbeat (empty AE) with a `leader_commit` past the follower's
/// `last_index` must clamp to `last_index` — never advance commit
/// past entries we don't have.
#[test]
fn leader_commit_clamped_to_last_index() {
    let mut node = RaftNode::new(follower_config(), MemStorage::new());

    let req = AppendEntriesRequest {
        term: 1,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry(1, 1), entry(1, 2)],
        leader_commit: 99, // way past tail
        group_id: 1,
    };
    let resp = node.handle_append_entries(&req);
    assert!(resp.success);
    assert_eq!(
        node.commit_index(),
        2,
        "commit_index must not exceed last log index"
    );
}
