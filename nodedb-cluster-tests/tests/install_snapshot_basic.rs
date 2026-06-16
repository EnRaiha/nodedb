// SPDX-License-Identifier: BUSL-1.1

//! Basic InstallSnapshot recovery tests exercising `MultiRaft::handle_install_snapshot`
//! and the snapshot framing CRC path in `decode_snapshot_chunk`.
//!
//! Scope (per the A6 task decision):
//! - Single-chunk complete-snapshot application (done = true).
//! - Partial snapshot correctly deferred (done = false, no state mutation).
//! - Framing-CRC rejection via `decode_snapshot_chunk` (the path exercised
//!   in `handle_rpc.rs` before calling `handle_install_snapshot`).
//!
//! What is NOT tested here:
//! - Multi-chunk accumulation (chunk accumulator is its own task A6b).
//! - Full `RaftLoop` spin-up (requires tokio runtime + transport stubs).

use nodedb_cluster::{MultiRaft, RoutingTable};
use nodedb_raft::{
    InstallSnapshotRequest, SnapshotEngineId, decode_snapshot_chunk, encode_snapshot_chunk,
};

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Build a `MultiRaft` with group 0 as a single-node follower, backed by
/// a temporary redb file.
fn single_node_multi_raft(dir: &std::path::Path) -> MultiRaft {
    let routing = RoutingTable::uniform(1, &[1], 1);
    let mut mr = MultiRaft::new(1, routing, dir.to_path_buf());
    // Group 0 is the metadata group; a single peer list of [] means "I am the only
    // member" — this is the usual single-seed bootstrap configuration.
    mr.add_group(0, vec![]).unwrap();
    mr
}

fn snapshot_req(term: u64, index: u64, done: bool, data: Vec<u8>) -> InstallSnapshotRequest {
    InstallSnapshotRequest {
        term,
        leader_id: 99,
        last_included_index: index,
        last_included_term: term,
        offset: 0,
        data,
        done,
        group_id: 0,
        total_size: 0,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// A complete single-chunk snapshot (done = true) must advance commit_index
/// and last_applied to last_included_index.
#[test]
fn complete_snapshot_applies_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut mr = single_node_multi_raft(dir.path());

    let req = snapshot_req(2, 100, true, vec![]);
    let resp = mr.handle_install_snapshot(&req).unwrap();

    assert_eq!(resp.term, 2, "response term must reflect snapshot term");

    let node = mr.groups_mut().get_mut(&0).unwrap();
    assert_eq!(
        node.commit_index(),
        100,
        "commit_index must advance to last_included_index"
    );
    assert_eq!(
        node.last_applied(),
        100,
        "last_applied must advance to last_included_index"
    );
}

/// A partial chunk (done = false) must NOT mutate commit_index or last_applied.
/// The snapshot is still being streamed; state must remain at baseline.
#[test]
fn partial_snapshot_defers_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut mr = single_node_multi_raft(dir.path());

    // Baseline: commit_index and last_applied should be 0 on a fresh node.
    {
        let node = mr.groups_mut().get_mut(&0).unwrap();
        assert_eq!(node.commit_index(), 0, "baseline commit_index must be 0");
        assert_eq!(node.last_applied(), 0, "baseline last_applied must be 0");
    }

    let req = snapshot_req(3, 200, false, vec![]);
    let _resp = mr.handle_install_snapshot(&req).unwrap();

    let node = mr.groups_mut().get_mut(&0).unwrap();
    assert_eq!(
        node.commit_index(),
        0,
        "partial snapshot (done=false) must not advance commit_index"
    );
    assert_eq!(
        node.last_applied(),
        0,
        "partial snapshot (done=false) must not advance last_applied"
    );
}

/// `decode_snapshot_chunk` must reject a chunk with a flipped CRC byte.
///
/// This covers the framing-CRC validation path in `handle_rpc.rs` that
/// runs before `handle_install_snapshot` is called for non-empty data.
#[test]
fn framing_crc_corruption_rejected() {
    let payload = b"engine snapshot payload";
    let mut framed = encode_snapshot_chunk(SnapshotEngineId::DocumentSchemaless, payload);

    // CRC occupies bytes 8..12 in the frame header (magic[4] + version[2] +
    // engine_id[2] + crc32c[4]).  Flip one byte to corrupt the CRC.
    framed[8] ^= 0xFF;

    let result = decode_snapshot_chunk(&framed);
    assert!(
        result.is_err(),
        "decode_snapshot_chunk must reject a chunk with a corrupted CRC"
    );

    match result.unwrap_err() {
        nodedb_raft::SnapshotFramingError::CrcMismatch { .. } => {}
        other => panic!("expected CrcMismatch, got {other:?}"),
    }
}

/// `decode_snapshot_chunk` must reject a chunk that is too short to contain
/// the full frame header.
#[test]
fn framing_truncated_chunk_rejected() {
    // A frame needs at least 12 bytes (magic + version + engine_id + crc).
    let too_short = b"NDSN\x00\x01"; // only 6 bytes
    let result = decode_snapshot_chunk(too_short);
    assert!(
        result.is_err(),
        "decode_snapshot_chunk must reject a truncated chunk"
    );

    match result.unwrap_err() {
        nodedb_raft::SnapshotFramingError::Truncated(_) => {}
        other => panic!("expected Truncated, got {other:?}"),
    }
}

/// A correctly framed chunk must round-trip through encode → decode.
#[test]
fn framing_valid_chunk_accepted() {
    let payload = b"valid engine data";
    let framed = encode_snapshot_chunk(SnapshotEngineId::Vector, payload);

    let result = decode_snapshot_chunk(&framed);
    assert!(result.is_ok(), "valid chunk must decode without error");

    let (engine_id, decoded_payload) = result.unwrap();
    assert_eq!(engine_id, SnapshotEngineId::Vector);
    assert_eq!(decoded_payload, payload);
}
