// SPDX-License-Identifier: BUSL-1.1

//! Corruption in any of (columnar, FTS, Raft snapshot) segment files must
//! surface as `SegmentQuarantined` and appear in the debug enumeration endpoint.
//!
//! Each test writes a valid segment using the real segment writer, corrupts one
//! byte to flip the footer CRC, then drives the quarantine wrapper through two
//! open attempts — verifying the first returns a plain engine error (first
//! strike) and the second returns `SegmentQuarantined` (second strike).
//!
//! The final test verifies the HTTP endpoint's JSON response shape by
//! populating a registry with three entries and exercising the same
//! serialisation path used by the `/v1/cluster/debug/quarantined-segments`
//! handler.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb::storage::quarantine::{
    QuarantineEngine, QuarantineRegistry, SegmentKey,
    engines::{
        ColumnarOrQuarantine, FtsOrQuarantine, RaftOrQuarantine,
        decode_snapshot_chunk_with_quarantine, open_fts_segment_with_quarantine,
        open_segment_with_quarantine,
    },
};
use nodedb_columnar::{ColumnarMemtable, SegmentWriter};
use nodedb_raft::snapshot_framing::{SnapshotEngineId, encode_snapshot_chunk};
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a single-column schema with one Int64 column.
fn minimal_columnar_schema() -> ColumnarSchema {
    ColumnarSchema::new(vec![ColumnDef::required("id", ColumnType::Int64)]).expect("valid schema")
}

/// Produce a valid sealed columnar segment with one row.
fn write_columnar_segment() -> Vec<u8> {
    let schema = minimal_columnar_schema();
    let mut mem = ColumnarMemtable::new(&schema);
    mem.append_row(&[Value::Integer(42)]).expect("append row");

    SegmentWriter::plain()
        .write_segment(&schema, mem.columns(), mem.row_count(), None)
        .expect("write segment")
}

/// Produce a valid FTS segment with one term.
fn write_fts_segment() -> Vec<u8> {
    use nodedb_fts::CompactPosting;
    use nodedb_types::Surrogate;

    let mut postings: HashMap<String, Vec<CompactPosting>> = HashMap::new();
    postings.insert(
        "hello".to_string(),
        vec![CompactPosting {
            doc_id: Surrogate(1),
            term_freq: 1,
            fieldnorm: 128,
            positions: vec![0],
        }],
    );
    nodedb_fts::lsm::segment::writer::flush_to_segment(postings).expect("flush fts segment")
}

/// Produce a valid framed Raft snapshot chunk.
fn write_raft_chunk() -> Vec<u8> {
    encode_snapshot_chunk(SnapshotEngineId::Columnar, b"snapshot-payload")
}

/// Corrupt the last byte of a segment buffer (flips the footer CRC).
fn corrupt_last_byte(mut bytes: Vec<u8>) -> Vec<u8> {
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    bytes
}

// ── columnar corruption ───────────────────────────────────────────────────────

#[test]
fn columnar_real_segment_corrupt_crc_quarantines_on_second_strike() {
    let reg = Arc::new(QuarantineRegistry::new());
    let corrupt = corrupt_last_byte(write_columnar_segment());

    // First open: CRC-class error → first strike → ColumnarError returned.
    let r1 = open_segment_with_quarantine(&reg, &corrupt, "coll", "seg-col-1");
    assert!(
        matches!(r1, Err(ColumnarOrQuarantine::Columnar(_))),
        "first strike must return a ColumnarError"
    );

    // Second open: second strike → SegmentQuarantined.
    let r2 = open_segment_with_quarantine(&reg, &corrupt, "coll", "seg-col-1");
    assert!(
        matches!(r2, Err(ColumnarOrQuarantine::Quarantined(_))),
        "second strike must return SegmentQuarantined"
    );

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1, "exactly one segment must be quarantined");
    assert_eq!(snap[0].engine, "columnar");
    assert_eq!(snap[0].collection, "coll");
    assert_eq!(snap[0].segment_id, "seg-col-1");
    assert_eq!(snap[0].strikes, 2);
}

#[test]
fn columnar_good_segment_readable_after_quarantine_of_different_segment() {
    let reg = Arc::new(QuarantineRegistry::new());
    let good = write_columnar_segment();
    let corrupt = corrupt_last_byte(write_columnar_segment());

    // Quarantine the corrupt segment.
    open_segment_with_quarantine(&reg, &corrupt, "coll", "seg-corrupt").ok();
    open_segment_with_quarantine(&reg, &corrupt, "coll", "seg-corrupt").ok();

    // The good segment must still open successfully.
    let result = open_segment_with_quarantine(&reg, &good, "coll", "seg-good");
    assert!(result.is_ok(), "good segment must remain readable");
}

// ── FTS corruption ────────────────────────────────────────────────────────────

#[test]
fn fts_real_segment_corrupt_crc_quarantines_on_second_strike() {
    let reg = Arc::new(QuarantineRegistry::new());
    let corrupt = corrupt_last_byte(write_fts_segment());

    // First open: CRC-class error → first strike → SegmentError returned.
    let r1 = open_fts_segment_with_quarantine(&reg, corrupt.clone(), "ftscoll", "seg-fts-1");
    assert!(
        matches!(r1, Err(FtsOrQuarantine::Segment(_))),
        "first strike must return SegmentError, got: {r1:?}"
    );

    // Second open: second strike → SegmentQuarantined.
    let r2 = open_fts_segment_with_quarantine(&reg, corrupt, "ftscoll", "seg-fts-1");
    assert!(
        matches!(r2, Err(FtsOrQuarantine::Quarantined(_))),
        "second strike must return SegmentQuarantined, got: {r2:?}"
    );

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1, "exactly one segment must be quarantined");
    assert_eq!(snap[0].engine, "fts");
    assert_eq!(snap[0].collection, "ftscoll");
    assert_eq!(snap[0].segment_id, "seg-fts-1");
    assert_eq!(snap[0].strikes, 2);
}

// ── Raft snapshot corruption ──────────────────────────────────────────────────

#[test]
fn raft_snapshot_corrupt_crc_quarantines_on_second_strike() {
    let reg = Arc::new(QuarantineRegistry::new());

    // Flip a bit in the CRC field (bytes 8–11 of the frame header).
    let mut corrupt = write_raft_chunk();
    corrupt[8] ^= 0xFF;

    // First decode: CRC mismatch → first strike → SnapshotFramingError returned.
    let r1 = decode_snapshot_chunk_with_quarantine(&reg, &corrupt, "group=1:index=99");
    assert!(
        matches!(r1, Err(RaftOrQuarantine::Framing(_))),
        "first strike must return a SnapshotFramingError, got: {r1:?}"
    );

    // Second decode: second strike → SegmentQuarantined.
    let r2 = decode_snapshot_chunk_with_quarantine(&reg, &corrupt, "group=1:index=99");
    assert!(
        matches!(r2, Err(RaftOrQuarantine::Quarantined(_))),
        "second strike must return SegmentQuarantined, got: {r2:?}"
    );

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1, "exactly one snapshot must be quarantined");
    assert_eq!(snap[0].engine, "raft");
    assert_eq!(snap[0].collection, "_raft_snapshot");
    assert_eq!(snap[0].segment_id, "group=1:index=99");
    assert_eq!(snap[0].strikes, 2);
}

// ── HTTP endpoint response shape ──────────────────────────────────────────────

/// Mirror of the private response shape used by the
/// `/v1/cluster/debug/quarantined-segments` handler.
///
/// Matches the field names and structure of `QuarantinedSegmentsResponse` in
/// `routes/cluster_debug/quarantined_segments.rs` so the test verifies the
/// serialised JSON shape end-to-end without requiring a live HTTP server.
#[derive(serde::Serialize, serde::Deserialize)]
struct QuarantinedSegmentEntry {
    engine: String,
    collection: String,
    segment_id: String,
    quarantined_at_unix_ms: u64,
    last_error_summary: String,
    strikes: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct QuarantinedSegmentsResponse {
    segments: Vec<QuarantinedSegmentEntry>,
}

/// Serialise a quarantine registry snapshot using the same `sonic_rs` path as
/// `ok_json` in `cluster_debug/guard.rs`, then parse and verify all three
/// engine entries are present with the expected shape.
#[tokio::test]
async fn http_endpoint_response_lists_all_three_quarantined_engines() {
    let reg = QuarantineRegistry::new();

    // Seed one quarantined entry per engine by driving two strikes each.
    for (engine, collection, segment_id) in [
        (QuarantineEngine::Columnar, "coll-a", "seg-a"),
        (QuarantineEngine::Fts, "coll-b", "seg-b"),
        (QuarantineEngine::Raft, "_raft_snapshot", "group=2:index=1"),
    ] {
        let k = SegmentKey {
            engine,
            collection: collection.to_string(),
            segment_id: segment_id.to_string(),
        };
        reg.record_failure(k.clone(), "crc", None)
            .expect("first strike must be Ok");
        reg.record_failure(k, "crc", None)
            .expect_err("second strike must be Err");
    }

    assert_eq!(
        reg.quarantined_snapshot().len(),
        3,
        "all three engines must be quarantined before testing the response shape"
    );

    // Replicate the handler's response-building logic (sans auth gate).
    let snapshot = reg.quarantined_snapshot();
    let mut entries: Vec<QuarantinedSegmentEntry> = snapshot
        .into_iter()
        .map(|s| QuarantinedSegmentEntry {
            engine: s.engine,
            collection: s.collection,
            segment_id: s.segment_id,
            quarantined_at_unix_ms: s.quarantined_at_unix_ms,
            last_error_summary: s.last_error_summary,
            strikes: s.strikes,
        })
        .collect();
    // Same sort order as the handler: engine → collection → segment_id.
    entries.sort_by(|a, b| {
        a.engine
            .cmp(&b.engine)
            .then_with(|| a.collection.cmp(&b.collection))
            .then_with(|| a.segment_id.cmp(&b.segment_id))
    });

    let response_body = sonic_rs::to_string(&QuarantinedSegmentsResponse { segments: entries })
        .expect("serialization must succeed");

    // Deserialise and assert structural correctness.
    let parsed: QuarantinedSegmentsResponse =
        sonic_rs::from_str(&response_body).expect("response body must be valid JSON");

    assert_eq!(
        parsed.segments.len(),
        3,
        "response must contain exactly three quarantined segments"
    );

    // After sorting, order is: columnar, fts, raft.
    let engines: Vec<&str> = parsed.segments.iter().map(|e| e.engine.as_str()).collect();
    assert_eq!(
        engines,
        vec!["columnar", "fts", "raft"],
        "engines must appear in sorted order"
    );

    for entry in &parsed.segments {
        assert_eq!(entry.strikes, 2, "every entry must have exactly 2 strikes");
        assert!(
            entry.quarantined_at_unix_ms > 0,
            "quarantine timestamp must be non-zero"
        );
        assert!(
            !entry.last_error_summary.is_empty(),
            "last_error_summary must be non-empty"
        );
    }

    // Spot-check individual field values.
    let col = parsed
        .segments
        .iter()
        .find(|e| e.engine == "columnar")
        .expect("columnar entry");
    assert_eq!(col.collection, "coll-a");
    assert_eq!(col.segment_id, "seg-a");

    let fts = parsed
        .segments
        .iter()
        .find(|e| e.engine == "fts")
        .expect("fts entry");
    assert_eq!(fts.collection, "coll-b");
    assert_eq!(fts.segment_id, "seg-b");

    let raft = parsed
        .segments
        .iter()
        .find(|e| e.engine == "raft")
        .expect("raft entry");
    assert_eq!(raft.collection, "_raft_snapshot");
    assert_eq!(raft.segment_id, "group=2:index=1");
}
