// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the corrupt-segment quarantine subsystem.
//!
//! Tests cover the registry's two-strike logic, startup-scan rebuild,
//! per-engine open wrappers, and the HTTP/metrics snapshot surfaces.

use std::sync::Arc;

use nodedb::storage::quarantine::{
    QuarantineEngine, QuarantineRegistry, SegmentKey,
    engines::{ColumnarOrQuarantine, FtsOrQuarantine, VectorOrQuarantine},
};

// ── Registry: two-strike logic ───────────────────────────────────────────────

#[test]
fn quarantine_registry_first_strike_returns_ok() {
    let reg = QuarantineRegistry::new();
    let k = SegmentKey {
        engine: QuarantineEngine::Columnar,
        collection: "c".into(),
        segment_id: "1".into(),
    };
    assert!(reg.record_failure(k, "crc", None).is_ok());
}

#[test]
fn quarantine_registry_second_strike_returns_quarantined() {
    let reg = QuarantineRegistry::new();
    let k = SegmentKey {
        engine: QuarantineEngine::Columnar,
        collection: "c".into(),
        segment_id: "2".into(),
    };
    reg.record_failure(k.clone(), "crc", None).unwrap();
    let err = reg.record_failure(k, "crc", None).unwrap_err();
    assert!(
        matches!(
            err,
            nodedb::storage::quarantine::QuarantineError::SegmentQuarantined { .. }
        ),
        "{err}"
    );
}

#[test]
fn quarantine_registry_success_resets_strikes() {
    let reg = QuarantineRegistry::new();
    let k = SegmentKey {
        engine: QuarantineEngine::Fts,
        collection: "c".into(),
        segment_id: "3".into(),
    };
    reg.record_failure(k.clone(), "crc", None).unwrap();
    reg.record_success(&k);
    // First failure after reset is strike 1 again — not quarantined.
    assert!(reg.record_failure(k, "crc", None).is_ok());
}

#[test]
fn quarantine_registry_quarantined_ignores_success_call() {
    let reg = QuarantineRegistry::new();
    let k = SegmentKey {
        engine: QuarantineEngine::Vector,
        collection: "c".into(),
        segment_id: "4".into(),
    };
    reg.record_failure(k.clone(), "crc", None).unwrap();
    reg.record_failure(k.clone(), "crc", None).unwrap_err();
    // success() must NOT lift the quarantine.
    reg.record_success(&k);
    let err = reg.record_failure(k, "crc", None).unwrap_err();
    assert!(
        matches!(
            err,
            nodedb::storage::quarantine::QuarantineError::SegmentQuarantined { .. }
        ),
        "{err}"
    );
}

#[test]
fn quarantine_registry_concurrent_second_strike_both_see_quarantined() {
    let reg = Arc::new(QuarantineRegistry::new());
    let k = SegmentKey {
        engine: QuarantineEngine::Raft,
        collection: "c".into(),
        segment_id: "5".into(),
    };
    // Pre-seed one strike so both threads compete for the second.
    reg.record_failure(k.clone(), "first", None).unwrap();

    let r1 = Arc::clone(&reg);
    let k1 = k.clone();
    let r2 = Arc::clone(&reg);
    let k2 = k.clone();
    let t1 = std::thread::spawn(move || r1.record_failure(k1, "second", None));
    let t2 = std::thread::spawn(move || r2.record_failure(k2, "second", None));

    assert!(t1.join().unwrap().is_err());
    assert!(t2.join().unwrap().is_err());
    // Exactly one entry in the registry.
    assert_eq!(reg.quarantined_snapshot().len(), 1);
}

// ── Startup-scan rebuild ─────────────────────────────────────────────────────

#[test]
fn quarantine_rebuild_from_dir_restores_quarantine() {
    let tmp = tempfile::tempdir().unwrap();
    let ts = 1_700_000_000_123u64;
    std::fs::write(tmp.path().join(format!("seg10.quarantined.{ts}")), b"").unwrap();

    let reg = QuarantineRegistry::new();
    reg.rebuild_from_dir(QuarantineEngine::Columnar, tmp.path(), &|fname| {
        let stem = fname.split(".quarantined.").next()?;
        Some(("mydb".to_string(), stem.to_string()))
    });

    let k = SegmentKey {
        engine: QuarantineEngine::Columnar,
        collection: "mydb".into(),
        segment_id: "seg10".into(),
    };
    let err = reg.record_failure(k, "crc", None).unwrap_err();
    assert!(
        matches!(
            err,
            nodedb::storage::quarantine::QuarantineError::SegmentQuarantined {
                quarantined_at_unix_ms,
                ..
            } if quarantined_at_unix_ms == ts
        ),
        "{err}"
    );
}

// ── Columnar engine: corrupt CRC → quarantine ────────────────────────────────

#[test]
fn quarantine_columnar_corrupt_segment_two_strike_quarantine() {
    use nodedb::storage::quarantine::engines::open_segment_with_quarantine;

    // Empty bytes → TruncatedSegment (a CRC-class error), easiest way to
    // trigger the quarantine path without building a full columnar segment.
    let reg = Arc::new(QuarantineRegistry::new());

    assert!(
        matches!(
            open_segment_with_quarantine(&reg, &[], "coll", "seg1"),
            Err(ColumnarOrQuarantine::Columnar(_))
        ),
        "first strike must return ColumnarError"
    );

    assert!(
        matches!(
            open_segment_with_quarantine(&reg, &[], "coll", "seg1"),
            Err(ColumnarOrQuarantine::Quarantined(_))
        ),
        "second strike must return SegmentQuarantined"
    );

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].engine, "columnar");
    assert_eq!(snap[0].collection, "coll");
}

#[test]
fn quarantine_columnar_other_segments_still_readable_after_one_quarantine() {
    use nodedb::storage::quarantine::engines::open_segment_with_quarantine;

    let reg = Arc::new(QuarantineRegistry::new());

    // Quarantine "seg1".
    assert!(matches!(
        open_segment_with_quarantine(&reg, &[], "coll", "seg1"),
        Err(ColumnarOrQuarantine::Columnar(_))
    ));
    assert!(matches!(
        open_segment_with_quarantine(&reg, &[], "coll", "seg1"),
        Err(ColumnarOrQuarantine::Quarantined(_))
    ));

    // "seg2" with the same bad bytes is still at first strike (different segment_id).
    assert!(
        matches!(
            open_segment_with_quarantine(&reg, &[], "coll", "seg2"),
            Err(ColumnarOrQuarantine::Columnar(_))
        ),
        "seg2 must be first-strike ColumnarError, not quarantined"
    );
}

// ── FTS engine: corrupt CRC → quarantine ─────────────────────────────────────

#[test]
fn quarantine_fts_corrupt_segment_two_strike_quarantine() {
    use nodedb::storage::quarantine::engines::open_fts_segment_with_quarantine;

    let reg = Arc::new(QuarantineRegistry::new());

    // Empty → Truncated.
    assert!(matches!(
        open_fts_segment_with_quarantine(&reg, vec![], "ftscoll", "s1"),
        Err(FtsOrQuarantine::Segment(_))
    ));

    assert!(
        matches!(
            open_fts_segment_with_quarantine(&reg, vec![], "ftscoll", "s1"),
            Err(FtsOrQuarantine::Quarantined(_))
        ),
        "second strike must return Quarantined"
    );

    assert_eq!(reg.quarantined_snapshot().len(), 1);
}

// ── Vector engine: corrupt CRC on file → quarantine + rename ────────────────

#[test]
fn quarantine_vector_corrupt_crc_quarantines_and_renames_file() {
    use nodedb::storage::quarantine::engines::open_vector_segment_with_quarantine;
    use nodedb_vector::mmap_segment::{MmapVectorSegment, VectorSegmentDropPolicy};

    let tmp = tempfile::tempdir().unwrap();
    let good_path = tmp.path().join("good.seg");
    let corrupt_path = tmp.path().join("corrupt.seg");

    let v = vec![1.0f32, 2.0, 3.0];
    MmapVectorSegment::create(&good_path, 3, &[&v]).unwrap();

    // Corrupt the CRC in the footer (FOOTER_SIZE=46, CRC at bytes [34..38] from footer start).
    let mut bytes = std::fs::read(&good_path).unwrap();
    let footer_start = bytes.len() - 46;
    bytes[footer_start + 34] ^= 0xFF;
    std::fs::write(&corrupt_path, &bytes).unwrap();

    let reg = Arc::new(QuarantineRegistry::new());
    let policy = VectorSegmentDropPolicy::keep_resident();

    assert!(
        matches!(
            open_vector_segment_with_quarantine(&reg, &corrupt_path, policy, "vecdb", "s1"),
            Err(VectorOrQuarantine::Io(_))
        ),
        "first strike must be IoError"
    );

    assert!(
        matches!(
            open_vector_segment_with_quarantine(&reg, &corrupt_path, policy, "vecdb", "s1"),
            Err(VectorOrQuarantine::Quarantined(_))
        ),
        "second strike must be Quarantined"
    );

    // File must have been renamed.
    let renamed_count = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".quarantined."))
        .count();
    assert_eq!(renamed_count, 1, "exactly one .quarantined.* file expected");

    // Good segment still readable (is_ok checked by verifying no err).
    assert!(
        open_vector_segment_with_quarantine(&reg, &good_path, policy, "vecdb", "s2").is_ok(),
        "good segment must remain readable"
    );
}

// ── HTTP snapshot surfaces ────────────────────────────────────────────────────

#[test]
fn quarantine_http_snapshot_empty_before_any_quarantine() {
    let reg = QuarantineRegistry::new();
    assert!(reg.quarantined_snapshot().is_empty());
}

#[test]
fn quarantine_http_snapshot_returns_entry_after_second_strike() {
    let reg = QuarantineRegistry::new();
    let k = SegmentKey {
        engine: QuarantineEngine::Columnar,
        collection: "httpcoll".into(),
        segment_id: "seg99".into(),
    };
    reg.record_failure(k.clone(), "crc", None).unwrap();
    reg.record_failure(k, "crc", None).unwrap_err();

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].engine, "columnar");
    assert_eq!(snap[0].collection, "httpcoll");
    assert_eq!(snap[0].segment_id, "seg99");
    assert_eq!(snap[0].strikes, 2);
}

// ── Prometheus metrics surface ────────────────────────────────────────────────

#[test]
fn quarantine_metrics_active_counts_reflect_state() {
    let reg = QuarantineRegistry::new();

    let k1 = SegmentKey {
        engine: QuarantineEngine::Columnar,
        collection: "mc".into(),
        segment_id: "s1".into(),
    };
    let k2 = SegmentKey {
        engine: QuarantineEngine::Fts,
        collection: "mc".into(),
        segment_id: "s1".into(),
    };
    // Quarantine both.
    reg.record_failure(k1.clone(), "crc", None).unwrap();
    reg.record_failure(k1, "crc", None).unwrap_err();
    reg.record_failure(k2.clone(), "crc", None).unwrap();
    reg.record_failure(k2, "crc", None).unwrap_err();

    let active = reg.active_counts();
    assert_eq!(
        *active.get(&("columnar".into(), "mc".into())).unwrap_or(&0),
        1
    );
    assert_eq!(*active.get(&("fts".into(), "mc".into())).unwrap_or(&0), 1);

    let mut out = String::new();
    nodedb::control::metrics::SystemMetrics::prometheus_segment_quarantine_active(
        &mut out, &active,
    );
    assert!(out.contains("nodedb_segments_quarantined_active"), "{out}");
    assert!(out.contains(r#"engine="columnar""#), "{out}");
    assert!(out.contains("nodedb_segments_quarantined_total"), "{out}");
}
