// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill regressions for WAL truncation against memory-only
//! engines.
//!
//! `crash_recovery.rs` proves a committed write survives `kill -9` while its
//! WAL record still exists. These tests prove the harder half: that the write
//! survives after the periodic checkpoint has DELETED the WAL segment holding
//! its record. For an engine whose state lives only in memory, the WAL was the
//! only durable copy, and the checkpoint reported the core's write watermark as
//! though every engine had flushed to it — so `truncate_before` unlinked sealed
//! segments whose rows nothing else held. Default config, single node, every
//! five minutes.
//!
//! Reproducing that needs three things the existing crash tests do not do, and
//! all three are load-bearing:
//!
//!   1. A checkpoint cycle inside the test's lifetime. The default 300s
//!      interval dwarfs a crash test's runtime, so the truncation window never
//!      opened and the bug sat under a green suite until these tests forced a
//!      short interval.
//!   2. WAL segment ROTATION. `truncate_segments` skips the active segment
//!      unconditionally, so with a single segment nothing is ever deleted and
//!      this test would pass against the buggy code having proven nothing.
//!      Hence the filler writes: they push the canary's segment out of the
//!      active slot and into the sealed, deletable set.
//!   3. An assertion that a sealed segment was actually unlinked. Without it a
//!      pass cannot be told apart from "truncation never ran", which is the
//!      failure mode that hid the bug in the first place.
//!
//! The row read after the restart therefore cannot have come from the WAL. It
//! can only have come from the engine's own checkpoint, which is exactly the
//! claim under test.

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::{Duration, Instant};

/// Checkpoint cycle short enough to fire several times inside one test.
const CHECKPOINT_INTERVAL_SECS: &str = "2";

/// Smallest WAL segment target the config accepts (`wal_segment_target_mb` is
/// whole MiB), so the filler below only has to write a little over 1 MiB per
/// rotation.
const WAL_SEGMENT_TARGET_MB: &str = "1";

/// Filler payload per row — deliberately large so the whole segment-sealing
/// filler is a handful of writes, not dozens. Each filler INSERT is its own
/// WAL fsync round-trip against a spawned (unoptimized) server, so a few big
/// rows are dramatically faster than many small ones on a slow CI disk while
/// sealing the same number of segments. Stays under the 1 MiB segment target
/// (so one record fits in a segment) and far under the 64 MiB WAL record cap.
const FILLER_VALUE_BYTES: usize = 512 * 1024;

/// ~2.5 MiB of filler over a 1 MiB segment target: enough to seal at least two
/// segments, so the canary's segment is sealed and strictly below the active
/// one no matter where the boot records happened to land.
const FILLER_ROWS: usize = 5;

/// How long to wait for the checkpoint to unlink the canary's segment. Several
/// times the checkpoint interval, but bounded: a timeout here means truncation
/// never ran, and that must FAIL the test rather than let it pass vacuously.
const TRUNCATION_TIMEOUT: Duration = Duration::from_secs(45);

fn tuned_harness() -> CrashHarness {
    CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", CHECKPOINT_INTERVAL_SECS)
        .with_env("NODEDB_WAL_SEGMENT_TARGET_MB", WAL_SEGMENT_TARGET_MB)
}

fn filler_value() -> String {
    "x".repeat(FILLER_VALUE_BYTES)
}

/// The segment the write that just returned landed in.
///
/// The highest-numbered segment is the active one, and the WAL appends to the
/// active segment, so the record of the last acknowledged write is in it. Names
/// are `wal-<20-digit first LSN>.seg`, zero-padded, so lexicographic order is
/// LSN order and `last()` is the active segment.
///
/// Taking this snapshot BEFORE writing the filler is what makes the later
/// assertion exact rather than approximate. At this instant the segment cannot
/// have been truncated — it is active, and `truncate_segments` skips the active
/// segment — so it is unambiguously the canary's, and its later disappearance
/// is unambiguously the truncation of the canary's own WAL record.
fn active_segment(h: &CrashHarness) -> String {
    let segments = h.wal_segments();
    segments
        .last()
        .unwrap_or_else(|| panic!("no WAL segments on disk after an acknowledged write"))
        .clone()
}

/// Block until `segment` has been unlinked, panicking if it never is.
///
/// This is the assertion the whole test exists for: it is the only thing that
/// distinguishes "the engine's checkpoint restored the row" from "the WAL
/// record was still there all along".
fn await_segment_deleted(h: &CrashHarness, segment: &str) {
    let deadline = Instant::now() + TRUNCATION_TIMEOUT;
    loop {
        let live = h.wal_segments();
        if !live.iter().any(|s| s == segment) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "WAL segment {segment} still exists after {TRUNCATION_TIMEOUT:?} — the checkpoint \
             never truncated it, so this test proves NOTHING about surviving truncation. \
             Segments on disk: {live:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// KV is the flagship case: `KvEngine` is a plain in-memory `HashMap` with no
/// redb store behind it, so before it had a checkpoint the WAL held the only
/// copy of every KV row — and KV writes advanced the very watermark that
/// authorised deleting it.
#[tokio::test(flavor = "multi_thread")]
async fn kv_row_survives_wal_segment_truncation() {
    let mut h = tuned_harness();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec("CREATE COLLECTION trunc_kv (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await;
    h.exec("INSERT INTO trunc_kv (k, v) VALUES ('canary', 'survives')")
        .await;

    // The segment holding the canary's WAL record, captured while it is still
    // the active one and therefore provably not yet truncated.
    let canary_segment = active_segment(&h);

    // Live sanity BEFORE anything else: the row reads back now, so a failure
    // after the restart is attributable to recovery and not to test setup.
    let live = h
        .query_col("SELECT v FROM trunc_kv WHERE k = 'canary'", "v")
        .await;
    assert_eq!(
        live,
        vec!["survives".to_string()],
        "KV row must read back BEFORE the crash (test-setup sanity): {live:?}"
    );

    // Force rotation: seal the canary's segment by pushing past the 1 MiB
    // target several times over. Until this happens the canary's segment is the
    // active one and truncation skips it unconditionally.
    let filler = filler_value();
    for i in 0..FILLER_ROWS {
        h.exec(&format!(
            "INSERT INTO trunc_kv (k, v) VALUES ('filler{i}', '{filler}')"
        ))
        .await;
    }
    assert_ne!(
        active_segment(&h),
        canary_segment,
        "filler writes did not rotate the WAL — the canary's segment is still active and \
         truncation would skip it, making this test vacuous"
    );

    // Wait for a checkpoint cycle to actually delete it.
    await_segment_deleted(&h, &canary_segment);

    h.kill_9();
    h.reopen();

    // The canary's WAL record is gone from disk. If the row comes back, it came
    // from the KV checkpoint — the only other copy that can exist.
    let recovered = h
        .query_col("SELECT v FROM trunc_kv WHERE k = 'canary'", "v")
        .await;
    assert_eq!(
        recovered,
        vec!["survives".to_string()],
        "KV row was LOST: its WAL segment was truncated by the checkpoint and the KV engine \
         had no durable copy of its own (got {recovered:?})"
    );
}

/// Columnar is memory-only on both halves — the live memtable in
/// `columnar_engines` and the encoded bytes of already-flushed segments in
/// `columnar_flushed_segments`, neither of which had a store behind it — so it
/// faces the same truncation the KV engine does.
#[tokio::test(flavor = "multi_thread")]
async fn columnar_row_survives_wal_segment_truncation() {
    let mut h = tuned_harness();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION trunc_columnar \
         COLUMNS (id TEXT, region TEXT, payload TEXT) \
         WITH (engine='columnar')",
    )
    .await;
    h.exec("INSERT INTO trunc_columnar (id, region, payload) VALUES ('canary', 'us', 'small')")
        .await;

    let canary_segment = active_segment(&h);

    let live = h
        .query_col(
            "SELECT region FROM trunc_columnar WHERE id = 'canary'",
            "region",
        )
        .await;
    assert_eq!(
        live,
        vec!["us".to_string()],
        "columnar row must read back BEFORE the crash (test-setup sanity): {live:?}"
    );

    let filler = filler_value();
    for i in 0..FILLER_ROWS {
        h.exec(&format!(
            "INSERT INTO trunc_columnar (id, region, payload) VALUES ('filler{i}', 'eu', '{filler}')"
        ))
        .await;
    }
    assert_ne!(
        active_segment(&h),
        canary_segment,
        "filler writes did not rotate the WAL — the canary's segment is still active and \
         truncation would skip it, making this test vacuous"
    );

    await_segment_deleted(&h, &canary_segment);

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "SELECT region FROM trunc_columnar WHERE id = 'canary'",
            "region",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["us".to_string()],
        "columnar row was LOST: its WAL segment was truncated by the checkpoint and the \
         columnar engine had no durable copy of its own (got {recovered:?})"
    );
}
