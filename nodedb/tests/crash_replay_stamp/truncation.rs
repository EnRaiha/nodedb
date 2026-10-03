// SPDX-License-Identifier: BUSL-1.1

//! The WAL-truncation steps of the in-flight run: filler writes that seal A's
//! segment, and the checks that truncation keeps A's segment while A is parked
//! and removes it once A settled.

use std::time::{Duration, Instant};

use crate::crash_harness::log_fields::{boot_section, log_field};
use crate::crash_harness::wal_truncation::{
    WAL_TRUNCATED, segment_first_lsn, truncation_finished_from,
};
use crate::crash_harness::{CrashHarness, diagnostics};

use super::run::CHECKPOINT_DEADLINE;

/// One filler value. Five of them hold 2.5 MiB, which seals the segment that
/// holds A's record under a 1 MiB target.
const FILLER_VALUE_BYTES: usize = 512 * 1024;
const FILLER_ROWS: usize = 5;

/// Filler rows into `filler` that seal the active WAL segment.
pub(super) async fn write_filler(h: &CrashHarness, filler: &str, tag: &str) {
    let value = "x".repeat(FILLER_VALUE_BYTES);
    for i in 0..FILLER_ROWS {
        h.exec(&format!(
            "INSERT INTO {filler} (k, v) VALUES ('{tag}{i}', '{value}')"
        ))
        .await;
    }
}

/// Seal the segment that holds A's record, then wait while A is parked for a
/// checkpoint that ran after the seal and a truncation that removed segments.
/// Returns the name of A's segment.
///
/// A parked after its append and before any B write, and the B writes are too
/// small to fill a segment. So the active segment holds A's record. The
/// filler applies in its own data group, so A's parked entry holds none of it
/// back.
pub(super) async fn truncate_while_held(h: &CrashHarness, filler: &str) -> String {
    let held_segment = h.active_wal_segment();
    write_filler(h, filler, "above").await;
    let active = h.active_wal_segment();
    assert_ne!(
        active, held_segment,
        "the filler did not seal A's segment. Truncation never removes the active \
         segment, so this run proves nothing"
    );

    // Every record in the active segment is above A. A marker at or above its
    // first LSN comes from a checkpoint that ran after the seal.
    let sealed_at = segment_first_lsn(&active);
    let deadline = Instant::now() + CHECKPOINT_DEADLINE;
    loop {
        let log = boot_section(&h.server_log(), 2);
        let ran_after_seal = truncation_finished_from(&log, sealed_at);
        let truncated = !log_field(&log, WAL_TRUNCATED, "segments_deleted").is_empty();
        if ran_after_seal && truncated {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "within {CHECKPOINT_DEADLINE:?} no checkpoint finished truncation after the filler \
             sealed A's segment ({ran_after_seal}), or no truncation removed a segment below A \
             ({truncated}).{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let segments = h.wal_segments();
    assert!(
        segments.contains(&held_segment),
        "a truncation removed {held_segment} while write A in it was in flight: \
         truncation must stay below the lowest checkpoint floor. Segments: {segments:?}"
    );
    held_segment
}

/// After the restart A is applied, so truncation must remove A's segment.
/// A new write lets the Event Plane persist a watermark above it too.
pub(super) async fn truncation_advances_once_settled(
    h: &mut CrashHarness,
    filler: &str,
    segment: &str,
) {
    h.exec(&format!(
        "INSERT INTO {filler} (k, v) VALUES ('settled', 's')"
    ))
    .await;
    let deadline = Instant::now() + CHECKPOINT_DEADLINE;
    while h.wal_segments().iter().any(|name| name == segment) {
        assert!(
            Instant::now() < deadline,
            "truncation never removed {segment} after write A settled: the floor held \
             below a record that has its outcome.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
