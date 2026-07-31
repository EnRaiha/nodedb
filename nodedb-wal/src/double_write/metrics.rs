// SPDX-License-Identifier: Apache-2.0

//! Process-wide double-write buffer counters.
//!
//! Torn-write protection can be lost quietly — a DWB that fails to open, a
//! slot write that errors, a record too large to mirror. None of those fail
//! the WAL append, so a one-off log line is the only trace they would
//! otherwise leave. These counters make the degraded state something an
//! operator can alert on.

use std::sync::atomic::{AtomicU64, Ordering};

static DWB_BYTES_WRITTEN_TOTAL: AtomicU64 = AtomicU64::new(0);
static DWB_UNPROTECTED_RECORDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static DWB_DEGRADATIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Total bytes written to DWB files since process start.
///
/// Surfaces the duplicate-write cost of running the DWB alongside an
/// O_DIRECT WAL.
pub fn wal_dwb_bytes_written_total() -> u64 {
    DWB_BYTES_WRITTEN_TOTAL.load(Ordering::Relaxed)
}

/// Records appended to a WAL without a double-write copy behind them.
///
/// Non-zero means torn-write recovery cannot reconstruct those records: if one
/// of them is the last record in its segment and is torn by a power loss, it
/// is dropped as an unfsynced tail.
pub fn wal_dwb_unprotected_records_total() -> u64 {
    DWB_UNPROTECTED_RECORDS_TOTAL.load(Ordering::Relaxed)
}

/// Times a configured double-write buffer stopped protecting a writer.
pub fn wal_dwb_degradations_total() -> u64 {
    DWB_DEGRADATIONS_TOTAL.load(Ordering::Relaxed)
}

pub(crate) fn add_bytes_written(bytes: u64) {
    DWB_BYTES_WRITTEN_TOTAL.fetch_add(bytes, Ordering::Relaxed);
}

pub(crate) fn add_unprotected_record() {
    DWB_UNPROTECTED_RECORDS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn add_degradation() {
    DWB_DEGRADATIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}
