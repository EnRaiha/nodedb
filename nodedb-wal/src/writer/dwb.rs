// SPDX-License-Identifier: Apache-2.0

//! The writer's side of the double-write buffer: mirroring records into it,
//! and keeping the loss of torn-write protection visible when it fails.
//!
//! A DWB problem deliberately does NOT fail the WAL write that provoked it —
//! the WAL append is correct on its own and is still acknowledged, and turning
//! a degraded mode into an outage would be a worse trade. What a DWB problem
//! does cost is recoverability: a record whose CRC fails with no DWB copy
//! behind it is indistinguishable from the end of the committed prefix, so a
//! torn last record in a segment is dropped in silence. That cost is recorded
//! on the writer and in the process counters instead of being logged once and
//! forgotten.

use crate::double_write::{DwbDegradation, DwbMirror, DwbProtection, DwbSkipReason, metrics};
use crate::record::WalRecord;

use super::config::{open_dwb_at, resolve_dwb_mode};
use super::core::WalWriter;

impl WalWriter {
    /// Torn-write protection standing of this writer.
    pub fn dwb_protection(&self) -> DwbProtection {
        self.dwb_protection
    }

    /// Records appended to this segment with no double-write copy behind them.
    ///
    /// Non-zero means those records cannot be reconstructed if they are torn
    /// by a power loss at the tail of the segment.
    pub fn dwb_unprotected_records(&self) -> u64 {
        self.dwb_unprotected_records
    }

    /// Reopen a double-write buffer that was detached after a failure.
    ///
    /// Detaching keeps a broken device from being hammered once per append,
    /// but on its own it makes degradation permanent for the life of the
    /// writer — an operator who has fixed the fault would have no way back
    /// short of rolling the segment. Returns the resulting standing.
    pub fn reattach_double_write(&mut self) -> DwbProtection {
        let Some(dwb_path) = self.dwb_path.clone() else {
            return self.dwb_protection;
        };
        match open_dwb_at(&dwb_path, resolve_dwb_mode(&self.config)) {
            Some(dwb) => {
                self.double_write = Some(dwb);
                self.dwb_protection = DwbProtection::Active;
            }
            None => self.degrade_dwb(DwbDegradation::OpenFailed),
        }
        self.dwb_protection
    }

    /// Mirror a record that is already committed to the WAL write buffer.
    pub(super) fn mirror_into_dwb(&mut self, lsn: u64, record: &WalRecord) {
        let mirrored = self
            .double_write
            .as_mut()
            .map(|dwb| dwb.write_record_deferred(record));
        match mirrored {
            Some(Ok(DwbMirror::Mirrored)) => {}
            Some(Ok(DwbMirror::Skipped(reason))) => self.note_unprotected_record(lsn, reason),
            Some(Err(e)) => {
                tracing::warn!(lsn = lsn, error = %e, "DWB write failed, detaching DWB");
                self.double_write = None;
                self.degrade_dwb(DwbDegradation::WriteFailed);
                self.note_unprotected();
            }
            // No buffer attached: either the DWB is off by configuration
            // (nothing to report) or it already degraded, in which case every
            // record since is unprotected and has to keep being counted.
            None => {
                if self.dwb_protection.is_degraded() {
                    self.note_unprotected();
                }
            }
        }
    }

    /// Make the mirrored batch durable ahead of the WAL's own fsync.
    ///
    /// A failed DWB fsync leaves the batch mirrored only in the page cache, so
    /// its records are unprotected even though they reached slots. The WAL
    /// sync continues regardless — the WAL's own fsync is what decides
    /// durability — and the lost protection is recorded.
    pub(super) fn flush_dwb(&mut self) {
        let flushed = self.double_write.as_mut().map(|dwb| dwb.flush());
        if let Some(Err(e)) = flushed {
            tracing::warn!(error = %e, "DWB flush failed, detaching DWB");
            self.double_write = None;
            self.degrade_dwb(DwbDegradation::FlushFailed);
        }
    }

    /// Account for a record the WAL accepted but the DWB could not mirror.
    fn note_unprotected_record(&mut self, lsn: u64, reason: DwbSkipReason) {
        tracing::warn!(
            lsn = lsn,
            reason = %reason,
            "record appended without torn-write protection"
        );
        self.note_unprotected();
    }

    fn note_unprotected(&mut self) {
        self.dwb_unprotected_records = self.dwb_unprotected_records.saturating_add(1);
        metrics::add_unprotected_record();
    }

    /// Record that protection was requested and has now been lost.
    fn degrade_dwb(&mut self, degradation: DwbDegradation) {
        if !self.dwb_protection.is_degraded() {
            metrics::add_degradation();
        }
        self.dwb_protection = DwbProtection::Degraded(degradation);
    }
}
