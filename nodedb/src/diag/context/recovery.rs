// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for startup-replay capture sites.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A committed, CRC-valid WAL record could not be applied during startup
/// replay, so the replayed suffix would have had a hole in it.
pub(in crate::diag) struct ReplayRecordUnapplied<'a> {
    /// Which engine's replay arm detected it (`kv`, `fts`, `spatial`, ...).
    pub engine: &'a str,
    /// Which step inside that arm failed (`decode`, `handler`, `open`, ...).
    pub stage: &'a str,
    pub core_id: usize,
    pub record_lsn: u64,
    /// Why the step failed, as the detecting site described it.
    pub detail: &'a str,
}

impl DomainContext for ReplayRecordUnapplied<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.replay_record_unapplied"
    }

    fn grouping_key(&self) -> String {
        // Engine + failing step name the bug; LSN/core are the occurrence,
        // collapsing every record of one malformed class into one group.
        format!("engine={};stage={}", self.engine, self.stage)
    }

    fn to_json(&self) -> Value {
        json!({
            "engine": self.engine,
            "stage": self.stage,
            "core_id": self.core_id,
            "record_lsn": self.record_lsn,
            "detail": self.detail,
            "why_fatal": "the record's CRC verified, so its bytes are intact — it is a \
                          transaction that was acknowledged as committed and cannot be \
                          applied. Skipping it would open the database with committed \
                          writes silently missing from the replayed suffix, which no \
                          later read can distinguish from data that was never written",
            "operator_action": "the WAL tail at this LSN is intact but unreadable by this \
                                 build — check for a downgrade past the record shape that \
                                 wrote it, then preserve the WAL directory before any \
                                 further start attempt",
        })
    }
}

/// A WAL segment due for deletion could not be archived to cold storage, so
/// the checkpoint held truncation back at that segment.
pub(in crate::diag) struct WalArchivalFailedTruncationHeld<'a> {
    /// Which archival step failed (`list_segments`, `upload`, `segment_path`).
    pub stage: &'a str,
    /// Stable class of the failure, as the cold-storage layer described it.
    pub error_class: &'a str,
    /// First LSN of the segment the archive is missing.
    pub segment_first_lsn: u64,
}

impl DomainContext for WalArchivalFailedTruncationHeld<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.wal_archival_failed_truncation_held"
    }

    fn grouping_key(&self) -> String {
        // Stage + error class name the fault. Segment and LSN are the
        // occurrence: one unreachable cold store spans many cycles and many
        // segments, and all of them belong in one growing report.
        format!("stage={};class={}", self.stage, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "stage": self.stage,
            "error_class": self.error_class,
            "segment_first_lsn": self.segment_first_lsn,
            "why_fatal": "the archive is the only copy of a WAL segment once truncation \
                          unlinks it, so a segment that fails to upload and is deleted \
                          anyway leaves a permanent hole that no point-in-time recovery \
                          can cross. Truncation stops at this segment instead, which \
                          holds the local WAL on disk until archival recovers",
            "operator_action": "restore the cold-storage endpoint named in this node's \
                                 config — credentials, bucket policy, reachability. The \
                                 local WAL grows until the next checkpoint archives this \
                                 segment, so treat WAL disk usage as the clock",
        })
    }
}
