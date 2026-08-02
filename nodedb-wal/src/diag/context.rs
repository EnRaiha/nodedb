// SPDX-License-Identifier: Apache-2.0

//! Forensic payloads carried by WAL reports.
//!
//! Each type answers the questions someone debugging from the report alone —
//! with no reproduction and no access to the machine — has to answer: which
//! file, where in it, which LSNs are implicated, and how much of the log the
//! damage hides.
//!
//! Grouping keys deliberately carry no offset, LSN, or segment file name. Those
//! identify the *occurrence*; reports group by the *bug*, so a supervised
//! restart loop re-detecting one damaged segment files one report with a
//! counter rather than one directory per attempt.

use std::path::Path;

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// Round `n` down to a power of two, so a magnitude can enter a grouping key
/// without the exact value doing so.
fn magnitude_bucket(n: u64) -> u64 {
    if n == 0 {
        0
    } else {
        1u64 << (u64::BITS - 1 - n.leading_zeros())
    }
}

/// The segment's own file name, which locates it inside the WAL directory
/// without the leading path an operator's report would only have to redact.
fn file_name(path: &Path) -> Option<&str> {
    path.file_name().and_then(|n| n.to_str())
}

/// The stable class of a failure detail: the text before the first colon,
/// which every constructed detail uses to name what failed rather than which
/// errno it failed with.
fn detail_class(detail: &str) -> &str {
    detail.split(':').next().unwrap_or(detail).trim()
}

/// A WAL segment damaged in the middle, with committed records behind the hole.
pub(super) struct MidFileCorruption<'a> {
    pub path: &'a Path,
    pub offset: u64,
    pub resync_offset: u64,
    pub resync_lsn: u64,
    pub last_lsn: u64,
    pub file_len: Option<u64>,
}

impl DomainContext for MidFileCorruption<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb_wal.mid_file_corruption"
    }

    fn grouping_key(&self) -> String {
        // The size of the hole is what separates the causes: a span the size of
        // one record points at a bad record write, a span the size of a device
        // block or larger points at media damage or an overwrite by something
        // else. The offsets and LSNs themselves differ on every occurrence and
        // would split one bug into one group per crash.
        format!(
            "damaged_bytes~{}",
            magnitude_bucket(self.resync_offset.saturating_sub(self.offset))
        )
    }

    fn to_json(&self) -> Value {
        json!({
            "segment_path": self.path.display().to_string(),
            "segment_file": file_name(self.path),
            "segment_bytes": self.file_len,
            "damage_offset": self.offset,
            "resync_offset": self.resync_offset,
            "damaged_bytes": self.resync_offset.saturating_sub(self.offset),
            "bytes_behind_damage": self.file_len.map(|len| len.saturating_sub(self.offset)),
            "last_lsn_before_damage": self.last_lsn,
            "resync_lsn": self.resync_lsn,
            "lsns_hidden_by_damage": self
                .resync_lsn
                .saturating_sub(self.last_lsn)
                .saturating_sub(1),
            "why_fatal": "an intact record with a higher LSN follows the damage, so the \
                          stop point is a hole and not the unfsynced tail of the last write; \
                          truncating there would discard acknowledged records",
        })
    }
}

/// A whole segment missing from the middle of the log.
pub(super) struct SegmentLsnGap<'a> {
    pub path: &'a Path,
    pub previous_path: &'a Path,
    pub previous_last_lsn: u64,
    pub expected_lsn: u64,
    pub found_lsn: u64,
}

impl DomainContext for SegmentLsnGap<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb_wal.segment_lsn_gap"
    }

    fn grouping_key(&self) -> String {
        // How much of the log went missing distinguishes one deleted segment
        // from a swept directory; the LSNs themselves are per-occurrence.
        format!(
            "missing_lsns~{}",
            magnitude_bucket(self.found_lsn.saturating_sub(self.expected_lsn))
        )
    }

    fn to_json(&self) -> Value {
        json!({
            "segment_path": self.path.display().to_string(),
            "segment_file": file_name(self.path),
            "previous_segment_path": self.previous_path.display().to_string(),
            "previous_segment_file": file_name(self.previous_path),
            "previous_last_lsn": self.previous_last_lsn,
            "expected_first_lsn": self.expected_lsn,
            "found_first_lsn": self.found_lsn,
            "missing_lsns": self.found_lsn.saturating_sub(self.expected_lsn),
            "why_fatal": "segments are written in strictly increasing LSN order, so a later \
                          segment starting above the previous one's successor means at least \
                          one whole segment file is gone from the middle of the log",
        })
    }
}

/// A replay whose requested suffix starts below what the WAL still retains.
pub(super) struct ReplayBelowRetainedFloor<'a> {
    pub path: &'a Path,
    pub from_lsn: u64,
    pub retained_floor_lsn: u64,
}

impl DomainContext for ReplayBelowRetainedFloor<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb_wal.replay_below_retained_floor"
    }

    fn grouping_key(&self) -> String {
        // How far past the floor the request reached separates a watermark that
        // slipped by one checkpoint from one that was lost entirely. The LSNs
        // and the surviving segment's name are per-occurrence.
        format!(
            "missing_lsns~{}",
            magnitude_bucket(self.retained_floor_lsn.saturating_sub(self.from_lsn))
        )
    }

    fn to_json(&self) -> Value {
        json!({
            "requested_from_lsn": self.from_lsn,
            "retained_floor_lsn": self.retained_floor_lsn,
            "missing_lsns": self.retained_floor_lsn.saturating_sub(self.from_lsn),
            "earliest_segment_path": self.path.display().to_string(),
            "earliest_segment_file": file_name(self.path),
            "why_fatal": "the requested suffix was truncated, so filtering the surviving \
                          records by LSN would return a shorter suffix that looks exactly like \
                          a complete one; a consumer would advance its watermark past records \
                          it never received and lose every effect keyed on them",
            "operator_action": "checkpoint truncation is expected to hold at or below every \
                                consumer's persisted watermark — a request below the floor means \
                                that hold was not applied or a consumer watermark was lost or \
                                reset; the truncated records cannot be recovered from this WAL",
        })
    }
}

/// A writer that a failed fsync put into its terminal state.
pub(super) struct DurabilityLost<'a> {
    pub detail: &'a str,
}

impl DomainContext for DurabilityLost<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb_wal.durability_lost"
    }

    fn grouping_key(&self) -> String {
        // The failing operation, not the errno text the kernel rendered.
        format!("cause={}", detail_class(self.detail))
    }

    fn to_json(&self) -> Value {
        json!({
            "detail": self.detail,
            "why_terminal": "Linux reports a writeback error exactly once and drops the dirty \
                             pages that failed, so the bytes already handed to the page cache \
                             are gone and no retry can put them back; the writer refuses all \
                             further work rather than let a later sync acknowledge records that \
                             no longer exist",
            "operator_action": "the segment must be re-opened from what is actually on disk; \
                                check dmesg for the underlying device error",
        })
    }
}

/// A record that still carries `ENCRYPTED_FLAG` where plaintext is required.
pub(super) struct EncryptedRecordWithoutKey {
    pub lsn: u64,
    pub site: &'static str,
}

impl DomainContext for EncryptedRecordWithoutKey {
    fn domain_kind(&self) -> &'static str {
        "nodedb_wal.encrypted_record_without_key"
    }

    fn grouping_key(&self) -> String {
        // The site names the code path that demanded plaintext, which is the
        // thing that is misconfigured or mis-wired. The LSN is the occurrence.
        format!("site={}", self.site)
    }

    fn to_json(&self) -> Value {
        json!({
            "lsn": self.lsn,
            "site": self.site,
            "why_fatal": "passing ciphertext through would hand a downstream decoder bytes it \
                          cannot parse, and skipping the record would silently drop a committed \
                          write; neither is recoverable at the WAL layer",
            "operator_action": "the replay key ring is absent or does not cover this segment's \
                                key epoch — restore the encryption configuration the log was \
                                written under",
        })
    }
}

/// The device filled up under an append.
pub(super) struct OutOfSpace {
    pub site: &'static str,
    pub file_offset: u64,
    pub pending_bytes: u64,
}

impl DomainContext for OutOfSpace {
    fn domain_kind(&self) -> &'static str {
        "nodedb_wal.out_of_space"
    }

    fn grouping_key(&self) -> String {
        format!("site={}", self.site)
    }

    fn to_json(&self) -> Value {
        json!({
            "site": self.site,
            "file_offset": self.file_offset,
            "pending_bytes": self.pending_bytes,
            "why_fatal": "the batch cannot succeed on retry, so the writer stops acknowledging \
                          rather than spin; the buffer and file offset are left untouched so the \
                          same bytes are retried at the same offset once space is freed",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magnitude_buckets_collapse_nearby_sizes() {
        assert_eq!(magnitude_bucket(0), 0);
        assert_eq!(magnitude_bucket(1), 1);
        assert_eq!(magnitude_bucket(4095), 2048);
        assert_eq!(magnitude_bucket(4096), 4096);
        assert_eq!(magnitude_bucket(6000), 4096);
    }

    #[test]
    fn grouping_keys_ignore_the_instance() {
        let path = Path::new("/wal/wal-000000000001.log");
        let near = MidFileCorruption {
            path,
            offset: 4096,
            resync_offset: 8192,
            resync_lsn: 90,
            last_lsn: 12,
            file_len: Some(65536),
        };
        let far = MidFileCorruption {
            path: Path::new("/wal/wal-000000000900.log"),
            offset: 1_048_576,
            resync_offset: 1_052_672,
            resync_lsn: 9000,
            last_lsn: 8000,
            file_len: Some(4_194_304),
        };
        assert_eq!(near.grouping_key(), far.grouping_key());
    }

    #[test]
    fn durability_grouping_drops_the_errno_text() {
        let a = DurabilityLost {
            detail: "fsync failed: Input/output error (os error 5)",
        };
        let b = DurabilityLost {
            detail: "fsync failed: No space left on device (os error 28)",
        };
        assert_eq!(a.grouping_key(), b.grouping_key());
        assert_eq!(a.grouping_key(), "cause=fsync failed");
    }
}
