// SPDX-License-Identifier: BUSL-1.1

//! Hash-chain verification over the whole audit log.
//!
//! Each entry's `prev_hash` must equal the hash of the entry before it.
//! Retention pruning removes the oldest entries, so the oldest surviving entry
//! links to a deleted one. Each prune records an `AuditCheckpoint` entry first.
//! Its detail names the first surviving seq and the hash of the last deleted entry.
//! The checkpoint is itself a normal chain link, so its detail is tamper-evident.

use std::collections::HashMap;

use super::entry::{AuditEntry, hash_entry};
use super::event::AuditEvent;

/// Detail token naming the first seq that survives a prune.
const OLDEST_KEPT_SEQ: &str = "oldest_kept_seq=";
/// Detail token naming the hash of the last entry a prune deletes.
const ANCHOR_HASH: &str = "anchor_hash=";

/// Build the detail of an `AuditCheckpoint` entry for one prune.
pub fn checkpoint_detail(pruned_count: u64, oldest_kept_seq: u64, anchor_hash: &str) -> String {
    format!(
        "pruned_count={pruned_count} {OLDEST_KEPT_SEQ}{oldest_kept_seq} {ANCHOR_HASH}{anchor_hash}"
    )
}

/// The `(oldest_kept_seq, anchor_hash)` pair a checkpoint entry records.
fn checkpoint_anchor(entry: &AuditEntry) -> Option<(u64, &str)> {
    if entry.event != AuditEvent::AuditCheckpoint {
        return None;
    }
    let mut seq = None;
    let mut hash = None;
    for token in entry.detail.split_whitespace() {
        if let Some(value) = token.strip_prefix(OLDEST_KEPT_SEQ) {
            seq = value.parse::<u64>().ok();
        } else if let Some(value) = token.strip_prefix(ANCHOR_HASH) {
            hash = Some(value);
        }
    }
    Some((seq?, hash?))
}

/// Outcome of one chain verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainReport {
    /// Entries in the requested seq range whose link passed.
    pub checked: u64,
    /// First seq in the range whose link failed.
    ///
    /// A link fails when an entry's `prev_hash` differs from its predecessor's hash.
    /// So an altered row is this seq or the seq before it.
    /// For the newest entry, a mismatch with the live tail hash also fails.
    pub broken_at: Option<u64>,
    /// Hash of the last entry that passed, or empty when none passed.
    pub last_hash: String,
}

/// Verify the chain over `entries` and report on the seqs in `from_seq..=to_seq`.
///
/// `entries` must be the whole log in ascending seq order with one entry per seq.
/// Entries before `from_seq` still feed the link of the first entry in range.
/// `tail_hash` is the live log's last hash.
/// It covers the newest entry, which has no successor to detect an edit to it.
pub fn verify_chain(
    entries: &[AuditEntry],
    from_seq: u64,
    to_seq: u64,
    tail_hash: &str,
) -> ChainReport {
    let anchors: HashMap<u64, &str> = entries.iter().filter_map(checkpoint_anchor).collect();
    let mut report = ChainReport {
        checked: 0,
        broken_at: None,
        last_hash: String::new(),
    };
    let mut expected: Option<String> = None;

    for entry in entries {
        if entry.seq > to_seq {
            return report;
        }
        let linked = match &expected {
            Some(prev) => entry.prev_hash == *prev,
            // The oldest entry is either the genesis entry or a pruned head with a recorded anchor.
            None => {
                (entry.seq == 1 && entry.prev_hash.is_empty())
                    || anchors.get(&entry.seq) == Some(&entry.prev_hash.as_str())
            }
        };
        let hash = hash_entry(entry);
        if entry.seq >= from_seq {
            if !linked {
                report.broken_at = Some(entry.seq);
                return report;
            }
            report.checked += 1;
            report.last_hash.clone_from(&hash);
        }
        expected = Some(hash);
    }

    if let Some(newest) = entries.last()
        && newest.seq >= from_seq
        && report.last_hash != tail_hash
    {
        report.broken_at = Some(newest.seq);
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::AuditLog;

    fn chain_of(details: &[&str]) -> (Vec<AuditEntry>, String) {
        let mut log = AuditLog::new(1_000);
        for detail in details {
            log.record(AuditEvent::AdminAction, None, "src", detail);
        }
        (
            log.all().iter().cloned().collect(),
            log.last_hash().to_string(),
        )
    }

    #[test]
    fn intact_chain_is_valid() {
        let (entries, tail) = chain_of(&["a", "b", "c"]);
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, None);
        assert_eq!(report.checked, 3);
        assert_eq!(report.last_hash, tail);
    }

    #[test]
    fn altered_middle_row_breaks_at_its_successor() {
        let (mut entries, tail) = chain_of(&["a", "b", "c", "d"]);
        entries[1].detail = "tampered".to_string();
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(3));
    }

    #[test]
    fn altered_prev_hash_breaks_at_that_row() {
        let (mut entries, tail) = chain_of(&["a", "b", "c"]);
        entries[1].prev_hash = "0".repeat(64);
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(2));
    }

    #[test]
    fn altered_newest_row_breaks_against_the_tail_hash() {
        let (mut entries, tail) = chain_of(&["a", "b", "c"]);
        entries[2].detail = "tampered".to_string();
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(3));
    }

    #[test]
    fn deleted_row_breaks_at_the_next_row() {
        let (mut entries, tail) = chain_of(&["a", "b", "c", "d"]);
        entries.remove(1);
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(3));
    }

    #[test]
    fn unanchored_truncated_head_is_broken() {
        let (entries, tail) = chain_of(&["a", "b", "c"]);
        let report = verify_chain(&entries[1..], 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(2));
    }

    #[test]
    fn pruned_head_with_checkpoint_is_valid() {
        let mut log = AuditLog::new(1_000);
        log.record(AuditEvent::AdminAction, None, "src", "a");
        log.record(AuditEvent::AdminAction, None, "src", "b");
        log.record(AuditEvent::AdminAction, None, "src", "c");
        let anchor = hash_entry(&log.all()[1]);
        log.record(
            AuditEvent::AuditCheckpoint,
            None,
            "retention",
            &checkpoint_detail(2, 3, &anchor),
        );
        log.record(AuditEvent::AdminAction, None, "src", "d");
        let survivors: Vec<AuditEntry> = log.all().iter().skip(2).cloned().collect();
        let report = verify_chain(&survivors, 1, u64::MAX, log.last_hash());
        assert_eq!(report.broken_at, None);
        assert_eq!(report.checked, 3);
    }

    #[test]
    fn range_uses_earlier_entries_for_its_first_link() {
        let (entries, tail) = chain_of(&["a", "b", "c", "d"]);
        let report = verify_chain(&entries, 3, 3, &tail);
        assert_eq!(report.broken_at, None);
        assert_eq!(report.checked, 1);
    }
}
