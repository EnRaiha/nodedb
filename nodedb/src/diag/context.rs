// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for capture sites outside the WAL.
//!
//! Grouping keys deliberately carry no per-occurrence value (a raft index, a
//! transaction's epoch/position) — those identify the *occurrence*, and
//! reports group by the *bug*, so a retry loop hitting the same root cause
//! files one report with a growing occurrence count rather than one
//! directory per retry.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A durable host-side effect failed while applying a committed metadata
/// entry, so the Raft applier stopped without advancing its watermark.
pub(super) struct MetadataApplyWedged<'a> {
    pub raft_index: u64,
    pub last_applied_watermark: u64,
    pub entry_kind: &'a str,
    pub error_class: &'a str,
    /// The applier judged this failure deterministic in the entry and the
    /// local state, so re-delivery cannot clear it and the node withdrew from
    /// readiness. `false` means halt-and-retry is still expected to heal.
    pub permanent: bool,
}

impl DomainContext for MetadataApplyWedged<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.metadata_apply_wedged"
    }

    fn grouping_key(&self) -> String {
        // The entry variant and the stable class of the error name the bug;
        // the raft index and watermark are the occurrence — every
        // re-delivery of the same stuck entry carries a different watermark
        // snapshot but the same root cause, and must collapse to one group.
        format!("entry={};cause={}", self.entry_kind, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "raft_index": self.raft_index,
            "last_applied_watermark": self.last_applied_watermark,
            "entry_kind": self.entry_kind,
            "error_class": self.error_class,
            "permanent": self.permanent,
            "why_fatal": "the apply loop never advances the watermark past an entry it \
                          could not durably apply; a deterministic failure re-fails on \
                          every re-delivery, so this node's Raft applier is wedged and \
                          callers only see an unrelated-looking lease timeout, never this. \
                          When 'permanent' is true the node has withdrawn from readiness \
                          instead of pretending a retry will heal it",
            "operator_action": "when 'permanent' is false, look for a clearing condition \
                                 (a full disk, redb contention, a subsystem handle not \
                                 installed yet) — the applier resumes on its own once the \
                                 same entry applies cleanly. When it is true, the entry \
                                 and the local state fully determine the failure: inspect \
                                 this node's catalog against the replicated log for the \
                                 named descriptor, since no retry will change the outcome",
        })
    }
}

/// A Calvin cross-shard transaction's completion wait timed out with no
/// signal for why the transaction never completed.
pub(super) struct CalvinCompletionTimeout {
    pub epoch: u64,
    pub position: u32,
    pub participants: usize,
    pub timeout_secs: u64,
}

impl DomainContext for CalvinCompletionTimeout {
    fn domain_kind(&self) -> &'static str {
        "nodedb.calvin_completion_timeout"
    }

    fn grouping_key(&self) -> String {
        // Coarse and constant: every occurrence of this timeout is the same
        // bug shape — a completion ack never arrived within budget —
        // regardless of which transaction hit it, so epoch/position/
        // participants must not enter the key.
        "completion_timeout".to_owned()
    }

    fn to_json(&self) -> Value {
        json!({
            "epoch": self.epoch,
            "position": self.position,
            "participants": self.participants,
            "timeout_secs": self.timeout_secs,
            "why_fatal": "this timeout is the only signal a Calvin-routed write ever \
                          produces for a completion ack that never arrived; the caller \
                          sees a generic internal error with no indication of which \
                          participant or stage stalled, and the write's outcome is \
                          unknown to the client",
            "operator_action": "check the sequencer-group leader and the listed \
                                 participant shards for a stalled scheduler, a lost \
                                 CompletionAck proposal, or a network partition between \
                                 them",
        })
    }
}
