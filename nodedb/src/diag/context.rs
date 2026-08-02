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

/// How a terminating ILP connection's already-accepted lines fared.
///
/// A stable class, never a count: it names the shape of the failure so a
/// flapping client collapses into one report instead of one per connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IlpFlushOutcome {
    /// Nothing was buffered, so the termination cost no accepted line.
    NothingBuffered,
    /// The buffered lines were dispatched before the connection closed.
    Recovered,
    /// The final dispatch itself failed; the buffered lines are gone.
    Lost,
}

impl IlpFlushOutcome {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::NothingBuffered => "nothing_buffered",
            Self::Recovered => "recovered",
            Self::Lost => "lost",
        }
    }
}

/// An ILP connection hit a terminal read-side failure while lines it had
/// already accepted were still waiting for their coalescing flush.
pub(super) struct IlpAcceptedLinesDropped<'a> {
    /// Stable cause label — the reason the connection is terminating.
    pub cause: &'static str,
    /// Peer address of the connection being terminated.
    pub peer: &'a str,
    /// Database the connection was authenticated against, which together with
    /// `peer` identifies the ingest stream that lost its tail.
    pub database_id: u64,
    /// Lines accepted into the batch but not yet dispatched when the failure
    /// was detected.
    pub buffered_lines: u64,
    pub outcome: IlpFlushOutcome,
}

impl DomainContext for IlpAcceptedLinesDropped<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.ilp_accepted_lines_dropped"
    }

    fn grouping_key(&self) -> String {
        // Cause and flush outcome name the bug. The peer, the database and the
        // buffered-line count are the occurrence: a misbehaving client
        // reconnecting in a loop would otherwise file one report per
        // connection and drown the recorder in a report storm.
        format!("cause={};outcome={}", self.cause, self.outcome.as_str())
    }

    fn to_json(&self) -> Value {
        json!({
            "cause": self.cause,
            "peer": self.peer,
            "database_id": self.database_id,
            "buffered_lines": self.buffered_lines,
            "outcome": self.outcome.as_str(),
            "why_fatal": "ILP is fire-and-forget: an accepted line is never acked, so a \
                          connection that dies holding a partially filled batch gives its \
                          client no way to learn which lines landed. The lines are flushed \
                          before the connection closes, but the client still lost the rest \
                          of its stream and will keep writing to a socket the server has \
                          already given up on",
            "operator_action": "correlate 'peer' with the client that owns it: 'invalid_utf8' \
                                 means it is framing non-UTF-8 bytes as ILP, 'line_read_failed' \
                                 means the socket broke or the line exceeded the configured \
                                 length cap. When 'outcome' is 'lost' the buffered lines never \
                                 reached the engine and must be re-sent by the client",
        })
    }
}

/// A committed, CRC-valid WAL record could not be applied during startup
/// replay, so the replayed suffix would have had a hole in it.
pub(super) struct ReplayRecordUnapplied<'a> {
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
        // The engine and the failing step name the bug. The LSN and the core
        // are the occurrence: one malformed record class typically fails on
        // every record of that class across every core, and those must collapse
        // into one group rather than one directory per record.
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
