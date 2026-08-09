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

/// A write whose redo record the Control-Plane funnel was supposed to mint
/// reached the client acknowledgement with no durable LSN to wait on.
pub(super) struct WriteAckedWithoutDurability {
    /// Engine whose every write-class op is expected to mint a WAL redo on
    /// this path (`kv`, `vector`, `graph`, ...).
    pub engine: &'static str,
}

impl DomainContext for WriteAckedWithoutDurability {
    fn domain_kind(&self) -> &'static str {
        "nodedb.write_acked_without_durability"
    }

    fn grouping_key(&self) -> String {
        // The engine names the bug: a missing redo arm is a property of the
        // engine's WAL-append classifier, not of the row that happened to hit
        // it. Anything finer would file one report per acknowledged write.
        format!("engine={}", self.engine)
    }

    fn to_json(&self) -> Value {
        json!({
            "engine": self.engine,
            "why_fatal": "the funnel appended this write's redo itself, so a missing LSN \
                          means no record was minted at all — the durable-at-ack barrier \
                          is skipped and the client is told the write committed. This \
                          engine's state survives a restart only by WAL replay, so a \
                          'kill -9' after the ack loses an acknowledged write with no \
                          error anywhere",
            "operator_action": "inspect the named engine's arm in the Control-Plane WAL \
                                 append dispatch: a write-class op filed under the \
                                 'no durable record' group mints nothing. Either give it \
                                 a redo record or move the engine out of the set the \
                                 barrier holds to this invariant",
        })
    }
}

/// A document write was rejected because its full-text index update failed.
///
/// The row and the index share one write transaction, so the rejection is
/// clean — neither half is durable. What the report captures is that a
/// collection's inverted index is refusing writes at all, which no query will
/// ever surface: clients see failing writes, not a diagnosis.
pub(super) struct FtsIndexUpdateFailed<'a> {
    /// Collection whose inverted index rejected the document's terms.
    pub collection: &'a str,
    /// Global surrogate identity of the document that failed to index.
    pub surrogate: u32,
    /// Stable class of the failure, as the index layer described it.
    pub error_class: &'a str,
}

impl DomainContext for FtsIndexUpdateFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.fts_index_update_failed"
    }

    fn grouping_key(&self) -> String {
        // The collection and the stable class of the error name the bug. The
        // surrogate is the occurrence: a bulk load hitting the same index
        // failure would otherwise file one report per row and bury the single
        // fact that matters — this collection's index is refusing writes.
        format!("collection={};cause={}", self.collection, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "collection": self.collection,
            "surrogate": self.surrogate,
            "error_class": self.error_class,
            "why_fatal": "the inverted index shares the row's write transaction, so the \
                          failure aborts the whole write and the client is told the write \
                          did not happen — nothing is silently half-applied. It is filed \
                          anyway because a structural cause makes EVERY write to this \
                          collection fail from here on, and the only symptom the operator \
                          sees is writes being refused with no indication that the index \
                          is what refused them",
            "operator_action": "read the error class: a transient cause (redb contention, \
                                 a full disk) clears once the resource does, while a \
                                 structural one (a corrupt or type-mismatched FTS table) \
                                 will re-fail on every write until the collection's index \
                                 is rebuilt",
        })
    }
}

/// A document batch insert arrived without a surrogate for every row, so the
/// rows it carries have no cross-engine identity to be indexed under.
///
/// Every index in the system — FTS, vector, spatial, and the secondary btree
/// — is keyed by a row's global surrogate. A batch whose surrogate list is not
/// parallel to its document list therefore cannot be indexed at all, and
/// storing it would put rows in the collection that no index can ever return.
/// The plan is rejected instead; the report exists because the malformation is
/// upstream (a plan builder or a short WAL record), and the rejection alone
/// says nothing about where it came from.
pub(super) struct BatchInsertWithoutSurrogates<'a> {
    /// Collection the malformed batch targeted.
    pub collection: &'a str,
    /// Rows the batch carried.
    pub document_count: usize,
    /// Surrogates it carried for them.
    pub surrogate_count: usize,
}

impl DomainContext for BatchInsertWithoutSurrogates<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.batch_insert_without_surrogates"
    }

    fn grouping_key(&self) -> String {
        // The collection names the bug — one producer emitting malformed
        // batches. The two counts are the occurrence: a client retrying the
        // same malformed batch, or a replay walking many short records, would
        // otherwise file a report per distinct batch size and bury the single
        // fact that matters.
        format!("collection={}", self.collection)
    }

    fn to_json(&self) -> Value {
        json!({
            "collection": self.collection,
            "document_count": self.document_count,
            "surrogate_count": self.surrogate_count,
            "why_fatal": "the batch is refused outright, so nothing is written and the \
                          client is told the insert did not happen. It is filed anyway \
                          because the defect is in whatever produced the plan, and that \
                          producer is invisible from the rejection: the alternative — \
                          storing the rows unindexed and reporting success — would leave \
                          rows that full-text, vector, spatial, and secondary-index \
                          lookups all silently omit",
            "operator_action": "identify the producer: a native batch-insert builder \
                                 assigns one surrogate per document, so a mismatch points \
                                 either at a client path that bypassed assignment or at a \
                                 truncated replicated write record",
        })
    }
}

/// What had already happened to the write whose response the Data Plane could
/// not deliver.
///
/// A stable class, never a request id: it names the shape of the failure so a
/// saturated response ring collapses into one report instead of one per lost
/// response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostResponseWrite {
    /// The batch transaction committed before the response was lost, so the
    /// write is durable while the caller will only ever see a deadline.
    Committed,
    /// The batch transaction was rolled back, so nothing was applied and the
    /// caller's deadline matches reality — only the answer itself is gone.
    RolledBack,
}

impl LostResponseWrite {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
        }
    }
}

/// A Data-Plane core finished a write but could not hand its response back to
/// the Control Plane, because the bounded response ring refused the push.
pub(super) struct DataPlaneResponseLost {
    /// Core whose response ring refused the push.
    pub core_id: usize,
    pub write: LostResponseWrite,
}

impl DomainContext for DataPlaneResponseLost {
    fn domain_kind(&self) -> &'static str {
        "nodedb.data_plane_response_lost"
    }

    fn grouping_key(&self) -> String {
        // The write's fate names the bug — an ambiguous committed write is a
        // different defect from a merely lost error answer. The core id is the
        // occurrence: a saturated ring drops on whichever core is unlucky, and
        // keying on it would file one report per core for one root cause.
        format!("write={}", self.write.as_str())
    }

    fn to_json(&self) -> Value {
        json!({
            "core_id": self.core_id,
            "write": self.write.as_str(),
            "why_fatal": "the response ring is the only channel a Data-Plane core has to \
                          report an outcome. A dropped response leaves the caller waiting \
                          until its deadline and then reporting a timeout — and when the \
                          batch had already committed, that timeout names a write which \
                          IS durable, so a client that retries on timeout double-applies \
                          it and a client that compensates erases a committed row",
            "operator_action": "the ring only refuses a push when the Control Plane stopped \
                                 draining it: look for a stalled response poller or a \
                                 disconnected bridge consumer on the named core, not for a \
                                 storage fault",
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
