// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for ingest-path capture sites.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// How a terminating ILP connection's already-accepted lines fared. A
/// stable class, never a count, so a flapping client collapses to one report.
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
    pub(in crate::diag) fn as_str(self) -> &'static str {
        match self {
            Self::NothingBuffered => "nothing_buffered",
            Self::Recovered => "recovered",
            Self::Lost => "lost",
        }
    }
}

/// An ILP connection hit a terminal read-side failure while lines it had
/// already accepted were still waiting for their coalescing flush.
pub(in crate::diag) struct IlpAcceptedLinesDropped<'a> {
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
        // Cause + outcome name the bug; peer/database/buffered-line count are
        // the occurrence, or a reconnect loop would storm the recorder.
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
