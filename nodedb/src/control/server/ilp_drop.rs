// SPDX-License-Identifier: BUSL-1.1

//! Terminal ILP read-side failures that would otherwise discard accepted lines.
//!
//! ILP is fire-and-forget: an accepted line is never acked, so a connection
//! that dies holding a partially filled batch gives its client no way to learn
//! which lines landed. Both terminal read-side failures route through here so
//! the accepted-but-unflushed lines get one last dispatch and the termination
//! is recorded, instead of the connection dropping them in silence.

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::warn;

use crate::control::server::ilp_auth::AuthenticatedIlpContext;
use crate::control::state::SharedState;
use crate::diag::IlpFlushOutcome;

use super::ilp_batch::flush_ilp_batch;

/// Why an ILP connection is terminating with accepted lines still buffered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IlpDropCause {
    /// A line's bytes were not valid UTF-8.
    InvalidUtf8,
    /// The socket read failed, or the line exceeded the configured byte cap.
    LineReadFailed,
}

impl IlpDropCause {
    /// The message returned to the caller that terminates the connection.
    ///
    /// Deliberately unchanged from what each site returned before: these
    /// strings are the connection-level error surface, and widening them would
    /// tell a client more about the server's parsing state than it needs.
    fn client_detail(self) -> &'static str {
        match self {
            Self::InvalidUtf8 => "ILP payload is not valid UTF-8",
            Self::LineReadFailed => "ILP line rejected",
        }
    }
}

/// Classify the final flush so the report distinguishes "the client lost the
/// rest of its stream" from "the client also lost lines the server had already
/// accepted".
fn classify(buffered_lines: u64, flush_failed: bool) -> IlpFlushOutcome {
    if buffered_lines == 0 {
        IlpFlushOutcome::NothingBuffered
    } else if flush_failed {
        IlpFlushOutcome::Lost
    } else {
        IlpFlushOutcome::Recovered
    }
}

/// Flush the lines this connection already accepted, file one report for the
/// termination, and return the typed error the connection loop propagates.
///
/// The connection still terminates — a stream whose framing broke cannot be
/// resynchronized, and continuing to read it would splice unrelated bytes into
/// the next line. What changes is that terminating no longer throws away lines
/// the server had already accepted, and never does so without a trace.
pub(super) async fn terminate_with_buffered_flush(
    state: &Arc<SharedState>,
    context: &AuthenticatedIlpContext,
    peer: SocketAddr,
    cause: IlpDropCause,
    batch: &str,
    buffered_lines: u64,
) -> crate::Error {
    let mut flush_failed = false;
    if buffered_lines > 0
        && let Err(error) = flush_ilp_batch(state, context, batch).await
    {
        flush_failed = true;
        warn!(
            %peer,
            buffered_lines,
            error = %error,
            "final ILP flush failed while terminating the connection; accepted lines lost"
        );
    }

    let outcome = classify(buffered_lines, flush_failed);
    let peer = peer.to_string();
    let database_id = context.database_id().as_u64();
    match cause {
        IlpDropCause::InvalidUtf8 => {
            crate::diag::ilp_invalid_utf8_drop(&peer, database_id, buffered_lines, outcome);
        }
        IlpDropCause::LineReadFailed => {
            crate::diag::ilp_line_read_drop(&peer, database_id, buffered_lines, outcome);
        }
    }

    crate::Error::BadRequest {
        detail: cause.client_detail().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{IlpDropCause, classify};
    use crate::diag::IlpFlushOutcome;

    #[test]
    fn empty_batch_is_reported_as_no_accepted_line_lost() {
        assert_eq!(classify(0, false), IlpFlushOutcome::NothingBuffered);
        // A failed flush cannot be reported for a batch that had nothing to
        // flush, so the flag must not be able to manufacture a loss.
        assert_eq!(classify(0, true), IlpFlushOutcome::NothingBuffered);
    }

    #[test]
    fn buffered_lines_separate_recovered_from_lost() {
        assert_eq!(classify(3, false), IlpFlushOutcome::Recovered);
        assert_eq!(classify(3, true), IlpFlushOutcome::Lost);
    }

    #[test]
    fn each_cause_keeps_its_own_client_visible_detail() {
        assert_eq!(
            IlpDropCause::InvalidUtf8.client_detail(),
            "ILP payload is not valid UTF-8"
        );
        assert_eq!(
            IlpDropCause::LineReadFailed.client_detail(),
            "ILP line rejected"
        );
        assert_ne!(
            IlpDropCause::InvalidUtf8.client_detail(),
            IlpDropCause::LineReadFailed.client_detail()
        );
    }
}
