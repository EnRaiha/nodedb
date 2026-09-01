// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for line-protocol ingest that dropped accepted input.

use faultbox::{Capture, EventKind};

use crate::diag::context;

/// Report an ILP connection terminated by an undecodable line while it still
/// held accepted lines. Called from the invalid-UTF-8 arm; the sibling
/// read-failure arm is a different root cause and files its own report.
pub fn ilp_invalid_utf8_drop(
    peer: &str,
    database_id: u64,
    buffered_lines: u64,
    outcome: context::IlpFlushOutcome,
) {
    record_ilp_drop("invalid_utf8", peer, database_id, buffered_lines, outcome);
}

/// Report an ILP connection terminated by a failed or over-length line read
/// while it still held accepted lines. Called from the read-error arm.
pub fn ilp_line_read_drop(
    peer: &str,
    database_id: u64,
    buffered_lines: u64,
    outcome: context::IlpFlushOutcome,
) {
    record_ilp_drop(
        "line_read_failed",
        peer,
        database_id,
        buffered_lines,
        outcome,
    );
}

/// Shared emit for the two ILP termination causes. Private so the only entry
/// points are the one-per-cause functions above.
fn record_ilp_drop(
    cause: &'static str,
    peer: &str,
    database_id: u64,
    buffered_lines: u64,
    outcome: context::IlpFlushOutcome,
) {
    let ctx = context::IlpAcceptedLinesDropped {
        cause,
        peer,
        database_id,
        buffered_lines,
        outcome,
    };
    let _ = Capture::new(
        EventKind::Error,
        "ILP connection terminated holding lines the client can never learn the fate of",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
