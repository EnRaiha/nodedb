// SPDX-License-Identifier: BUSL-1.1

//! Black-box recorder wiring for capture sites outside the WAL.
//!
//! Mirrors `nodedb-wal/src/diag/`: one report per root cause, filed at the
//! site that detects the failure and never re-emitted as the error
//! propagates. This crate is the recorder's host (`bootstrap::diagnostics`
//! calls `faultbox::init`), so unlike the WAL crate these entry points are
//! unconditional — no feature gate, no inert fallback — `faultbox` is always
//! in this binary's dependency graph.

mod context;
mod recording;

pub use context::IlpFlushOutcome;
pub use recording::{
    calvin_completion_timeout, entry_kind, ilp_invalid_utf8_drop, ilp_line_read_drop,
    metadata_apply_wedged, replay_record_unapplied,
};
