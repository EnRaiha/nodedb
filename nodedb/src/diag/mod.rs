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

pub use context::{IlpFlushOutcome, LostResponseWrite};
pub use recording::{
    batch_insert_without_surrogates, calvin_completion_timeout, data_plane_response_lost,
    data_plane_responses_lost, entry_kind, fts_index_update_failed, ilp_invalid_utf8_drop,
    ilp_line_read_drop, metadata_apply_wedged, replay_record_unapplied,
    write_acked_without_durability,
};
