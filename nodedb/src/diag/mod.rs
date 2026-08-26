// SPDX-License-Identifier: BUSL-1.1

//! Black-box recorder wiring for capture sites outside the WAL. One report
//! per root cause, filed at the detecting site, never re-emitted. This
//! crate hosts the recorder (`bootstrap::diagnostics` calls `faultbox::init`),
//! so these entry points are unconditional — no feature gate, no fallback.

mod context;
mod recording;

pub use context::{IlpFlushOutcome, LostResponseWrite};
pub use recording::{
    batch_insert_without_surrogates, calvin_completion_timeout, catalog_apply_orphan_row,
    collection_purge_row_missing, data_plane_response_lost, data_plane_responses_lost, entry_kind,
    fts_index_update_failed, ilp_invalid_utf8_drop, ilp_line_read_drop, metadata_apply_wedged,
    replay_record_unapplied, write_acked_without_durability,
};
