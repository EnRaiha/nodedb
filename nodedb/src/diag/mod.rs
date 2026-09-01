// SPDX-License-Identifier: BUSL-1.1

//! Black-box recorder wiring for capture sites outside the WAL. One report
//! per root cause, filed at the detecting site, never re-emitted. This
//! crate hosts the recorder (`bootstrap::diagnostics` calls `faultbox::init`),
//! so these entry points are unconditional — no feature gate, no fallback.

mod context;
mod recording;

pub use context::{DATABASE_SCOPE, IlpFlushOutcome, LostResponseWrite, TENANT_SCOPE};
pub use recording::{
    batch_insert_without_surrogates, calvin_completion_timeout, catalog_apply_orphan_row,
    collection_purge_row_missing, data_plane_response_lost, data_plane_responses_lost, entry_kind,
    fts_index_update_failed, ilp_invalid_utf8_drop, ilp_line_read_drop, metadata_apply_wedged,
    quota_row_invalid, quota_row_undecodable, quota_row_write_failed, quota_scope_purge_incomplete,
    quota_scope_replay_aborted, replay_record_unapplied, write_acked_without_durability,
};
