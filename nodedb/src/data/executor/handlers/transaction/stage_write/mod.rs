// SPDX-License-Identifier: BUSL-1.1

//! Statement-time execution of in-transaction point writes by STAGING them
//! into the per-transaction overlay.
//!
//! A point write issued inside a `BEGIN..COMMIT` block is evaluated here
//! against BASE ∪ OVERLAY: constraint violations surface immediately (at the
//! statement, not deferred to COMMIT), the real affected-row count is
//! computed, and the resulting encoded body (or a tombstone) is recorded in
//! the overlay so a later same-transaction read-modify-write observes it. The
//! write is NOT made durable here — the buffered plan is still replayed
//! through the real apply path inside the COMMIT `TransactionBatch`, which
//! remains the sole durable apply.

mod body;
mod constraint;
mod context;
mod dispatch;
mod stage_bulk_delete;
mod stage_bulk_update;
mod stage_insert_select;
mod stage_kv;

pub(in crate::data::executor) use stage_bulk_delete::StageBulkDeleteParams;
pub(in crate::data::executor) use stage_bulk_update::StageBulkUpdateParams;
pub(in crate::data::executor) use stage_insert_select::StageInsertSelectParams;
pub(in crate::data::executor) use stage_kv::{hex_key, unhex_key};
