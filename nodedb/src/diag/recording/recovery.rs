// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for the write and replay paths: a record that did not
//! apply, a write acked without durability, an index update that was lost.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a committed, CRC-valid WAL record that startup replay could not
/// apply. Called only from `replay_abort`, so a WAL tail that fails
/// identically on every core files one growing report, not one per core.
pub fn replay_record_unapplied(
    engine: &str,
    stage: &str,
    core_id: usize,
    record_lsn: u64,
    detail: &str,
) {
    let ctx = context::ReplayRecordUnapplied {
        engine,
        stage,
        core_id,
        record_lsn,
        detail,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "WAL replay: a committed record could not be applied",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report an acknowledged write whose redo record the Control-Plane funnel
/// was supposed to mint but did not. Called only from the durable-at-ack
/// barrier in `submit_write`, so a hammered op files one growing report.
pub fn write_acked_without_durability(engine: &'static str) {
    let ctx = context::WriteAckedWithoutDurability { engine };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "write acknowledged with no durable redo record",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a document write rejected because its inverted-index update
/// failed. Called from `index_document_in_txn`'s error arm — the client's
/// error message says the write failed, not that the FTS index caused it.
pub fn fts_index_update_failed(err: &crate::Error, collection: &str, surrogate: u32) {
    let class = error_class(err);
    let ctx = context::FtsIndexUpdateFailed {
        collection,
        surrogate,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "document write rejected: full-text index update failed",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a document batch insert refused because its rows carry no
/// surrogates. Called from the batch-insert handler's parallel-length guard;
/// the actual defect is in whatever produced the mismatched plan.
pub fn batch_insert_without_surrogates(
    collection: &str,
    document_count: usize,
    surrogate_count: usize,
) {
    let ctx = context::BatchInsertWithoutSurrogates {
        collection,
        document_count,
        surrogate_count,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "document batch insert refused: rows carry no cross-engine identity",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
