// SPDX-License-Identifier: BUSL-1.1

//! Recording implementation of capture sites outside the WAL.
//!
//! Each function is called only from the one site that detects its failure,
//! never re-emitted as the error propagates. `Capture::emit` never panics
//! and returns `None` when unrecorded, so the result is deliberately
//! discarded.

use std::sync::atomic::{AtomicU64, Ordering};

use faultbox::{Capture, EventKind, error_chain_of};
use nodedb_cluster::MetadataEntry;

use super::context;

/// Count of finished Data-Plane writes whose response the bounded response
/// ring refused, leaving the caller with nothing but a deadline. Makes the
/// failure visible to the metrics exporter even with no recorder configured.
static DATA_PLANE_RESPONSES_LOST: AtomicU64 = AtomicU64::new(0);

/// Read the count of Data-Plane responses lost to a full response ring.
/// Exposed for the metrics exporter and tests.
pub fn data_plane_responses_lost() -> u64 {
    DATA_PLANE_RESPONSES_LOST.load(Ordering::Relaxed)
}

/// The decoded entry's variant name, read off its `Debug` text rather than an
/// exhaustive match — a forensic label tolerates the approximation, and a
/// new variant keeps reporting a real name with no arm to maintain.
pub fn entry_kind(entry: &MetadataEntry) -> String {
    let debug = format!("{entry:?}");
    match debug.find(|c: char| !(c.is_alphanumeric() || c == '_')) {
        Some(end) => debug[..end].to_owned(),
        None => debug,
    }
}

/// The stable class of an error's `Display` text: the text before the first
/// colon, which names what failed rather than the per-occurrence detail
/// after it.
fn error_class(err: &dyn std::error::Error) -> String {
    let text = err.to_string();
    text.split(':').next().unwrap_or(&text).trim().to_owned()
}

/// Report a durable host-side effect failure that stopped the metadata
/// applier without advancing its watermark. Called from the `apply` loop's
/// `break` on `apply_host_side_effects` error, so a re-delivered failing
/// entry files one growing report, not one per retry.
pub fn metadata_apply_wedged(
    err: &crate::Error,
    entry: &MetadataEntry,
    raft_index: u64,
    last_applied_watermark: u64,
    permanent: bool,
) {
    let kind = entry_kind(entry);
    let class = error_class(err);
    let ctx = context::MetadataApplyWedged {
        raft_index,
        last_applied_watermark,
        entry_kind: &kind,
        error_class: &class,
        permanent,
    };
    let _ = Capture::new(
        EventKind::Error,
        "metadata applier: durable host-side effect failed; watermark not advanced",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

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

/// Report a completed Data-Plane write whose response the bounded response
/// ring refused, so the caller can only learn a deadline. Called from the
/// response-push helper every core-loop completion path funnels through.
pub fn data_plane_response_lost(core_id: usize, write: context::LostResponseWrite) {
    DATA_PLANE_RESPONSES_LOST.fetch_add(1, Ordering::Relaxed);
    let ctx = context::DataPlaneResponseLost { core_id, write };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "Data-Plane response dropped: the caller can never learn this write's outcome",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a `catalog_entry::apply_to` call that left redb with an orphaned
/// parent-replicated row (a primary row with no matching `StoredOwner`, or
/// the reverse). Called from `apply_to`, right after `verify_redb_integrity`.
pub fn catalog_apply_orphan_row(entry_kind: &str, orphan_kind: &str, orphan_count: usize) {
    let ctx = context::CatalogApplyOrphanRow {
        entry_kind,
        orphan_kind,
        orphan_count,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "catalog_entry::apply_to left an orphaned parent-replicated catalog row",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a collection purge that found no catalog row to deactivate.
/// Called only from `apply::collection::prepare_purge_checked`.
pub fn collection_purge_row_missing(database_id: u64, tenant_id: u64, name: &str) {
    let ctx = context::CollectionPurgeRowMissing {
        database_id,
        tenant_id,
        name,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "collection purge found no catalog row to deactivate",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a Calvin cross-shard transaction whose completion wait timed out.
/// Called from the completion-timeout arm of
/// `submit_and_await_calvin_with_timeout`; the sibling "channel closed" arm
/// is a different root cause and is not reported here.
pub fn calvin_completion_timeout(
    err: &crate::Error,
    epoch: u64,
    position: u32,
    participants: usize,
    timeout_secs: u64,
) {
    let ctx = context::CalvinCompletionTimeout {
        epoch,
        position,
        participants,
        timeout_secs,
    };
    let _ = Capture::new(
        EventKind::Error,
        "Calvin transaction completion wait timed out",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a persisted quota row whose stored value did not decode, so boot
/// replay left its scope uncapped. Called from the lossy listings' bad-key
/// arms; `scope` is `database` or `tenant`.
pub fn quota_row_undecodable(scope: &'static str, database_id: u64, tenant_id: Option<u64>) {
    let ctx = context::QuotaRowNotInstalled {
        cause: "undecodable",
        scope,
        database_id,
        tenant_id,
        detail: "stored value did not decode as a quota record",
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "quota replay: a persisted row did not decode, so its scope runs uncapped",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a persisted quota row that failed validation, so boot replay left
/// its scope uncapped. Called from the replay validation arm.
pub fn quota_row_invalid(
    err: &(dyn std::error::Error + 'static),
    scope: &'static str,
    database_id: u64,
    tenant_id: Option<u64>,
) {
    let detail = err.to_string();
    let ctx = context::QuotaRowNotInstalled {
        cause: "invalid_record",
        scope,
        database_id,
        tenant_id,
        detail: &detail,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "quota replay: a persisted row broke its invariants, so its scope runs uncapped",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a boot quota replay that could not list its catalog table, so no
/// row of that scope was installed. Called from each listing's error arm.
pub fn quota_scope_replay_aborted(err: &crate::Error, scope: &'static str) {
    let class = error_class(err);
    let ctx = context::QuotaScopeReplayAborted {
        scope,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "quota replay: catalog read failed, so every scope of this kind runs uncapped",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a quota row write or delete refused while applying a committed
/// catalog entry. Called from each apply arm that logs and continues.
pub fn quota_row_write_failed(
    err: &crate::Error,
    operation: &'static str,
    database_id: u64,
    tenant_id: Option<u64>,
) {
    let class = error_class(err);
    let ctx = context::QuotaRowWriteFailed {
        operation,
        database_id,
        tenant_id,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "quota apply: a committed row write failed, so this node diverges from consensus",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a dropped scope whose tenant quota rows could not be scanned, so
/// some survive the drop. Called from each purge scan's error arm.
pub fn quota_scope_purge_incomplete(
    err: &crate::Error,
    scope: &'static str,
    database_id: Option<u64>,
    tenant_id: Option<u64>,
) {
    let class = error_class(err);
    let ctx = context::QuotaScopePurgeIncomplete {
        scope,
        database_id,
        tenant_id,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "quota purge: tenant row scan failed, so rows of a dropped scope survive",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
