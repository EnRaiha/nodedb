// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for the metadata applier and the catalog rows it leaves
//! behind when an apply is incomplete.

use faultbox::{Capture, EventKind, error_chain_of};
use nodedb_cluster::MetadataEntry;

use super::shared::{entry_kind, error_class};
use crate::diag::context;

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

/// Report consumer-group offsets that a replicated deletion or migration left
/// behind on this node. Called from the post-apply offset-store arms.
pub fn consumer_group_offsets_retained(
    err: &crate::Error,
    database_id: u64,
    tenant_id: u64,
    stream: &str,
    group: &str,
    operation: &'static str,
) {
    let class = error_class(err);
    let ctx = context::ConsumerGroupOffsetsRetained {
        database_id,
        tenant_id,
        stream,
        group,
        operation,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "post-apply: consumer-group offsets survived a replicated deletion on this node",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
