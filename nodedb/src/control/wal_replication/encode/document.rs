// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Document` variants into `ReplicatedWrite`.
//!
//! # The materialized-sum resolution travels with the write
//!
//! Every document write that can maintain a derived total carries the
//! resolution the proposing node made at plan time — the join-key VALUE → target
//! row SURROGATE table — and, where the plan has one, the list of targets whose
//! delta was split onto a sibling `ApplyBalanceDelta` entry. Both are copied
//! onto the record here rather than left for the applier to re-derive: see
//! `ReplicatedWrite::PointPut::resolved_sum_targets` for why no applying node
//! can answer either question locally.

use super::super::types::ReplicatedWrite;
use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Surrogate;

/// Flatten a plan's resolution into its wire shape.
///
/// `Surrogate` is a newtype over `u32` and every other identity on this wire
/// travels as the bare `u32`, so the resolution does too.
fn wire_targets(resolved: &[(String, Surrogate)]) -> Vec<(String, u32)> {
    resolved
        .iter()
        .map(|(join_value, surrogate)| (join_value.clone(), surrogate.as_u32()))
        .collect()
}

pub(super) fn point_put(
    collection: &str,
    document_id: &str,
    value: &[u8],
    surrogate: u32,
    resolved_sum_targets: &[(String, Surrogate)],
) -> ReplicatedWrite {
    ReplicatedWrite::PointPut {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        surrogate,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
    }
}

pub(super) fn point_insert(
    collection: &str,
    document_id: &str,
    value: &[u8],
    if_absent: bool,
    surrogate: u32,
    resolved_sum_targets: &[(String, Surrogate)],
    deferred_sum_targets: &[String],
) -> ReplicatedWrite {
    ReplicatedWrite::PointInsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        if_absent,
        surrogate,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        deferred_sum_targets: deferred_sum_targets.to_vec(),
    }
}

pub(super) fn point_delete(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    resolved_sum_targets: &[(String, Surrogate)],
) -> ReplicatedWrite {
    ReplicatedWrite::PointDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
    }
}

pub(super) fn point_update(
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
    surrogate: u32,
    resolved_sum_targets: &[(String, Surrogate)],
) -> ReplicatedWrite {
    ReplicatedWrite::PointUpdate {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        updates: updates.to_vec(),
        surrogate,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
    }
}

pub(super) fn upsert(
    collection: &str,
    document_id: &str,
    value: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
    surrogate: u32,
    resolved_sum_targets: &[(String, Surrogate)],
) -> ReplicatedWrite {
    ReplicatedWrite::DocUpsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        on_conflict_updates: on_conflict_updates.to_vec(),
        surrogate,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
    }
}

pub(super) fn batch_insert(
    collection: &str,
    documents: &[(String, Vec<u8>)],
    surrogates: &[Surrogate],
    resolved_sum_targets: &[(String, Surrogate)],
    deferred_sum_targets: &[String],
) -> ReplicatedWrite {
    ReplicatedWrite::DocBatchInsert {
        collection: collection.to_owned(),
        documents: documents.to_vec(),
        surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        deferred_sum_targets: deferred_sum_targets.to_vec(),
    }
}

/// `DocumentOp::Truncate` replicates as a plain `DocTruncate` entry: it is
/// autocommit-only and clearing a collection is idempotent + deterministic,
/// so every replica safely re-executes the clear on apply. No surrogate to
/// carry — the whole collection is cleared, not a single row. The balance the
/// cleared rows fed is not re-derivable, so its resolution rides along.
pub(super) fn truncate(
    collection: &str,
    restart_identity: bool,
    resolved_sum_targets: &[(String, Surrogate)],
) -> ReplicatedWrite {
    ReplicatedWrite::DocTruncate {
        collection: collection.to_owned(),
        restart_identity,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
    }
}

/// Single-shard bulk predicate writes replicate as a plain `BulkDml` entry:
/// each replica re-scans local state at the committed log position and
/// applies the predicate deterministically (Raft log order ⇒ identical prior
/// state ⇒ identical matching set). An OLLP-prepared bulk plan (carrying
/// `ollp_predicted_surrogates` / `ollp_predicted_edges`) belongs to the
/// cross-shard Calvin path and is NOT encoded here — the caller returns
/// `None` for those and dispatches via Calvin instead.
pub(super) fn bulk_delete(
    collection: &str,
    filters: &[u8],
    resolved_sum_targets: &[(String, Surrogate)],
) -> ReplicatedWrite {
    ReplicatedWrite::BulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: false,
        updates: Vec::new(),
        resolved_sum_targets: wire_targets(resolved_sum_targets),
    }
}

pub(super) fn bulk_update(
    collection: &str,
    filters: &[u8],
    updates: &[(String, UpdateValue)],
    resolved_sum_targets: &[(String, Surrogate)],
) -> ReplicatedWrite {
    ReplicatedWrite::BulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: true,
        updates: updates.to_vec(),
        resolved_sum_targets: wire_targets(resolved_sum_targets),
    }
}

/// `INSERT ... SELECT ... WHERE <predicate>` replicates as a plain
/// `InsertSelect` entry: each replica re-scans the source at the committed
/// log position and copies the predicate matches, reusing each source row's
/// surrogate/doc_id. Deterministic by Raft log order ⇒ identical prior state
/// ⇒ identical copied set.
pub(super) fn insert_select(
    target_collection: &str,
    source_collection: &str,
    source_filters: &[u8],
    source_limit: usize,
) -> ReplicatedWrite {
    ReplicatedWrite::InsertSelect {
        target_collection: target_collection.to_owned(),
        source_collection: source_collection.to_owned(),
        source_filters: source_filters.to_vec(),
        source_limit,
    }
}

/// `DocumentOp::ApplyBalanceDelta` replicates as the DELTA it is.
///
/// Modelled on `KvIncr`: the record says what the statement did, every replica
/// applies it exactly once in log order, and the balance each replica ends up
/// with is its own prior balance plus the same signed amount. The decimal
/// travels as a string because a balance is not integral and `f64` is lossy
/// past 15 significant digits — the same reason the stored total is a string.
pub(super) fn apply_balance_delta(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    column: &str,
    delta: &str,
    join_column: &str,
    join_value: &str,
) -> ReplicatedWrite {
    ReplicatedWrite::ApplyBalanceDelta {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
        column: column.to_owned(),
        delta: delta.to_owned(),
        join_column: join_column.to_owned(),
        join_value: join_value.to_owned(),
    }
}
