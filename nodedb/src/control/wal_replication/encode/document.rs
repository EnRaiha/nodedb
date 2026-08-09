// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Document` variants into `ReplicatedWrite`.

use super::super::types::ReplicatedWrite;
use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Surrogate;

pub(super) fn point_put(
    collection: &str,
    document_id: &str,
    value: &[u8],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::PointPut {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        surrogate,
    }
}

pub(super) fn point_insert(
    collection: &str,
    document_id: &str,
    value: &[u8],
    if_absent: bool,
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::PointInsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        if_absent,
        surrogate,
    }
}

pub(super) fn point_delete(collection: &str, document_id: &str, surrogate: u32) -> ReplicatedWrite {
    ReplicatedWrite::PointDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
    }
}

pub(super) fn point_update(
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::PointUpdate {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        updates: updates.to_vec(),
        surrogate,
    }
}

pub(super) fn upsert(
    collection: &str,
    document_id: &str,
    value: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::DocUpsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        on_conflict_updates: on_conflict_updates.to_vec(),
        surrogate,
    }
}

pub(super) fn batch_insert(
    collection: &str,
    documents: &[(String, Vec<u8>)],
    surrogates: &[Surrogate],
) -> ReplicatedWrite {
    ReplicatedWrite::DocBatchInsert {
        collection: collection.to_owned(),
        documents: documents.to_vec(),
        surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
    }
}

/// `DocumentOp::Truncate` replicates as a plain `DocTruncate` entry: it is
/// autocommit-only and clearing a collection is idempotent + deterministic,
/// so every replica safely re-executes the clear on apply. No surrogate to
/// carry — the whole collection is cleared, not a single row.
pub(super) fn truncate(collection: &str, restart_identity: bool) -> ReplicatedWrite {
    ReplicatedWrite::DocTruncate {
        collection: collection.to_owned(),
        restart_identity,
    }
}

/// Single-shard bulk predicate writes replicate as a plain `BulkDml` entry:
/// each replica re-scans local state at the committed log position and
/// applies the predicate deterministically (Raft log order ⇒ identical prior
/// state ⇒ identical matching set). An OLLP-prepared bulk plan (carrying
/// `ollp_predicted_surrogates` / `ollp_predicted_edges`) belongs to the
/// cross-shard Calvin path and is NOT encoded here — the caller returns
/// `None` for those and dispatches via Calvin instead.
pub(super) fn bulk_delete(collection: &str, filters: &[u8]) -> ReplicatedWrite {
    ReplicatedWrite::BulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: false,
        updates: Vec::new(),
    }
}

pub(super) fn bulk_update(
    collection: &str,
    filters: &[u8],
    updates: &[(String, UpdateValue)],
) -> ReplicatedWrite {
    ReplicatedWrite::BulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: true,
        updates: updates.to_vec(),
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
