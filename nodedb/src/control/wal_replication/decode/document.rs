// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Document`.

use super::ctx::{DecodeCtx, bind_or_lookup};
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{DocumentOp, UpdateValue};

pub(super) fn point_put(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            &pk_bytes,
            carried,
        )?,
        None => carried,
    };
    Ok(PhysicalPlan::Document(DocumentOp::PointPut {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        surrogate,
        pk_bytes,
        // A replay re-applies the row; it answers no client, so it projects
        // nothing and needs no read gate — see `point_delete`.
        returning: None,
        rls_filters: Vec::new(),
    }))
}

pub(super) fn point_insert(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    if_absent: bool,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let pk_bytes = document_id.as_bytes();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            pk_bytes,
            carried,
        )?,
        None => carried,
    };
    Ok(PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        if_absent,
        surrogate,
        // Replay projects nothing back — see `point_delete`.
        returning: None,
        rls_filters: Vec::new(),
    }))
}

pub(super) fn point_delete(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, &pk_bytes, carried)?;
    Ok(PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
        pk_bytes,
        returning: None,
        rls_filters: Vec::new(),
        // A replayed entry carries no policy of its own: the leader decided
        // this row against the writer's write policy before the record was
        // committed, and a follower must apply exactly what the leader applied
        // or the replicas diverge. Both slots stay empty for the same reason.
        rls_write_check: Vec::new(),
    }))
}

pub(super) fn point_update(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, &pk_bytes, carried)?;
    Ok(PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
        pk_bytes,
        updates: updates.to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        // Empty on replay — see `point_delete`.
        rls_write_check: Vec::new(),
    }))
}

pub(super) fn doc_upsert(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, &pk_bytes, carried)?;
    Ok(PhysicalPlan::Document(DocumentOp::Upsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        on_conflict_updates: on_conflict_updates.to_vec(),
        surrogate,
        // Empty on replay — see `point_delete`.
        rls_write_check: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
    }))
}

/// Reconstruct a `BatchInsert` plan, binding each row's carried surrogate to
/// its `document_id` on this replica (mirrors `kv::batch_put`). On apply the
/// existing `execute_document_batch_insert` handler lands each row via
/// `apply_point_put` keyed by the bound surrogate, so a replayed entry
/// overwrites the identical rows — idempotent under exactly-once, LSN-ordered
/// Raft apply.
pub(super) fn batch_insert(
    ctx: &DecodeCtx,
    collection: &str,
    documents: &[(String, Vec<u8>)],
    surrogates: &[u32],
) -> crate::Result<PhysicalPlan> {
    // `zip` below stops at the shorter side, so a record that lost surrogates
    // would decode into a plan whose rows have no cross-engine identity — the
    // apply then refuses the whole batch, but only after the truncation has
    // already been silently baked into the plan. Refuse it here, where the
    // discrepancy is still visible as what it is: a malformed record.
    if documents.len() != surrogates.len() {
        return Err(crate::Error::Serialization {
            format: "replicated_write".into(),
            detail: format!(
                "batch insert record for '{collection}' carries {} documents but {} \
                 surrogates; every row must carry its own surrogate",
                documents.len(),
                surrogates.len(),
            ),
        });
    }
    let resolved = documents
        .iter()
        .zip(surrogates.iter())
        .map(|((document_id, _value), carried)| {
            let carried = nodedb_types::Surrogate::new(*carried);
            match ctx.assigner {
                Some(a) => a.bind(
                    ctx.database_id,
                    ctx.tenant_id,
                    collection,
                    document_id.as_bytes(),
                    carried,
                ),
                None => Ok(carried),
            }
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(PhysicalPlan::Document(DocumentOp::BatchInsert {
        collection: collection.to_owned(),
        documents: documents.to_vec(),
        surrogates: resolved,
        // Replay projects nothing back — see `point_delete`.
        returning: None,
        rls_filters: Vec::new(),
    }))
}

/// Reconstruct the bulk plan in its plain (non-OLLP) form. The apply
/// re-scans local state at this committed log position and mutates the
/// predicate matches; `ollp_predicted_surrogates = None` selects the
/// local-scan path in the executor (no leader-only verification, no
/// predicted set). Deterministic across replicas: Raft log order ⇒
/// identical prior state ⇒ identical matching set; cascade cleanup keys off
/// each matched row's existing surrogate. No surrogate binding is needed
/// here — the matches already carry their identity.
pub(super) fn bulk_dml(
    collection: &str,
    filters: &[u8],
    is_update: bool,
    updates: &[(String, UpdateValue)],
) -> PhysicalPlan {
    if is_update {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: collection.to_owned(),
            filters: filters.to_vec(),
            updates: updates.to_vec(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            // Empty on replay — see `point_delete`.
            rls_write_check: Vec::new(),
        })
    } else {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: collection.to_owned(),
            filters: filters.to_vec(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            // Empty on replay — see `point_delete`.
            rls_write_check: Vec::new(),
        })
    }
}

/// Reconstruct a `Truncate` plan. `DocumentOp::Truncate` is autocommit-only
/// and clearing a collection is idempotent + deterministic, so every replica
/// safely re-executes the same clear on apply. No surrogate binding: there is
/// no per-row identity, just a whole-collection clear.
pub(super) fn truncate(collection: &str, restart_identity: bool) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Truncate {
        collection: collection.to_owned(),
        restart_identity,
    })
}

pub(super) fn insert_select(
    target_collection: &str,
    source_collection: &str,
    source_filters: &[u8],
    source_limit: usize,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::InsertSelect {
        target_collection: target_collection.to_owned(),
        source_collection: source_collection.to_owned(),
        source_filters: source_filters.to_vec(),
        source_limit,
    })
}
