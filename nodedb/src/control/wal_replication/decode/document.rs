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
        })
    } else {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: collection.to_owned(),
            filters: filters.to_vec(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
        })
    }
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
