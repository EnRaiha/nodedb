// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Graph`.

use super::ctx::DecodeCtx;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

/// Fields of the `EdgePut` wire variant, bundled so [`edge_put`] stays under
/// the `too_many_arguments` clippy threshold.
pub(super) struct EdgePutFields<'a> {
    pub(super) collection: &'a str,
    pub(super) src_id: &'a str,
    pub(super) label: &'a str,
    pub(super) dst_id: &'a str,
    pub(super) properties: &'a [u8],
    pub(super) src_surrogate: u32,
    pub(super) dst_surrogate: u32,
}

pub(super) fn edge_put(ctx: &DecodeCtx, f: EdgePutFields) -> crate::Result<PhysicalPlan> {
    let carried_src = nodedb_types::Surrogate::new(f.src_surrogate);
    let src_surrogate = match ctx.assigner {
        Some(a) => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            f.collection,
            f.src_id.as_bytes(),
            carried_src,
        )?,
        None => carried_src,
    };
    let carried_dst = nodedb_types::Surrogate::new(f.dst_surrogate);
    let dst_surrogate = match ctx.assigner {
        Some(a) => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            f.collection,
            f.dst_id.as_bytes(),
            carried_dst,
        )?,
        None => carried_dst,
    };
    Ok(PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: f.collection.to_owned(),
        src_id: f.src_id.to_owned(),
        label: f.label.to_owned(),
        dst_id: f.dst_id.to_owned(),
        properties: f.properties.to_vec(),
        src_surrogate,
        dst_surrogate,
    }))
}

pub(super) fn edge_delete(
    ctx: &DecodeCtx,
    collection: &str,
    src_id: &str,
    label: &str,
    dst_id: &str,
    src_surrogate: u32,
    dst_surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried_src = nodedb_types::Surrogate::new(src_surrogate);
    let src_surrogate = match ctx.assigner {
        Some(a) => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            src_id.as_bytes(),
            carried_src,
        )?,
        None => carried_src,
    };
    let carried_dst = nodedb_types::Surrogate::new(dst_surrogate);
    let dst_surrogate = match ctx.assigner {
        Some(a) => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            dst_id.as_bytes(),
            carried_dst,
        )?,
        None => carried_dst,
    };
    Ok(PhysicalPlan::Graph(GraphOp::EdgeDelete {
        collection: collection.to_owned(),
        src_id: src_id.to_owned(),
        label: label.to_owned(),
        dst_id: dst_id.to_owned(),
        src_surrogate,
        dst_surrogate,
        // A replicated write was already admitted by the write policy on the
        // leader that accepted it; re-deciding it on the follower would make
        // replication depend on per-node policy state.
        rls_write_check: Vec::new(),
    }))
}

pub(super) fn set_node_labels(node_id: &str, labels: &[String]) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::SetNodeLabels {
        node_id: node_id.to_owned(),
        labels: labels.to_vec(),
    })
}

pub(super) fn remove_node_labels(node_id: &str, labels: &[String]) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
        node_id: node_id.to_owned(),
        labels: labels.to_vec(),
    })
}

/// Bind the endpoint surrogates for every edge in a `ReplicatedBatchEdge` slice,
/// producing a `Vec<BatchEdge>` with leader-assigned surrogates installed in the
/// local catalog. Shared by the `EdgePutBatch` and `EdgeDeleteBatch` decode arms.
fn bind_batch_edges(
    ctx: &DecodeCtx,
    edges: &[super::super::types::ReplicatedBatchEdge],
) -> crate::Result<Vec<BatchEdge>> {
    let mut bound = Vec::with_capacity(edges.len());
    for e in edges {
        let carried_src = nodedb_types::Surrogate::new(e.src_surrogate);
        let src_surrogate = match ctx.assigner {
            Some(a) => a.bind(
                ctx.database_id,
                ctx.tenant_id,
                &e.collection,
                e.src_id.as_bytes(),
                carried_src,
            )?,
            None => carried_src,
        };
        let carried_dst = nodedb_types::Surrogate::new(e.dst_surrogate);
        let dst_surrogate = match ctx.assigner {
            Some(a) => a.bind(
                ctx.database_id,
                ctx.tenant_id,
                &e.collection,
                e.dst_id.as_bytes(),
                carried_dst,
            )?,
            None => carried_dst,
        };
        bound.push(BatchEdge {
            collection: e.collection.clone(),
            src_id: e.src_id.clone(),
            label: e.label.clone(),
            dst_id: e.dst_id.clone(),
            src_surrogate,
            dst_surrogate,
        });
    }
    Ok(bound)
}

pub(super) fn edge_put_batch(
    ctx: &DecodeCtx,
    edges: &[super::super::types::ReplicatedBatchEdge],
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Graph(GraphOp::EdgePutBatch {
        edges: bind_batch_edges(ctx, edges)?,
    }))
}

pub(super) fn edge_delete_batch(
    ctx: &DecodeCtx,
    edges: &[super::super::types::ReplicatedBatchEdge],
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch {
        edges: bind_batch_edges(ctx, edges)?,
    }))
}
