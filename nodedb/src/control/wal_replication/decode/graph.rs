// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Graph`.

use super::ctx::DecodeCtx;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{BatchEdge, GraphOp};
use nodedb_types::RlsWriteCheck;

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
        collection: nodedb_types::QualifiedCollection::from_stored(f.collection.to_owned()),
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
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        src_id: src_id.to_owned(),
        label: label.to_owned(),
        dst_id: dst_id.to_owned(),
        src_surrogate,
        dst_surrogate,
        // No predicate here: this node is a follower applying an
        // already-committed write. The writing identity is not available on
        // this node. The leader enforces RLS before proposing the write.
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
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
            collection: nodedb_types::QualifiedCollection::from_stored(e.collection.clone()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::{ReplicatedEntry, ReplicatedWrite};
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_types::QualifiedCollection;

    /// Decide + encode in one call, so each test names only the plan it encodes.
    fn to_replicated_entry(
        tenant_id: TenantId,
        database_id: DatabaseId,
        vshard_id: VShardId,
        plan: &PhysicalPlan,
    ) -> crate::Result<Option<ReplicatedEntry>> {
        let write = crate::control::wal_replication::ReplicableWrite::decide_for_replication(plan)?;
        crate::control::wal_replication::encode::to_replicated_entry(
            tenant_id,
            database_id,
            vshard_id,
            &write,
        )
    }

    #[test]
    fn edge_put_surrogates_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "graph"),
            src_id: "alice".into(),
            label: "knows".into(),
            dst_id: "bob".into(),
            properties: vec![],
            src_surrogate: nodedb_types::Surrogate::new(11),
            dst_surrogate: nodedb_types::Surrogate::new(22),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("EdgePut should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();

        // Verify wire representation carries the raw u32 values.
        let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
        match &decoded_entry.write {
            ReplicatedWrite::EdgePut {
                src_surrogate,
                dst_surrogate,
                ..
            } => {
                assert_eq!(
                    *src_surrogate, 11u32,
                    "src_surrogate must roundtrip on wire"
                );
                assert_eq!(
                    *dst_surrogate, 22u32,
                    "dst_surrogate must roundtrip on wire"
                );
            }
            other => panic!("expected EdgePut, got {other:?}"),
        }

        // Verify the decoded PhysicalPlan uses the carried (authoritative) surrogates.
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Graph(GraphOp::EdgePut {
                src_surrogate,
                dst_surrogate,
                ..
            }) => {
                assert_eq!(
                    src_surrogate,
                    nodedb_types::Surrogate::new(11),
                    "src_surrogate must survive encode→decode"
                );
                assert_eq!(
                    dst_surrogate,
                    nodedb_types::Surrogate::new(22),
                    "dst_surrogate must survive encode→decode"
                );
            }
            other => panic!("expected Graph(EdgePut), got {other:?}"),
        }
    }
}
