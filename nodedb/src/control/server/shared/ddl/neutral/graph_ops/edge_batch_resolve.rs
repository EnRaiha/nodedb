// SPDX-License-Identifier: BUSL-1.1

//! Per-edge resolution for the batched edge statements.
//!
//! Split out of `edge_batch.rs` to stay under the per-file line limit. The
//! batch entry points share one shape: resolve each triple's surrogates and its
//! row-level-security verdict, then decide whether the edge can ride a
//! property-less `BatchEdge` plan or has to keep the single-edge route.

use nodedb_physical::physical_plan::BatchEdge;
use nodedb_sql::ddl_ast::GraphEdgeTuple;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::surrogate_exchange::assign_surrogates_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_types::Surrogate;

use super::super::super::result::DdlError;
use super::support::ddl_err;

/// One batch's worth of resolved delete edges, split by how they must be
/// dispatched.
pub(super) struct ResolvedDelete {
    /// Edges that can ride a property-less `EdgeDeleteBatch` plan.
    pub batch: Vec<BatchEdge>,
    /// Edges a write policy governs. `BatchEdge` carries no write-gate slot, so
    /// these keep the single-edge route where the Data Plane decides the
    /// compiled predicate against the stored image. Batching them instead would
    /// bypass the policy.
    pub governed: Vec<GraphEdgeTuple>,
}

/// Resolve surrogate identities and the RLS verdict for every triple in a
/// `GRAPH DELETE EDGES` statement.
///
/// The verdict belongs to the injection pass: a delete carries no image, so a
/// policy compiles into the plan's write-gate slot. Only a plan that ends up
/// carrying a `Predicate` is governed — `NoPolicyApplies` means injection ran
/// and found nothing for this identity here, which is the common case and the
/// one that batches.
pub(super) async fn resolve_delete_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    edges: Vec<GraphEdgeTuple>,
) -> Result<ResolvedDelete, DdlError> {
    let tenant_id = identity.tenant_id;
    let key = nodedb_types::CollectionKey::from_bare(database_id, collection);
    let mut batch: Vec<BatchEdge> = Vec::new();
    let mut governed: Vec<GraphEdgeTuple> = Vec::new();

    let surrogates = assign_endpoints(state, key, tenant_id, &edges).await?;
    for (tuple, (src_surrogate, dst_surrogate)) in edges.into_iter().zip(surrogates) {
        let resolved = super::edge_rls::resolve_edge_write_rls(
            state,
            identity,
            database_id,
            nodedb_physical::physical_plan::GraphOp::EdgeDelete {
                collection: nodedb_types::QualifiedCollection::new(database_id, collection),
                src_id: tuple.src.clone(),
                label: tuple.label.clone(),
                dst_id: tuple.dst.clone(),
                src_surrogate,
                dst_surrogate,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            },
        )?;
        let nodedb_physical::physical_plan::GraphOp::EdgeDelete {
            collection: qc,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            rls_write_check,
        } = resolved
        else {
            return Err(ddl_err(
                "XX000",
                "edge delete plan changed shape during RLS resolution",
            ));
        };

        if matches!(rls_write_check, nodedb_types::RlsWriteCheck::Predicate(_)) {
            governed.push(tuple);
            continue;
        }
        batch.push(BatchEdge {
            collection: qc,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
        });
    }

    Ok(ResolvedDelete { batch, governed })
}

/// Assign the surrogate of every endpoint of `edges`, as `(src, dst)` pairs in
/// `edges` order.
///
/// One call per statement. Each endpoint the local catalog does not bind goes
/// to the collection's home as part of one request set, so a statement costs
/// one assignment round instead of two per edge.
async fn assign_endpoints(
    state: &SharedState,
    key: nodedb_types::CollectionKey<'_>,
    tenant_id: crate::types::TenantId,
    edges: &[GraphEdgeTuple],
) -> Result<Vec<(Surrogate, Surrogate)>, DdlError> {
    let pks: Vec<&[u8]> = edges
        .iter()
        .flat_map(|t| [t.src.as_bytes(), t.dst.as_bytes()])
        .collect();
    let bound = assign_surrogates_routed(state, key, tenant_id, &pks, TraceId::ZERO)
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    Ok(bound.chunks_exact(2).map(|p| (p[0], p[1])).collect())
}

/// Validate every triple in a batch statement and drop repeats of the same
/// `(src, dst, label)`. Both callers run this before dispatching or staging any
/// edge, so a late rejection cannot leave the edges before it already written.
///
/// Dedup is about cost, not the count: it saves the redundant dispatch. On the
/// delete side a repeat contributes nothing anyway, because the second copy
/// reads the first copy's tombstone as a pre-image and counts zero.
pub(super) fn check_and_dedupe(
    edges: &mut Vec<GraphEdgeTuple>,
    stmt: &str,
) -> Result<(), DdlError> {
    for tuple in edges.iter() {
        if tuple.src.is_empty() || tuple.dst.is_empty() {
            return Err(ddl_err("42601", format!("{stmt} requires FROM and TO")));
        }
        super::edge_parse::validate_edge_label(&tuple.label)?;
    }
    let mut seen: std::collections::HashSet<(String, String, String)> =
        std::collections::HashSet::new();
    edges.retain(|t| seen.insert((t.src.clone(), t.dst.clone(), t.label.clone())));
    Ok(())
}

/// One batch's worth of resolved insert edges.
pub(super) struct ResolvedInsert {
    /// Edges that can ride a property-less `EdgePutBatch` plan.
    pub batch: Vec<BatchEdge>,
}

/// Resolve surrogate identities and the write-policy verdict for every triple
/// in a `GRAPH INSERT EDGES` statement.
pub(super) async fn resolve_insert_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    edges: Vec<GraphEdgeTuple>,
) -> Result<ResolvedInsert, DdlError> {
    let tenant_id = identity.tenant_id;
    let key = nodedb_types::CollectionKey::from_bare(database_id, collection);
    let mut batch: Vec<BatchEdge> = Vec::new();

    let surrogates = assign_endpoints(state, key, tenant_id, &edges).await?;
    for (tuple, (src_surrogate, dst_surrogate)) in edges.into_iter().zip(surrogates) {
        let resolved = super::edge_rls::resolve_edge_write_rls(
            state,
            identity,
            database_id,
            nodedb_physical::physical_plan::GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(database_id, collection),
                src_id: tuple.src,
                label: tuple.label,
                dst_id: tuple.dst,
                properties: Vec::new(),
                src_surrogate,
                dst_surrogate,
            },
        )?;

        match resolved {
            nodedb_physical::physical_plan::GraphOp::EdgePut {
                collection: qc,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
            } if properties.is_empty() => {
                batch.push(BatchEdge {
                    collection: qc,
                    src_id,
                    label,
                    dst_id,
                    src_surrogate,
                    dst_surrogate,
                });
            }
            // `BatchEdge` carries no properties, so an image-carrying plan
            // cannot ride the batch. Injection reads the image and never writes
            // it, so this arm is unreachable; fail loudly instead of dropping
            // the image if that ever changes.
            other => {
                return Err(DdlError::internal(format!(
                    "edge batch resolution cannot batch plan: {other:?}"
                )));
            }
        }
    }

    Ok(ResolvedInsert { batch })
}

/// Delete one edge through the single-edge path and return the count it
/// reported, so a batch caller sums real removals instead of counting triples.
#[allow(clippy::too_many_arguments)]
pub(super) async fn delete_one_summing(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    src: String,
    dst: String,
    label: String,
    txn_ctx: &crate::control::server::shared::session::DmlTxnCtx<'_>,
) -> Result<u64, DdlError> {
    let mut affected: u64 = 0;
    for result in super::edge::delete_edge(
        state,
        identity,
        database_id,
        super::edge::EdgeRef {
            collection: collection.to_string(),
            src,
            dst,
            label,
        },
        txn_ctx,
    )
    .await?
    {
        if let crate::control::server::shared::ddl::result::DdlResult::Status {
            rows_affected: Some(n),
            ..
        } = result
        {
            affected = affected.saturating_add(n);
        }
    }
    Ok(affected)
}

/// Bucket by source home and dispatch one apply burst per home, returning the
/// summed removed count.
///
/// Single-node only: the source home writes both the forward and the reverse
/// row locally, so each edge belongs to exactly one bucket and no edge is
/// counted twice. The count comes from the Data Plane's own live-pre-image
/// read, never from the batch length.
pub(super) async fn dispatch_delete_batches_by_src_home(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    database_id: DatabaseId,
    edges: Vec<BatchEdge>,
) -> Result<u64, DdlError> {
    use std::collections::BTreeMap;

    let mut buckets: BTreeMap<u32, Vec<BatchEdge>> = BTreeMap::new();
    for edge in edges {
        buckets
            .entry(VShardId::from_key(edge.src_id.as_bytes()).as_u32())
            .or_default()
            .push(edge);
    }

    let mut removed: u64 = 0;
    for (vshard, group) in buckets {
        let plan = crate::bridge::envelope::PhysicalPlan::Graph(
            nodedb_physical::physical_plan::GraphOp::EdgeDeleteBatch { edges: group },
        );
        let response =
            crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                state,
                tenant_id,
                database_id,
                VShardId::new(vshard),
                plan,
                crate::event::EventSource::User,
            )
            .await
            .map_err(|e| DdlError::from_error(&e))?;
        super::support::data_plane_verdict(&response)?;
        removed =
            removed.saturating_add(super::edge::affected_count(&response, "batch edge delete")?);
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(src: &str, dst: &str, label: &str) -> GraphEdgeTuple {
        GraphEdgeTuple {
            src: src.to_string(),
            dst: dst.to_string(),
            label: label.to_string(),
        }
    }

    /// A repeat of one triple is the same edge, so the insert count must drop
    /// it. The un-deduped form reports 2 for this input.
    #[test]
    fn check_and_dedupe_drops_a_repeated_triple() {
        let mut edges = vec![
            tuple("a", "b", "L"),
            tuple("a", "b", "L"),
            tuple("a", "b", "M"),
        ];

        check_and_dedupe(&mut edges, "GRAPH INSERT EDGES").expect("valid triples");

        assert_eq!(
            edges.len(),
            2,
            "the repeated triple must collapse, the distinct label must not"
        );
    }

    /// An empty endpoint is rejected for the whole list, before any caller can
    /// stage the tuples ahead of it.
    #[test]
    fn check_and_dedupe_rejects_an_empty_endpoint() {
        let mut edges = vec![tuple("a", "b", "L"), tuple("", "z", "L")];

        let error = check_and_dedupe(&mut edges, "GRAPH INSERT EDGES")
            .expect_err("an empty endpoint must be rejected");

        assert!(
            error.message.contains("FROM and TO"),
            "the error must name the missing endpoint: {}",
            error.message
        );
    }
}
