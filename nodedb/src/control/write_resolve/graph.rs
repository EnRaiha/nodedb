// SPDX-License-Identifier: BUSL-1.1

//! Graph implementation of [`EngineWriteResolver`].
//!
//! Resolves a governed `GraphOp::EdgeDelete` by deciding the policy against the
//! edge's stored property object, then rebuilds the same delete with a decided
//! check.
//!
//! A delete names its edge in full on the plan — collection, endpoints, label —
//! so nothing about the write itself has to be resolved. What cannot cross the
//! wire is the predicate: the image a policy governs is the stored property
//! object, which only the node holding the edge can read, and a follower has no
//! writing identity to evaluate `$auth.*` against. So the resolve pass reads
//! that image and decides it while the identity is live, and the proposed
//! delete carries the verdict instead of the predicate.

use crate::types::VShardId;
use async_trait::async_trait;
use nodedb_types::{DatabaseId, RlsWriteCheck};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local_on_vshard;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::GraphOp;

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// A governed graph edge delete, extracted at interception.
pub struct GraphWriteResolver {
    /// Collection the edge belongs to. Names the write, but does NOT home it.
    collection: String,
    /// Source endpoint. An edge is key-homed, so this is the vshard key.
    src_id: String,
    /// The intercepted delete verbatim, live write predicate included.
    op: GraphOp,
}

/// The resolver for `op`, or `None` when it carries no live write predicate.
///
/// Exhaustive over `GraphOp`: a new graph op fails to compile here rather than
/// silently skipping resolution.
pub(super) fn resolver_for_graph_op(op: &GraphOp) -> Option<Box<dyn EngineWriteResolver>> {
    let (collection, src_id) = match op {
        GraphOp::EdgeDelete {
            collection,
            src_id,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() {
                return None;
            }
            (collection, src_id)
        }
        // `EdgePut` carries its `PROPERTIES` image on the plan, so the policy
        // already decided it at injection. The batch forms carry no image at
        // all and RLS injection refuses them outright. Everything else is a
        // read, a label write that names no collection, or the resolve pass
        // itself.
        GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::ResolveEdgeDelete(_)
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::Stats { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_) => return None,
    };
    Some(Box::new(GraphWriteResolver {
        collection: collection.clone(),
        src_id: src_id.clone(),
        op: op.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for GraphWriteResolver {
    fn collection(&self) -> &str {
        &self.collection
    }

    /// Key-homed on the source endpoint, where the forward edge row lives.
    fn vshard(&self, _database_id: DatabaseId) -> VShardId {
        VShardId::from_key(self.src_id.as_bytes())
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::ResolveEdgeDelete(Box::new(self.op.clone())))
    }

    /// An edge the Data Plane's write-policy gate refuses surfaces here as
    /// `crate::Error::DataPlane(ErrorCode::RejectedAuthz { .. })` — the exact
    /// error the direct delete already returns, because the resolve handler
    /// runs the same `admit_edge_properties` gate against the same pre-image.
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows> {
        let collection = &self.collection;
        let resp = dispatch_local_on_vshard(
            state,
            ctx.tenant_id,
            ctx.database_id,
            self.vshard(ctx.database_id),
            op,
            None,
        )
        .await?;
        if resp.status != Status::Ok {
            return Err(match resp.error_code {
                Some(code) => crate::Error::DataPlane(*code),
                None => crate::Error::Dispatch {
                    detail: format!(
                        "graph governed edge delete: resolve on '{collection}' returned status \
                         {:?} with no error code",
                        resp.status
                    ),
                },
            });
        }
        Ok(ResolvedRows::GraphEdgeDeleteAdmitted)
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        let ResolvedRows::GraphEdgeDeleteAdmitted = resolved else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "graph write resolver for '{}' was handed another engine's resolution; \
                     resolver_for_plan dispatched the wrong engine",
                    self.collection
                ),
            });
        };
        let GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            rls_write_check: _,
        } = self.op.clone()
        else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "graph write resolver for '{}' holds a plan that is not an edge delete",
                    self.collection
                ),
            });
        };
        Ok(PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
        }))
    }
}
