// SPDX-License-Identifier: BUSL-1.1

//! Document implementation of [`EngineWriteResolver`].
//!
//! Resolves a governed deferred document write — `PointUpdate`, `PointDelete`,
//! `Upsert`, `BulkUpdate`, `BulkDelete` — to the concrete row mutations it
//! would apply, then rebuilds it as `DocumentOp::ResolvedWrite`.
//!
//! `Merge` and `UpdateFromJoin` are NOT resolved here. Their Control-Plane
//! orchestrators already expand them into concrete point ops inside an
//! already-open, Calvin-locked transaction, which has no drift window between
//! the decision and the write. This path is autocommit-then-Raft and has
//! exactly that window, which is why every mutation it ships carries a
//! body-level precondition.

use async_trait::async_trait;
use nodedb_types::RlsWriteCheck;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{DocumentOp, DocumentResolveOutcome};

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// A governed deferred document write, extracted at interception.
pub struct DocumentWriteResolver {
    /// Routing collection — also the vshard key.
    collection: String,
    /// The intercepted write verbatim, live write predicate included. The Data
    /// Plane decides the predicate against the images it computes, where the
    /// writing identity is still available.
    op: DocumentOp,
}

/// The resolver for `op`, or `None` when it carries no live write predicate.
///
/// Exhaustive over `DocumentOp`: a new document op fails to compile here rather
/// than silently skipping resolution.
pub(super) fn resolver_for_document_op(op: &DocumentOp) -> Option<Box<dyn EngineWriteResolver>> {
    let collection = match op {
        DocumentOp::PointUpdate {
            collection,
            rls_write_check,
            ..
        }
        | DocumentOp::PointDelete {
            collection,
            rls_write_check,
            ..
        }
        | DocumentOp::Upsert {
            collection,
            rls_write_check,
            ..
        }
        // A predicate write's row set is known only after committed state is
        // scanned, so it resolves like a state-dependent point write.
        | DocumentOp::BulkUpdate {
            collection,
            rls_write_check,
            ..
        }
        | DocumentOp::BulkDelete {
            collection,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() {
                return None;
            }
            collection
        }
        // Already solved by `control::merge_orchestrator` /
        // `control::update_from_join_orchestrator`, which expand them inside an
        // open transaction. Routing them here would resolve them twice.
        DocumentOp::Merge { .. }
        | DocumentOp::UpdateFromJoin { .. }
        // Already decided: `ResolveWrite` is the read-only resolve itself and
        // `ResolvedWrite` carries the verdict. Every remaining op either writes
        // an image the Control Plane already holds, or writes nothing.
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::PointGet { .. }
        | DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => return None,
    };
    Some(Box::new(DocumentWriteResolver {
        collection: collection.clone(),
        op: op.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for DocumentWriteResolver {
    fn collection(&self) -> &str {
        &self.collection
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::ResolveWrite(Box::new(self.op.clone())))
    }

    /// A row the Data Plane's write-policy gate refuses surfaces here as
    /// `crate::Error::DataPlane(ErrorCode::RejectedAuthz { .. })` — the exact
    /// error the same op dispatched directly already returns, because the
    /// resolve handler runs the same `rls_write_gate` gate.
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows> {
        let collection = &self.collection;
        let resp =
            dispatch_local(state, ctx.tenant_id, ctx.database_id, collection, op, None).await?;
        if resp.status != Status::Ok {
            return Err(match resp.error_code {
                Some(code) => crate::Error::DataPlane(*code),
                None => crate::Error::Dispatch {
                    detail: format!(
                        "document governed write: resolve on '{collection}' returned status \
                         {:?} with no error code",
                        resp.status
                    ),
                },
            });
        }

        let outcome: DocumentResolveOutcome =
            zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                detail: format!(
                    "document governed write: could not decode resolved mutations for \
                     '{collection}': {e}"
                ),
            })?;
        Ok(ResolvedRows::Document {
            mutations: outcome.mutations,
            response_payload: outcome.response_payload,
        })
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        match resolved {
            ResolvedRows::Document {
                mutations,
                response_payload,
            } => Ok(PhysicalPlan::Document(DocumentOp::ResolvedWrite {
                mutations,
                response_payload,
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            })),
            ResolvedRows::Update(_) | ResolvedRows::Delete(_) | ResolvedRows::Kv { .. } => {
                Err(crate::Error::Internal {
                    detail: format!(
                        "document write resolver for '{}' was handed another engine's \
                         resolution; resolver_for_plan dispatched the wrong engine",
                        self.collection
                    ),
                })
            }
        }
    }
}
