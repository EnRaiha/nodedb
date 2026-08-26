// SPDX-License-Identifier: BUSL-1.1

//! KV implementation of [`EngineWriteResolver`].
//!
//! Resolves a governed state-dependent KV write — an atomic increment, a CAS,
//! a field merge, a TTL mutation, a transfer, a predicate UPDATE/DELETE — to
//! the concrete mutations it would apply, then rebuilds it as
//! `KvOp::ResolvedWrite`.
//!
//! A KV write is not row-set shaped: one statement can put, delete, and move
//! TTL across two collections, and a `CAS` that did not match owes the caller
//! a reply while writing nothing. So the decided form is a mutation list plus
//! the exact response payload, both reported by `KvOp::ResolveWrite`.

use async_trait::async_trait;
use nodedb_types::RlsWriteCheck;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{KvOp, KvResolveOutcome};

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// A governed state-dependent KV write, extracted at interception.
pub struct KvWriteResolver {
    /// Routing collection — also the vshard key. `TransferItem` reports its
    /// source collection, matching `KvOp::collection()`: both of its rows
    /// co-locate on one vshard.
    collection: String,
    /// The intercepted write verbatim, live write predicate included. The
    /// Data Plane decides the predicate against the images it computes, where
    /// the writing identity is still available.
    op: KvOp,
}

/// The resolver for `op`, or `None` when it carries no live write predicate.
///
/// Exhaustive over `KvOp`: a new KV op fails to compile here rather than
/// silently skipping resolution.
pub(super) fn resolver_for_kv_op(op: &KvOp) -> Option<Box<dyn EngineWriteResolver>> {
    let collection = match op {
        KvOp::InsertOnConflictUpdate {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Delete {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Expire {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Persist {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::FieldSet {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Incr {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::IncrFloat {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Cas {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::GetSet {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Transfer {
            collection,
            rls_write_check,
            ..
        }
        // A predicate write's row set is known only after committed state is
        // scanned, so it resolves like a state-dependent point write.
        | KvOp::PredicateUpdate {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::PredicateDelete {
            collection,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() {
                return None;
            }
            collection
        }
        // Both collections are checked: an identity may give a row up but not
        // receive it. `sole_rls_write_check` reports `None` here by design
        // and would wave the write through.
        KvOp::TransferItem {
            source_collection,
            source_rls_write_check,
            dest_rls_write_check,
            ..
        } => {
            if !source_rls_write_check.has_predicate() && !dest_rls_write_check.has_predicate() {
                return None;
            }
            source_collection
        }
        // Already decided: `ResolveWrite` is the read-only resolve itself and
        // `ResolvedWrite` carries the verdict. Every remaining op either
        // writes an image the Control Plane already holds, or writes nothing.
        KvOp::Get { .. }
        | KvOp::Put { .. }
        | KvOp::Insert { .. }
        | KvOp::InsertIfAbsent { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::BatchPut { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldGet { .. }
        | KvOp::Truncate { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::ResolveWrite(_)
        | KvOp::ResolvedWrite { .. } => return None,
    };
    Some(Box::new(KvWriteResolver {
        collection: collection.clone(),
        op: op.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for KvWriteResolver {
    fn collection(&self) -> &str {
        &self.collection
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::ResolveWrite(Box::new(self.op.clone())))
    }

    /// A row the Data Plane's write-policy gate refuses surfaces here as
    /// `crate::Error::DataPlane(ErrorCode::RejectedAuthz { .. })` — the exact
    /// error the same op dispatched directly already returns, because the
    /// resolve handler runs the same `admit_kv_row` gate.
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
                        "kv governed write: resolve on '{collection}' returned status {:?} with \
                         no error code",
                        resp.status
                    ),
                },
            });
        }

        let outcome: KvResolveOutcome =
            zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                detail: format!(
                    "kv governed write: could not decode resolved mutations for '{collection}': {e}"
                ),
            })?;
        Ok(ResolvedRows::Kv {
            mutations: outcome.mutations,
            response_payload: outcome.response_payload,
        })
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        match resolved {
            ResolvedRows::Kv {
                mutations,
                response_payload,
            } => Ok(PhysicalPlan::Kv(KvOp::ResolvedWrite {
                mutations,
                response_payload,
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            })),
            ResolvedRows::Update(_) | ResolvedRows::Delete(_) => Err(crate::Error::Internal {
                detail: format!(
                    "kv write resolver for '{}' was handed a columnar row resolution; \
                     resolver_for_plan dispatched the wrong engine",
                    self.collection
                ),
            }),
        }
    }
}
