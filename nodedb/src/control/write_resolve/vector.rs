// SPDX-License-Identifier: BUSL-1.1

//! Vector-primary implementation of [`EngineWriteResolver`].
//!
//! Resolves a vector-primary write into `VectorOp::ResolvedDirectWrite`: a
//! row-mutation list plus the response payload, each mutation carrying the
//! sidecar it read so the apply refuses a resolution that drifted.
//!
//! A write resolves when it carries a live write predicate, a row predicate,
//! or a patch expression, and a multi-row page always resolves. The sidecar store is written through while the
//! index restores from its checkpoint, so replay runs against newer sidecars.
//! A log record therefore names rows and their values, never something
//! replay evaluates again.

use async_trait::async_trait;
use nodedb_types::{QualifiedCollection, RlsWriteCheck};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{
    UpdateValue, VectorOp, VectorResolveOutcome, VectorWriteTargets,
};

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// Index settings the resolved write carries so an apply can create the
/// index a first `Upsert` lands in. A `DirectDelete` never creates one.
#[derive(Default)]
struct ResolvedIndexSettings {
    quantization: nodedb_types::VectorQuantization,
    storage_dtype: nodedb_types::VectorStorageDtype,
    payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
}

/// A governed vector-primary write, extracted at interception.
pub struct VectorWriteResolver {
    /// Routing collection — also the vshard key.
    collection: QualifiedCollection,
    /// Vector column name; keys the HNSW index.
    field: String,
    settings: ResolvedIndexSettings,
    /// The intercepted write verbatim, live write predicate included. The
    /// Data Plane decides the predicate against the images it computes,
    /// where the writing identity is still available.
    op: VectorOp,
}

/// Whether `targets` names rows by a predicate replay would evaluate again.
fn by_predicate(targets: &VectorWriteTargets) -> bool {
    match targets {
        VectorWriteTargets::Predicate(_) => true,
        VectorWriteTargets::Surrogates(_) => false,
    }
}

/// Whether `patch` holds an expression replay would evaluate again.
fn has_expression(patch: &[(String, UpdateValue)]) -> bool {
    patch.iter().any(|(_, value)| match value {
        UpdateValue::Expr(_) => true,
        UpdateValue::Literal(_) => false,
    })
}

/// The resolver for `op`, or `None` when the op logs as it is. Exhaustive
/// over `VectorOp` — a new op fails to compile here.
pub(super) fn resolver_for_vector_op(op: &VectorOp) -> Option<Box<dyn EngineWriteResolver>> {
    let (collection, field, settings) = match op {
        VectorOp::DirectDelete {
            collection,
            field,
            targets,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() && !by_predicate(targets) {
                return None;
            }
            (collection, field, ResolvedIndexSettings::default())
        }
        VectorOp::DirectUpdate {
            collection,
            field,
            targets,
            payload_patch,
            quantization,
            storage_dtype,
            payload_indexes,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate()
                && !by_predicate(targets)
                && !has_expression(payload_patch)
            {
                return None;
            }
            (
                collection,
                field,
                ResolvedIndexSettings {
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    payload_indexes: payload_indexes.clone(),
                },
            )
        }
        // A whole-row upsert is decided Control-Plane-side against its
        // payload. A conflict patch is decided against the stored row.
        VectorOp::DirectUpsert {
            collection,
            field,
            on_conflict_updates,
            quantization,
            storage_dtype,
            payload_indexes,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() && !has_expression(on_conflict_updates) {
                return None;
            }
            (
                collection,
                field,
                ResolvedIndexSettings {
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    payload_indexes: payload_indexes.clone(),
                },
            )
        }
        // A page always resolves: whether a row lands depends on the rows
        // stored and on the page's earlier rows, and the record names only
        // the rows the page stores.
        VectorOp::DirectBatchWrite {
            collection,
            field,
            quantization,
            storage_dtype,
            payload_indexes,
            ..
        } => (
            collection,
            field,
            ResolvedIndexSettings {
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes: payload_indexes.clone(),
            },
        ),
        // Decided Control-Plane-side against the payload image.
        VectorOp::DirectInsert { .. }
        | VectorOp::DirectInsertIfAbsent { .. }
        // Refused at injection under a write policy; carries no predicate.
        | VectorOp::DirectTruncate { .. }
        // Already decided, or reads.
        | VectorOp::ResolveDirectWrite(_)
        | VectorOp::ResolvedDirectWrite { .. }
        | VectorOp::Search { .. }
        | VectorOp::Insert { .. }
        | VectorOp::BatchInsert { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::MultiVectorScoreSearch { .. } => return None,
    };
    Some(Box::new(VectorWriteResolver {
        collection: collection.clone(),
        field: field.clone(),
        settings,
        op: op.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for VectorWriteResolver {
    fn collection(&self) -> &str {
        self.collection.as_str()
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::ResolveDirectWrite(Box::new(self.op.clone())))
    }

    /// A refused row surfaces as `DataPlane(RejectedAuthz)`, same as the op
    /// dispatched directly — the resolve handler runs the same gate.
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows> {
        let collection = self.collection.as_str();
        let resp =
            dispatch_local(state, ctx.tenant_id, ctx.database_id, collection, op, None).await?;
        if resp.status != Status::Ok {
            return Err(match resp.error_code {
                Some(code) => crate::Error::DataPlane(*code),
                None => crate::Error::Dispatch {
                    detail: format!(
                        "vector governed write: resolve on '{collection}' returned status {:?} \
                         with no error code",
                        resp.status
                    ),
                },
            });
        }

        let outcome: VectorResolveOutcome =
            zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                detail: format!(
                    "vector governed write: could not decode resolved mutations for \
                     '{collection}': {e}"
                ),
            })?;
        Ok(ResolvedRows::Vector {
            mutations: outcome.mutations,
            response_payload: outcome.response_payload,
        })
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        match resolved {
            ResolvedRows::Vector {
                mutations,
                response_payload,
            } => Ok(PhysicalPlan::Vector(VectorOp::ResolvedDirectWrite {
                collection: self.collection.clone(),
                field: self.field.clone(),
                quantization: self.settings.quantization,
                storage_dtype: self.settings.storage_dtype,
                payload_indexes: self.settings.payload_indexes.clone(),
                mutations,
                response_payload,
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            })),
            ResolvedRows::Update(_)
            | ResolvedRows::Delete(_)
            | ResolvedRows::Kv { .. }
            | ResolvedRows::Document { .. }
            | ResolvedRows::Timeseries { .. }
            | ResolvedRows::GraphEdgeDeleteAdmitted => Err(crate::Error::Internal {
                detail: format!(
                    "vector write resolver for '{}' was handed another engine's resolution; \
                     resolver_for_plan dispatched the wrong engine",
                    self.collection.as_str()
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{UpdateValue, VectorOp, VectorWriteTargets};
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate};

    use super::resolver_for_vector_op;
    use crate::bridge::envelope::PhysicalPlan;

    fn delete(targets: VectorWriteTargets, check: RlsWriteCheck) -> VectorOp {
        VectorOp::DirectDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field: "emb".into(),
            targets,
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: check,
        }
    }

    fn update(targets: VectorWriteTargets, value: UpdateValue) -> VectorOp {
        VectorOp::DirectUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field: "emb".into(),
            targets,
            new_vector: None,
            payload_patch: vec![("n".into(), value)],
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        }
    }

    fn rows() -> VectorWriteTargets {
        VectorWriteTargets::Surrogates(vec![Surrogate::new(4)])
    }

    /// A write that names its rows and values logs as it is.
    #[test]
    fn a_write_naming_rows_and_values_logs_as_it_is() {
        for check in [
            RlsWriteCheck::NoPolicyApplies,
            RlsWriteCheck::decided_earlier_in_request(),
        ] {
            assert!(resolver_for_vector_op(&delete(rows(), check)).is_none());
        }
        let literal = UpdateValue::Literal(vec![0x01]);
        assert!(resolver_for_vector_op(&update(rows(), literal)).is_none());
    }

    /// A multi-row page resolves with no predicate and no expression: the
    /// rows it stores depend on the rows already stored.
    #[test]
    fn a_page_always_resolves() {
        let page = VectorOp::DirectBatchWrite {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field: "emb".into(),
            intent: nodedb_physical::physical_plan::VectorDirectWriteIntent::InsertIfAbsent,
            rows: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        };
        let resolver = resolver_for_vector_op(&page).expect("a page resolves before logging");
        assert_eq!(resolver.collection(), "vecs");
    }

    /// A live write predicate, a row predicate, or a patch expression is
    /// resolved to rows and values before it is logged.
    #[test]
    fn anything_replay_evaluates_again_selects_the_resolver() {
        let governed = delete(rows(), RlsWriteCheck::from_injected(vec![1, 2, 3]));
        let by_predicate = delete(
            VectorWriteTargets::Predicate(Vec::new()),
            RlsWriteCheck::NoPolicyApplies,
        );
        let literal = UpdateValue::Literal(vec![0x01]);
        let update_by_predicate = update(VectorWriteTargets::Predicate(Vec::new()), literal);
        let expression = UpdateValue::Expr(nodedb_query::expr::SqlExpr::Column("n".into()));
        let update_by_expression = update(rows(), expression);
        for op in [
            governed,
            by_predicate,
            update_by_predicate,
            update_by_expression,
        ] {
            let resolver = resolver_for_vector_op(&op).expect("the op resolves before logging");
            assert_eq!(resolver.collection(), "vecs");
            assert!(matches!(
                resolver.build_resolve_op(),
                PhysicalPlan::Vector(VectorOp::ResolveDirectWrite(_))
            ));
        }
    }
}
