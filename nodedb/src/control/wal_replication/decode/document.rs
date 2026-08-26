// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Document`.
//!
//! Materialized-sum resolution is read off the record, never re-derived: the
//! pk → surrogate binding needs an async round-trip to another node's leader,
//! and asking twice could get different answers.

use super::ctx::{DecodeCtx, bind_or_lookup};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::wal_replication::types::ReplicatedSumTarget;
use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget, ReturningSpec, UpdateValue};

/// A decoded RETURNING projection spec plus the read filters gating it.
/// Bundled to stay under clippy's arity lint once every point op carries it.
pub(super) struct ReturningFields<'a> {
    pub returning: Option<ReturningSpec>,
    pub rls_filters: &'a [u8],
}

/// The two slots a record carries its materialized-sum resolution in. Travel
/// together — passing only the older one silently strips every entry's target.
pub(super) struct WireSumResolution<'a> {
    /// The AUTHORITATIVE slot: `(target collection, join value)` → surrogate.
    pub bindings: &'a [ReplicatedSumTarget],
    /// The superseded `(join value, surrogate)` slot. Read only when `bindings`
    /// is empty — see [`plan_targets`].
    pub legacy: &'a [(String, u32)],
}

/// Lift the wire resolution back into plan shape. `bindings` wins whenever
/// non-empty; `legacy` entries name no target, so lift untargeted.
fn plan_targets(wire: &WireSumResolution<'_>) -> Vec<ResolvedSumTarget> {
    if !wire.bindings.is_empty() {
        return wire
            .bindings
            .iter()
            .map(|entry| {
                ResolvedSumTarget::new(
                    &entry.target_collection,
                    &entry.join_value,
                    nodedb_types::Surrogate::new(entry.surrogate),
                )
            })
            .collect();
    }
    wire.legacy
        .iter()
        .map(|(join_value, surrogate)| {
            ResolvedSumTarget::untargeted(join_value, nodedb_types::Surrogate::new(*surrogate))
        })
        .collect()
}

pub(super) fn point_put(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    surrogate: u32,
    resolved_sum_targets: &WireSumResolution<'_>,
    returning: ReturningFields<'_>,
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
        // Carried on the record — a replay re-executes for the originating request.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
    }))
}

/// The materialized-sum decisions the proposer made, carried on the record.
/// Travel together: the proposer resolved-and-folds or deferred to a sibling
/// task per binding; splitting these risks a double-counted or dropped balance.
pub(super) struct SumDecisions<'a> {
    /// `(target collection, join value)` → target surrogate, resolved by the
    /// node that accepted the statement. Never re-resolved here — see this
    /// module's doc.
    pub resolved: WireSumResolution<'a>,
    /// Bindings whose delta a sibling task owns, so the inline fold skips them.
    pub deferred: &'a [String],
}

/// `point_insert`'s materialized-sum decisions plus its RETURNING pair,
/// bundled together — plain positional arguments there exceed clippy's arity
/// lint once `returning` joins the signature.
pub(super) struct PointInsertOptions<'a> {
    pub sums: SumDecisions<'a>,
    pub returning: ReturningFields<'a>,
}

pub(super) fn point_insert(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    if_absent: bool,
    surrogate: u32,
    options: PointInsertOptions<'_>,
) -> crate::Result<PhysicalPlan> {
    let PointInsertOptions { sums, returning } = options;
    let SumDecisions {
        resolved: resolved_sum_targets,
        deferred: deferred_sum_targets,
    } = sums;
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
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(&resolved_sum_targets),
        deferred_sum_targets: deferred_sum_targets.to_vec(),
    }))
}

pub(super) fn point_delete(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    surrogate: u32,
    resolved_sum_targets: &WireSumResolution<'_>,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, &pk_bytes, carried)?;
    Ok(PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
        pk_bytes,
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // No predicate: writing identity isn't available on a follower; the
        // leader enforces RLS before proposing.
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
    }))
}

pub(super) fn point_update(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
    surrogate: u32,
    resolved_sum_targets: &WireSumResolution<'_>,
    returning: ReturningFields<'_>,
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
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // No predicate on replay — see `point_delete`.
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
    }))
}

/// `doc_upsert`'s materialized-sum resolution plus its RETURNING pair,
/// bundled together — plain positional arguments there exceed clippy's arity
/// lint once `returning` joins the signature.
pub(super) struct UpsertExtras<'a> {
    pub resolved_sum_targets: &'a WireSumResolution<'a>,
    pub returning: ReturningFields<'a>,
}

pub(super) fn doc_upsert(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
    surrogate: u32,
    extras: UpsertExtras<'_>,
) -> crate::Result<PhysicalPlan> {
    let UpsertExtras {
        resolved_sum_targets,
        returning,
    } = extras;
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, &pk_bytes, carried)?;
    Ok(PhysicalPlan::Document(DocumentOp::Upsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        on_conflict_updates: on_conflict_updates.to_vec(),
        surrogate,
        // No predicate on replay — see `point_delete`.
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
    }))
}

/// Reconstruct a `BatchInsert` plan, binding each row's carried surrogate to
/// its `document_id` on this replica (mirrors `kv::batch_put`). Idempotent
/// under exactly-once, LSN-ordered Raft apply.
pub(super) fn batch_insert(
    ctx: &DecodeCtx,
    collection: &str,
    documents: &[(String, Vec<u8>)],
    surrogates: &[u32],
    resolved_sum_targets: &WireSumResolution<'_>,
    deferred_sum_targets: &[String],
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    // `zip` stops at the shorter side, silently truncating rows with no identity.
    // Refuse here, where the discrepancy is still visibly a malformed record.
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
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
        deferred_sum_targets: deferred_sum_targets.to_vec(),
    }))
}

/// Reconstruct the bulk plan in its plain (non-OLLP) form. `ollp_predicted_surrogates
/// = None` selects the local-scan path — deterministic since Raft log order gives
/// identical prior state across replicas. No surrogate binding needed.
pub(super) fn bulk_dml(
    collection: &str,
    filters: &[u8],
    is_update: bool,
    updates: &[(String, UpdateValue)],
    resolved_sum_targets: &WireSumResolution<'_>,
    returning: ReturningFields<'_>,
) -> PhysicalPlan {
    // Matches are re-derived locally; target identity is read off the record.
    let resolved_sum_targets = plan_targets(resolved_sum_targets);
    if is_update {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: collection.to_owned(),
            filters: filters.to_vec(),
            updates: updates.to_vec(),
            // Carried on the record — see `point_put`.
            returning: returning.returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: returning.rls_filters.to_vec(),
            // No predicate on replay — see `point_delete`.
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            resolved_sum_targets,
        })
    } else {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: collection.to_owned(),
            filters: filters.to_vec(),
            // Carried on the record — see `point_put`.
            returning: returning.returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: returning.rls_filters.to_vec(),
            // No predicate on replay — see `point_delete`.
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            resolved_sum_targets,
        })
    }
}

/// Reconstruct a `Truncate` plan. Autocommit-only, idempotent, deterministic —
/// no surrogate binding since there's no per-row identity.
pub(super) fn truncate(
    collection: &str,
    restart_identity: bool,
    resolved_sum_targets: &WireSumResolution<'_>,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Truncate {
        collection: collection.to_owned(),
        restart_identity,
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
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

/// Reconstruct an `ApplyBalanceDelta` plan. No surrogate binding: the document
/// id here IS the hex surrogate, not a primary key. Idempotent like `KvIncr`.
pub(super) fn apply_balance_delta(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    column: &str,
    delta: &str,
    join_column: &str,
    join_value: &str,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate: nodedb_types::Surrogate::new(surrogate),
        column: column.to_owned(),
        delta: delta.to_owned(),
        join_column: join_column.to_owned(),
        join_value: join_value.to_owned(),
    })
}

/// Reconstruct a resolved document write plan (`DocumentOp::ResolvedWrite`).
/// Every mutation's surrogate binds via the assigner against its own
/// `(collection, primary key)`; everything else travels on the record.
pub(super) fn resolved_write(
    ctx: &DecodeCtx,
    mutations: &[super::super::types::DocumentResolvedMutationWire],
    response_payload: &[u8],
) -> crate::Result<PhysicalPlan> {
    use super::super::types::DocumentResolvedMutationWire as W;
    use nodedb_physical::physical_plan::DocumentResolvedMutation as M;

    let decoded = mutations
        .iter()
        .map(|m| -> crate::Result<M> {
            Ok(match m {
                W::Put {
                    collection,
                    document_id,
                    surrogate,
                    value,
                    precondition,
                    resolved_sum_targets,
                } => {
                    let pk_bytes = document_id.as_bytes().to_vec();
                    let carried = nodedb_types::Surrogate::new(*surrogate);
                    M::Put {
                        surrogate: bind_or_lookup(ctx, collection, &pk_bytes, carried)?,
                        collection: collection.clone(),
                        document_id: document_id.clone(),
                        pk_bytes,
                        value: value.clone(),
                        precondition: precondition.clone(),
                        resolved_sum_targets: plan_targets(&WireSumResolution {
                            bindings: resolved_sum_targets,
                            legacy: &[],
                        }),
                    }
                }
                W::Delete {
                    collection,
                    document_id,
                    surrogate,
                    precondition,
                    resolved_sum_targets,
                } => {
                    let pk_bytes = document_id.as_bytes().to_vec();
                    let carried = nodedb_types::Surrogate::new(*surrogate);
                    M::Delete {
                        surrogate: bind_or_lookup(ctx, collection, &pk_bytes, carried)?,
                        collection: collection.clone(),
                        document_id: document_id.clone(),
                        pk_bytes,
                        precondition: precondition.clone(),
                        resolved_sum_targets: plan_targets(&WireSumResolution {
                            bindings: resolved_sum_targets,
                            legacy: &[],
                        }),
                    }
                }
            })
        })
        .collect::<crate::Result<Vec<M>>>()?;

    Ok(PhysicalPlan::Document(DocumentOp::ResolvedWrite {
        mutations: decoded,
        response_payload: response_payload.to_vec(),
        // Decided before this entry was proposed; the record proves it.
        rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
    }))
}
