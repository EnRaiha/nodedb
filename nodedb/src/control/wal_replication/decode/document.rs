// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Document`.
//!
//! # The materialized-sum resolution is read off the record, never re-derived
//!
//! An applying node re-EXECUTES the write — the record carries the source row,
//! not a post-image of the derived total — so its own enforcement funnel folds
//! the delta and maintains the target's balance. That fold needs two things the
//! node cannot work out for itself: which target row each join-key value names,
//! and which targets were split onto a sibling `ApplyBalanceDelta` entry. Both
//! were decided once, by the node that accepted the statement, and both travel
//! on the record (see `ReplicatedWrite::PointPut::resolved_sum_targets`).
//!
//! Re-resolving here was the alternative, and it is not open to us. The
//! pk → surrogate binding for a target row lives in the catalog of the vShard
//! that owns that row's primary key — `lookup_surrogate_routed` routes the probe
//! to that vShard's LEADER — so a node replicating only the source's vShard has
//! no local answer, and the remote answer is an async round-trip through another
//! node's committed state taken from inside a synchronous apply loop. Two
//! replicas asking at different instants could get different answers, which is
//! precisely the divergence replication exists to prevent. This is the same
//! contract every other non-derivable value on this wire follows: the leader's
//! surrogate travels beside `pk_bytes` and is `bind`-installed rather than
//! re-allocated, and `KvPut::resolved_now_ms` carries the leader's clock rather
//! than letting each replica read its own.

use super::ctx::{DecodeCtx, bind_or_lookup};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::wal_replication::types::ReplicatedSumTarget;
use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget, UpdateValue};

/// The two slots a record carries its materialized-sum resolution in.
///
/// They travel together because they are one answer in two shapes, and a caller
/// that passed only the older one would silently strip every entry's target
/// collection — which is the ambiguity the newer slot exists to remove.
pub(super) struct WireSumResolution<'a> {
    /// The AUTHORITATIVE slot: `(target collection, join value)` → surrogate.
    pub bindings: &'a [ReplicatedSumTarget],
    /// The superseded `(join value, surrogate)` slot. Read only when `bindings`
    /// is empty — see [`plan_targets`].
    pub legacy: &'a [(String, u32)],
}

/// Lift the wire resolution back into plan shape.
///
/// `bindings` wins whenever it carries anything: a node that wrote it knew each
/// entry's target collection, and that is the key both planes look the
/// resolution up by.
///
/// The older slot is the fallback for one case only — a record committed before
/// that slot existed, which every node replays out of its own log across the
/// upgrade. Its entries name no target collection, so they are lifted
/// UNTARGETED and match any binding by join value alone. That is exactly what
/// the record meant when it was written; inventing a target collection for it
/// would be a resolution nobody made.
///
/// A record that carries both — every record a current node writes — is read
/// from `bindings`, so the fallback never widens a resolution that already knows
/// its targets.
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
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
    }))
}

/// The materialized-sum decisions the proposer made, carried on the record.
///
/// The two travel together because they are one decision per binding: the
/// proposer either resolved the target and folds inline, or deferred it onto a
/// sibling task. Splitting them across parameters lets a caller pass one and
/// forget the other, which is a double-counted or a dropped balance.
pub(super) struct SumDecisions<'a> {
    /// `(target collection, join value)` → target surrogate, resolved by the
    /// node that accepted the statement. Never re-resolved here — see this
    /// module's doc.
    pub resolved: WireSumResolution<'a>,
    /// Bindings whose delta a sibling task owns, so the inline fold skips them.
    pub deferred: &'a [String],
}

pub(super) fn point_insert(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    if_absent: bool,
    surrogate: u32,
    sums: SumDecisions<'_>,
) -> crate::Result<PhysicalPlan> {
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
        // Replay projects nothing back — see `point_delete`.
        returning: None,
        rls_filters: Vec::new(),
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
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
    }))
}

pub(super) fn doc_upsert(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
    surrogate: u32,
    resolved_sum_targets: &WireSumResolution<'_>,
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
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
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
    resolved_sum_targets: &WireSumResolution<'_>,
    deferred_sum_targets: &[String],
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
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
        deferred_sum_targets: deferred_sum_targets.to_vec(),
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
    resolved_sum_targets: &WireSumResolution<'_>,
) -> PhysicalPlan {
    // The MATCHES are re-derived locally (same log position ⇒ same rows); the
    // identity of the targets those matches credit is read off the record — see
    // this module's doc.
    let resolved_sum_targets = plan_targets(resolved_sum_targets);
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
            resolved_sum_targets,
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
            resolved_sum_targets,
        })
    }
}

/// Reconstruct a `Truncate` plan. `DocumentOp::Truncate` is autocommit-only
/// and clearing a collection is idempotent + deterministic, so every replica
/// safely re-executes the same clear on apply. No surrogate binding: there is
/// no per-row identity, just a whole-collection clear.
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

/// Reconstruct an `ApplyBalanceDelta` plan.
///
/// No surrogate BINDING happens here, unlike the point ops: this record
/// addresses a row of the TARGET collection that already exists — the leader
/// resolved it from a join value against a row it had to find — and the
/// surrogate space is Raft-replicated, so the carried identity names the same
/// row on this replica. Binding it to `document_id` would install a
/// primary-key mapping for a key that is not the target row's primary key at
/// all; the document id here IS the hex surrogate.
///
/// Idempotent under exactly-once, LSN-ordered Raft apply for the same reason
/// `KvIncr` is: the entry is applied once per replica, in log order, and the
/// read-modify-write it drives reads the balance this replica has already
/// committed. Re-applying it would double the delta — which is what
/// exactly-once apply exists to prevent, and why the record carries a delta
/// rather than being made idempotent by carrying an absolute total.
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
