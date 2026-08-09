// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane resolution of materialized-sum targets for the PREDICATE-driven
//! write plans.
//!
//! `BulkUpdate`, `BulkDelete` and `TRUNCATE` name their rows by predicate. The
//! rows are matched in the Data Plane, so at plan time there is no body to read
//! a join key off — which is why the body-driven pass
//! ([`super::resolve::resolve_materialized_sum_targets`]) deliberately skips
//! them. They are resolved here instead, from a reconnaissance scan of the SAME
//! predicate, exactly as the OLLP dependent-predicate path predicts its write
//! set before execution.
//!
//! # The gate comes first
//!
//! A collection driving no materialized-sum binding must not pay for a recon
//! scan — that scan is the entire cost of this path, and nearly every collection
//! drives nothing. [`super::resolve::source_drives_bindings`] is therefore
//! checked BEFORE the scan is issued: a collection with no binding costs one
//! cached index probe and nothing else.
//!
//! # The prediction is verified before it is written
//!
//! The scan happens before execution, so the matched set can move underneath it.
//! The Data-Plane leader recomputes the join-key set from the rows it actually
//! matched and returns `ErrorCode::OllpRetryRequired` — before writing anything —
//! when the resolution carried here does not cover it. Nothing is written on a
//! divergence, which is the whole point: a divergence written silently is a
//! stored total that disagrees with the `SUM(...)` over the source rows.

use std::sync::Arc;

use nodedb_physical::physical_plan::{DocumentOp, MaterializedSumBinding, UpdateValue};
use nodedb_types::Surrogate;

use super::recon::recon_scan_rows;
use super::resolve::{lookup_join_value, source_drives_bindings};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// Everything the recon scan and the fold need from a predicate-driven plan.
struct PredicateScope {
    /// Source collection as it appears on the plan (db-qualified).
    collection: String,
    /// Serialized `Vec<ScanFilter>`; empty means "every row".
    filters: Vec<u8>,
    /// The statement's `SET` assignments, so an update that rewrites a join
    /// column resolves the target it moves rows ONTO as well as the one it
    /// moves them off. Empty for the delete-shaped plans.
    updates: Vec<(String, UpdateValue)>,
}

/// Resolve `op`'s materialized-sum targets when it is a predicate-driven write.
///
/// Returns `Ok(true)` when `op` is one of those plans — whether or not its
/// collection drives a binding — so the caller knows the op is accounted for and
/// does not also run the body-driven pass over it. `Ok(false)` means `op` is not
/// predicate-driven and the caller still owns it.
pub(super) async fn resolve_predicate_sum_targets(
    state: &SharedState,
    op: &mut DocumentOp,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<bool> {
    let Some(scope) = predicate_scope(op) else {
        return Ok(false);
    };
    // The gate, before any scan: a collection driving nothing pays nothing.
    let Some(bindings) = source_drives_bindings(state, &scope.collection, tenant_id, database_id)?
    else {
        return Ok(true);
    };

    let rows = recon_scan_rows(
        state,
        tenant_id,
        database_id,
        &scope.collection,
        scope.filters,
    )
    .await?;
    let resolved = resolve_scanned_rows(
        state,
        &bindings,
        &scope.updates,
        &rows,
        tenant_id,
        database_id,
        trace_id,
    )
    .await?;
    set_predicate_resolution(op, resolved);
    Ok(true)
}

/// Resolve every join value the scanned rows need into its target row's
/// surrogate.
///
/// One entry per DISTINCT join value across every binding, mirroring the
/// body-driven resolution: a predicate matching many rows against one target
/// resolves that target once.
async fn resolve_scanned_rows(
    state: &SharedState,
    bindings: &Arc<Vec<MaterializedSumBinding>>,
    updates: &[(String, UpdateValue)],
    rows: &[serde_json::Value],
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<Vec<(String, Surrogate)>> {
    let mut resolved: Vec<(String, Surrogate)> = Vec::new();
    for binding in bindings.iter() {
        for join_value in crate::query::binding_join_keys(binding, updates, rows)? {
            if resolved.iter().any(|(value, _)| *value == join_value) {
                continue;
            }
            let surrogate = lookup_join_value(
                state,
                binding,
                &join_value,
                tenant_id,
                database_id,
                trace_id,
            )
            .await?;
            resolved.push((join_value, surrogate));
        }
    }
    Ok(resolved)
}

/// The scan inputs of a predicate-driven write, or `None` for every other op.
///
/// Exhaustive so a new `DocumentOp` variant must state whether it names its rows
/// by predicate. `UpdateFromJoin` is deliberately absent: which target rows it
/// matches depends on the SOURCE collection's rows, which are only shipped by
/// its Control-Plane orchestrator — so it resolves there, from the RESOLVE
/// pass's own classification, rather than from a predicate-only scan that would
/// over-approximate the match set.
fn predicate_scope(op: &DocumentOp) -> Option<PredicateScope> {
    match op {
        DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            ..
        } => Some(PredicateScope {
            collection: collection.clone(),
            filters: filters.clone(),
            updates: updates.clone(),
        }),
        DocumentOp::BulkDelete {
            collection,
            filters,
            ..
        } => Some(PredicateScope {
            collection: collection.clone(),
            filters: filters.clone(),
            updates: Vec::new(),
        }),
        // TRUNCATE removes every row, so it carries no filter — the empty
        // filter set is exactly "every row" to the recon scan.
        DocumentOp::Truncate { collection, .. } => Some(PredicateScope {
            collection: collection.clone(),
            filters: Vec::new(),
            updates: Vec::new(),
        }),
        DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointPut { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        // A derived balance write names its target row directly; it is not a
        // predicate-driven statement and drives no binding of its own.
        | DocumentOp::ApplyBalanceDelta { .. } => None,
    }
}

/// Write the resolution into the op's slot. Exhaustive for the same reason
/// [`predicate_scope`] is.
fn set_predicate_resolution(op: &mut DocumentOp, resolved: Vec<(String, Surrogate)>) {
    match op {
        DocumentOp::BulkUpdate {
            resolved_sum_targets,
            ..
        }
        | DocumentOp::BulkDelete {
            resolved_sum_targets,
            ..
        }
        | DocumentOp::Truncate {
            resolved_sum_targets,
            ..
        } => *resolved_sum_targets = resolved,
        DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointPut { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => {}
    }
}
