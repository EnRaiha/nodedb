// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane orchestrator for autocommit `UPDATE ... FROM <source>`.
//!
//! `UPDATE target SET ... FROM source WHERE target.col = source.col` reads the
//! SOURCE collection and updates the TARGET. The source and target collection
//! names hash to independent vShards that, on a multi-core node, can map to
//! DIFFERENT Data-Plane cores. The Data-Plane handler builds its source
//! join-map from the LOCAL core's store, so when the source's vShard lives on
//! another core the handler reads an empty source and silently updates nothing.
//!
//! Unlike `MERGE`, `UPDATE ... FROM` only UPDATES rows that already exist in the
//! target — it never inserts, so it needs no fresh-surrogate assignment and no
//! resolve/apply two-phase round trip. This orchestrator is therefore a single
//! pass:
//!
//! 1. **Source-ship**: scan `source_collection` to completion on its OWN core
//!    via the shared `read_all_source_rows` source-scan primitive (which routes
//!    by the source collection's vShard) and collect the RAW stored rows.
//! 2. **Dispatch**: build the `DocumentOp::UpdateFromJoin` plan with the shipped
//!    rows threaded into `source_rows` and dispatch it to the TARGET's core via
//!    `dispatch_local`. The Data Plane builds the join-map from the shipped rows
//!    instead of a local read, so cross-core `UPDATE ... FROM` is correct.
//!
//! In-transaction `UPDATE ... FROM` never reaches this autocommit orchestrator:
//! it is resolved + staged at STATEMENT time into concrete `PointPut` ops by
//! [`super::expand_staged_update_from_join::resolve_and_emit_update_from_join_ops`]
//! (driven from `control::server::shared::session::expander_stage`), so its
//! writes land in the transaction's overlay immediately (read-your-own-writes)
//! and commit atomically with sibling ops, indexing into every cross-engine
//! index.

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::planner::materialized_sum::{
    resolve_sum_targets_for_bodies, source_drives_bindings,
};
use crate::control::state::SharedState;
use crate::control::update_from_join_orchestrator::expand_staged_update_from_join::decode_resolved_update_rows;
use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget, ReturningSpec, UpdateValue};

/// Attempts an `UPDATE ... FROM` makes before a materialized-sum resolution that
/// keeps drifting is reported rather than retried forever. Mirrors the MERGE
/// orchestrator's bound, and for the same reason: the retry exists to absorb
/// concurrent drift, not to mask a resolution that can never converge.
const MAX_UPDATE_FROM_JOIN_RETRIES: u32 = 8;

/// Bundled arguments for [`run_update_from_join`], mirroring the fields of the
/// intercepted `DocumentOp::UpdateFromJoin` plan.
pub struct UpdateFromJoinArgs<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub updates: &'a [(String, UpdateValue)],
    pub target_filters: &'a [u8],
    pub returning: Option<&'a ReturningSpec>,
    /// Target-collection RLS read filters injected into the intercepted plan.
    /// Carried onto the dispatched plan so the rows a `RETURNING` update shows
    /// are gated exactly as a `SELECT` by the same principal would be.
    pub rls_filters: &'a [u8],
    /// Target-collection RLS write predicate injected into the intercepted
    /// plan. Carried onto the dispatched plan, which decides every matched
    /// row's post-image against it before writing. A separate slot from
    /// `rls_filters`: that one bounds what may be shown back, this one bounds
    /// what may be written.
    pub rls_write_check: &'a [u8],
}

/// Consume an authorized autocommit `UPDATE ... FROM` at orchestration.
pub async fn run_authorized_update_from_join(
    state: &SharedState,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        updates,
        target_filters,
        returning,
        resolve_only: false,
        source_rows: _,
        rls_filters,
        rls_write_check,
        // Unresolved on the way in: the orchestrator resolves the join keys of
        // the matched target rows before it dispatches the write pass.
        resolved_sum_targets: _,
    }) = task.plan
    else {
        return Err(crate::Error::BadRequest {
            detail: "authorized task is not unresolved autocommit UPDATE ... FROM".into(),
        });
    };
    run_update_from_join(
        state,
        UpdateFromJoinArgs {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            target_collection: &target_collection,
            source_collection: &source_collection,
            source_alias: &source_alias,
            target_join_col: &target_join_col,
            source_join_col: &source_join_col,
            updates: &updates,
            target_filters: &target_filters,
            returning: returning.as_ref(),
            rls_filters: &rls_filters,
            rls_write_check: &rls_write_check,
        },
    )
    .await
}

/// Drive an autocommit `UPDATE ... FROM <source>` from the Control Plane.
///
/// Returns the `{"affected": N}` (or RETURNING-rows) response the Data-Plane
/// handler produces, so the dispatch loops render the same command tag as a
/// co-resident single-shard update.
pub(crate) async fn run_update_from_join(
    state: &SharedState,
    args: UpdateFromJoinArgs<'_>,
) -> crate::Result<Response> {
    // Whether the TARGET collection — the one whose rows this statement writes,
    // and therefore the SOURCE of any materialized-sum binding — drives a
    // binding at all. Checked ONCE, before anything else: a target driving
    // nothing skips the RESOLVE round trip and the retry loop entirely, which is
    // the difference between this being free for nearly every collection and
    // being a tax on every `UPDATE ... FROM`.
    let drives_bindings = source_drives_bindings(
        state,
        args.target_collection,
        args.tenant_id,
        args.database_id,
    )?
    .is_some();

    let mut attempt: u32 = 0;
    loop {
        // Read the SOURCE where it lives. Its vShard can map to a DIFFERENT
        // Data-Plane core than the target's, so the target-core dispatch below
        // cannot read the source from local storage. Scan it on its OWN core via
        // the shared source-scan primitive (routes by the source collection's
        // vShard) and ship the RAW stored rows into the plan.
        let source_rows = read_all_source_rows(
            state,
            args.tenant_id,
            args.database_id,
            args.source_collection,
            None,
        )
        .await?;

        let resolved_sum_targets = if drives_bindings {
            match resolve_matched_sum_targets(state, &args, source_rows.clone()).await? {
                Some(resolved) => resolved,
                // The RESOLVE pass failed on the Data Plane; its response is the
                // statement's answer.
                None => {
                    return Err(crate::Error::Dispatch {
                        detail: "UPDATE ... FROM materialized-sum resolve pass failed".into(),
                    });
                }
            }
        } else {
            Vec::new()
        };

        let plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: args.target_collection.to_string(),
            source_collection: args.source_collection.to_string(),
            source_alias: args.source_alias.to_string(),
            target_join_col: args.target_join_col.to_string(),
            source_join_col: args.source_join_col.to_string(),
            updates: args.updates.to_vec(),
            target_filters: args.target_filters.to_vec(),
            returning: args.returning.cloned(),
            resolve_only: false,
            source_rows: Some(source_rows),
            rls_filters: args.rls_filters.to_vec(),
            rls_write_check: args.rls_write_check.to_vec(),
            resolved_sum_targets,
        });

        // Dispatch to the TARGET's core: the join-map is now built from the
        // shipped source rows, so the update lands correctly regardless of where
        // the source collection's vShard lives.
        let resp = dispatch_local(
            state,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            plan,
            None,
        )
        .await?;

        // The write pass re-derived the join-key set of the rows it matched and
        // found the resolution above no longer covers it — a concurrent write
        // moved the match set or a join key between the RESOLVE pass and now. It
        // wrote NOTHING, so re-resolving from a fresh pass and re-dispatching is
        // the whole recovery.
        if resp.error_code.as_deref() == Some(&ErrorCode::OllpRetryRequired) {
            attempt += 1;
            if attempt > MAX_UPDATE_FROM_JOIN_RETRIES {
                return Err(crate::Error::OllpExhausted {
                    retries: MAX_UPDATE_FROM_JOIN_RETRIES.min(u8::MAX as u32) as u8,
                });
            }
            continue;
        }

        // `dispatch_local` bypasses the pgwire autocommit funnel's post-apply
        // redo minting, so an update landing on a vector-indexed target carries
        // its per-row `Put` write-set (surrogate + post-image) back here
        // unconsumed. Mint it now: without this durable redo, a WAL-only restart
        // rebuilds the HNSW from the pre-update `Put` records and resurrects the
        // stale embeddings (`sparse.put` reconciled storage + overlays but minted
        // no WAL redo carrying the new body). Empty on non-vector targets, so
        // this is a no-op there.
        crate::control::server::wal_dispatch::mint_dispatch_local_redo(
            &state.wal,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            &resp,
        )?;

        return Ok(resp);
    }
}

/// Resolve the materialized-sum targets this statement's matched rows need.
///
/// Dispatches the shared read-only RESOLVE pass — the SAME classifier the write
/// pass runs, so the two cannot disagree about which rows match — and resolves
/// the join values of BOTH images of every matched row. Both sides are needed:
/// an assignment that rewrites the join column debits the target the row leaves
/// and credits the one it joins, so resolving post-images alone would leave the
/// abandoned target permanently overstated.
///
/// Predicting from `target_filters` alone would be wrong in the other direction:
/// a target row only matches when its join column names a SOURCE row, so a
/// filter-only prediction resolves rows the statement never touches and fails
/// the statement on any of them that names no target row.
///
/// `None` means the RESOLVE pass itself failed on the Data Plane.
async fn resolve_matched_sum_targets(
    state: &SharedState,
    args: &UpdateFromJoinArgs<'_>,
    source_rows: Vec<(String, Vec<u8>)>,
) -> crate::Result<Option<Vec<ResolvedSumTarget>>> {
    let resolve_plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection: args.target_collection.to_string(),
        source_collection: args.source_collection.to_string(),
        source_alias: args.source_alias.to_string(),
        target_join_col: args.target_join_col.to_string(),
        source_join_col: args.source_join_col.to_string(),
        updates: args.updates.to_vec(),
        target_filters: args.target_filters.to_vec(),
        returning: None,
        resolve_only: true,
        source_rows: Some(source_rows),
        // The RESOLVE pass emits no rows to the client and writes nothing, so
        // neither policy has anything to gate; the write pass below runs both
        // against the rows it actually rewrites.
        rls_filters: Vec::new(),
        rls_write_check: Vec::new(),
        // A read-only pass folds no delta, so it needs no resolution of its own.
        resolved_sum_targets: Vec::new(),
    });
    let resp = dispatch_local(
        state,
        args.tenant_id,
        args.database_id,
        args.target_collection,
        resolve_plan,
        None,
    )
    .await?;
    if resp.status != Status::Ok {
        return Ok(None);
    }

    let arms = decode_resolved_update_rows(&resp.payload)?;
    let bodies: Vec<&[u8]> = arms
        .iter()
        .flat_map(|(_, _, body, old_body)| [body.as_slice(), old_body.as_slice()])
        .collect();
    resolve_sum_targets_for_bodies(
        state,
        &bodies,
        args.target_collection,
        args.tenant_id,
        args.database_id,
        crate::types::TraceId::ZERO,
    )
    .await
    .map(Some)
}
