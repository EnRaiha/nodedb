// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane orchestrator for autocommit `MERGE`.
//!
//! `MERGE ... WHEN NOT MATCHED THEN INSERT` inserts brand-new rows into the
//! target. Every such row must receive its OWN globally-unique surrogate,
//! registered in the catalog so cross-engine search (vector / FTS / spatial)
//! can resolve a hit back to the target row's identity. Surrogate registration
//! is Control-Plane-only (WAL-durable, under the registry lock) and the Data
//! Plane never touches the catalog, so autocommit MERGE runs as a
//! Control-Plane-driven, TOCTOU-safe, atomic round trip:
//!
//! 0. **Source-ship**: the source collection's vShard can map to a DIFFERENT
//!    Data-Plane core than the target's, so the resolve/apply dispatches (which
//!    target the target core) cannot read the source from local storage. The
//!    Control Plane scans the source on its OWN core via the shared
//!    `MaterializeScan` primitive and ships the RAW stored rows into the plan's
//!    `source_rows`; the Data Plane builds the join-map from these instead of a
//!    local read. This is what makes cross-core MERGE correct.
//! 1. **Resolve** (`DocumentOp::Merge { resolve_only: true }`): the Data Plane
//!    classifies the merge against a point-in-time snapshot and returns the
//!    NOT-MATCHED insert rows as `Vec<(join_key, body)>` WITHOUT writing.
//! 2. **Assign**: for each insert row, allocate a fresh, registered surrogate
//!    keyed on the target collection's primary key exactly as a plain `INSERT`
//!    would (`assign` for a declared PK, `assign_fresh` for an auto-`_rowid`
//!    target). The source surrogate is never inherited.
//! 3. **Apply** (`DocumentOp::Merge { resolved_inserts: Some(..) }`): the Data
//!    Plane re-derives the classification, VERIFIES the recomputed insert-key
//!    set still equals the assigned keys — returning `OllpRetryRequired`
//!    WITHOUT writing on drift — and applies every arm's writes with the
//!    pre-assigned surrogates. The matched UPDATE and NOT-MATCHED INSERT arms
//!    share one redb transaction (all-or-nothing).
//!
//! ## TOCTOU
//!
//! The resolve (phase 1) and apply (phase 3) are distinct snapshots separated
//! by the surrogate-assignment round trip. A concurrent write to source/target
//! between them is caught by the apply-time verification, which returns
//! `ErrorCode::OllpRetryRequired`; this loop then re-resolves (fresh phase 1)
//! and retries — the same predict-verify-retry contract the OLLP dependent-read
//! path uses. Retries are bounded; exhaustion surfaces `OllpExhausted`.

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::document::merge_types::MergeClauseOp;
use nodedb_physical::physical_plan::{DocumentOp, ReturningSpec};

use super::resolve_arms::decode_resolve;
use crate::control::planner::materialized_sum::resolve_sum_targets_for_bodies;
use crate::control::target_identity::{
    assign_target_surrogate, bare_collection_name, resolve_target_pk,
};

/// Upper bound on resolve→apply retries under concurrent source/target drift.
/// Mirrors the OLLP dependent-read retry ceiling: a merge whose matched /
/// not-matched classification keeps changing every attempt is surfaced as
/// `OllpExhausted` rather than looping forever.
const MAX_MERGE_RETRIES: u32 = 10;

/// Bundled arguments for [`run_merge`], mirroring the fields of the intercepted
/// `DocumentOp::Merge` plan.
pub struct MergeArgs<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub clauses: &'a [MergeClauseOp],
    /// Projection for a `MERGE ... RETURNING`, attached by the RETURNING
    /// pre-processor. `None` selects the affected-count response.
    pub returning: Option<&'a ReturningSpec>,
    /// Target-collection RLS read filters injected into the intercepted plan.
    /// Carried onto the apply pass so the rows a `RETURNING` merge shows are
    /// gated exactly as a `SELECT` by the same principal would be.
    pub rls_filters: &'a [u8],
    /// Target-collection RLS write predicate injected into the intercepted
    /// plan. Carried onto the apply pass, which decides every arm's row image
    /// against it before writing. A separate slot from `rls_filters`: that one
    /// bounds what may be shown back, this one bounds what may be written.
    pub rls_write_check: &'a [u8],
}

/// Consume an authorized autocommit `MERGE` at the orchestration boundary.
pub async fn run_authorized_merge(
    state: &SharedState,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    let PhysicalPlan::Document(DocumentOp::Merge {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        clauses,
        resolve_only: false,
        resolved_inserts: None,
        source_rows: _,
        returning,
        rls_filters,
        rls_write_check,
        // Unresolved on the way in: the orchestrator's own RESOLVE pass is what
        // produces the join keys this is filled from.
        resolved_sum_targets: _,
    }) = task.plan
    else {
        return Err(crate::Error::BadRequest {
            detail: "authorized task is not an unresolved autocommit MERGE".into(),
        });
    };
    run_merge(
        state,
        MergeArgs {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            target_collection: &target_collection,
            source_collection: &source_collection,
            source_alias: &source_alias,
            target_join_col: &target_join_col,
            source_join_col: &source_join_col,
            clauses: &clauses,
            returning: returning.as_ref(),
            rls_filters: &rls_filters,
            rls_write_check: &rls_write_check,
        },
    )
    .await
}

/// Drive an autocommit `MERGE` from the Control Plane.
///
/// Returns the `{"affected": N}` (or RETURNING-rows) response the Data-Plane
/// merge handler produces, so the dispatch loops render the same command tag.
pub(crate) async fn run_merge(state: &SharedState, args: MergeArgs<'_>) -> crate::Result<Response> {
    let catalog = state.credentials.catalog();
    let target_bare = bare_collection_name(args.database_id, args.target_collection);
    let target = catalog
        .get_collection(args.database_id, args.tenant_id.as_u64(), &target_bare)?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id: args.tenant_id,
            collection: args.target_collection.to_string(),
        })?;
    let target_pk = resolve_target_pk(&target, "MERGE")?;

    let mut attempt: u32 = 0;
    loop {
        // Phase 0: read the SOURCE where it lives. The source collection's
        // vShard can map to a DIFFERENT Data-Plane core than the target's, so
        // the resolve/apply dispatches (which target the target core) cannot
        // read it from local storage. Scan it on its OWN core via the shared
        // source-scan primitive (which routes by the source collection's
        // vShard) and ship the RAW stored rows into the plan. A fresh read per
        // attempt keeps each attempt's resolve and apply on one consistent
        // source snapshot; a retry picks up concurrent source mutation.
        let source_rows = read_all_source_rows(
            state,
            args.tenant_id,
            args.database_id,
            args.source_collection,
            None,
        )
        .await?;

        // Phase 1: resolve the NOT-MATCHED insert rows (read-only snapshot).
        let resolve_plan = merge_plan(&args, true, None, Some(source_rows.clone()), Vec::new());
        let resolve_resp = dispatch_local(
            state,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            resolve_plan,
            None,
        )
        .await?;
        if resolve_resp.status != Status::Ok {
            return Ok(resolve_resp);
        }
        let arms = decode_resolve(&resolve_resp.payload)?;

        // Phase 2a: resolve this merge's materialized-sum targets from the
        // arms the RESOLVE pass just classified. Every arm moves a total — an
        // INSERT credits, a DELETE debits, an UPDATE applies the difference and,
        // when it rewrites the join key, both sides — so the pre- AND
        // post-images of every arm contribute a join key. Resolution is by
        // LOOKUP only: a join value naming no existing target row fails the
        // statement rather than minting identity for a row that does not exist.
        //
        // Drift between this classification and the apply is caught by the
        // apply's own insert-key verification, which returns
        // `OllpRetryRequired` before writing and sends this loop round again
        // with a fresh classification — the same guard the surrogates rely on.
        let sum_bodies: Vec<&[u8]> = arms
            .updates
            .iter()
            .flat_map(|(_, _, body, old_body)| [body.as_slice(), old_body.as_slice()])
            .chain(arms.deletes.iter().map(|(_, _, body)| body.as_slice()))
            .chain(arms.inserts.iter().map(|(_, body)| body.as_slice()))
            .collect();
        let resolved_sum_targets = resolve_sum_targets_for_bodies(
            state,
            &sum_bodies,
            args.target_collection,
            args.tenant_id,
            args.database_id,
            crate::types::TraceId::ZERO,
        )
        .await?;

        let insert_rows = arms.inserts;

        // Phase 2: assign a fresh, registered surrogate per inserted row.
        let mut resolved: Vec<(String, u32)> = Vec::with_capacity(insert_rows.len());
        for (join_key, body) in &insert_rows {
            let surrogate = assign_target_surrogate(
                state,
                args.database_id,
                args.tenant_id,
                args.target_collection,
                &target_pk,
                body,
            )?;
            resolved.push((join_key.clone(), surrogate.as_u32()));
        }

        // Phase 3: atomic apply with the pre-assigned surrogates + drift verify.
        // The apply reuses THIS attempt's source snapshot so the DP re-derives
        // the classification from the same source the resolve saw.
        let apply_plan = merge_plan(
            &args,
            false,
            Some(resolved),
            Some(source_rows),
            resolved_sum_targets,
        );
        let apply_resp = dispatch_local(
            state,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            apply_plan,
            None,
        )
        .await?;

        if apply_resp.error_code.as_deref() == Some(&ErrorCode::OllpRetryRequired) {
            attempt += 1;
            if attempt > MAX_MERGE_RETRIES {
                return Err(crate::Error::OllpExhausted {
                    retries: MAX_MERGE_RETRIES.min(u8::MAX as u32) as u8,
                });
            }
            // Concurrent drift: re-resolve (fresh phase 1) and retry. The
            // surrogates assigned this round are simply unused (harmless —
            // the counter is monotonic and gap-tolerant).
            continue;
        }

        // `dispatch_local` bypasses the pgwire autocommit funnel's post-apply
        // redo minting, so a MERGE landing on a vector-indexed target carries
        // its per-row Put/Delete write-set back here unconsumed. Mint it now —
        // without this durable redo a WAL-only restart rebuilds the HNSW from
        // the pre-merge Put records (apply_point_put/apply_point_delete
        // reconciled storage + overlays but minted no WAL redo carrying the new
        // bodies). No-op on non-vector targets.
        crate::control::server::wal_dispatch::mint_dispatch_local_redo(
            &state.wal,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            &apply_resp,
        )?;
        return Ok(apply_resp);
    }
}

/// Build a `DocumentOp::Merge` physical plan for one orchestrator pass.
///
/// `source_rows` carries the RAW stored source rows scanned on the source's own
/// core (phase 0) so the Data Plane builds the join-map from the shipped bytes
/// rather than reading the source from the target core's local store.
fn merge_plan(
    args: &MergeArgs<'_>,
    resolve_only: bool,
    resolved_inserts: Option<Vec<(String, u32)>>,
    source_rows: Option<Vec<(String, Vec<u8>)>>,
    resolved_sum_targets: Vec<nodedb_physical::physical_plan::ResolvedSumTarget>,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Merge {
        target_collection: args.target_collection.to_string(),
        source_collection: args.source_collection.to_string(),
        source_alias: args.source_alias.to_string(),
        target_join_col: args.target_join_col.to_string(),
        source_join_col: args.source_join_col.to_string(),
        clauses: args.clauses.to_vec(),
        // Only the APPLY pass can project rows. The RESOLVE pass is a read-only
        // classification whose payload is the `(updates, deletes, inserts)`
        // tuple `decode_resolve` expects; emitting RETURNING rows there would
        // replace that payload and strand the surrogate assignment.
        returning: if resolve_only {
            None
        } else {
            args.returning.cloned()
        },
        resolve_only,
        resolved_inserts,
        source_rows,
        rls_filters: args.rls_filters.to_vec(),
        // Carried on both passes. The RESOLVE pass writes nothing, so the check
        // is inert there; carrying it unconditionally keeps the two passes
        // byte-identical apart from the fields that must differ, so a future
        // writing resolve cannot silently lose the gate.
        rls_write_check: args.rls_write_check.to_vec(),
        // Empty on the RESOLVE pass — it writes nothing, so it folds no delta.
        // The APPLY pass carries the resolution derived from that pass's arms.
        resolved_sum_targets,
    })
}
