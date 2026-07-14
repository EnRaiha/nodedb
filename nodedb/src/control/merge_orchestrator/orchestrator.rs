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
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_plan::document::merge_types::MergeClauseOp;

use super::resolve_arms::decode_resolve;
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
}

/// Drive an autocommit `MERGE` from the Control Plane.
///
/// Returns a `{"affected": N}` response mirroring the shape the Data Plane
/// merge handler produces, so the dispatch loops render the same command tag.
pub async fn run_merge(state: &SharedState, args: MergeArgs<'_>) -> crate::Result<Response> {
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
        let resolve_plan = merge_plan(&args, true, None, Some(source_rows.clone()));
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
        let insert_rows = decode_resolve(&resolve_resp.payload)?.inserts;

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
        let apply_plan = merge_plan(&args, false, Some(resolved), Some(source_rows));
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
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Merge {
        target_collection: args.target_collection.to_string(),
        source_collection: args.source_collection.to_string(),
        source_alias: args.source_alias.to_string(),
        target_join_col: args.target_join_col.to_string(),
        source_join_col: args.source_join_col.to_string(),
        clauses: args.clauses.to_vec(),
        returning: None,
        resolve_only,
        resolved_inserts,
        source_rows,
    })
}
