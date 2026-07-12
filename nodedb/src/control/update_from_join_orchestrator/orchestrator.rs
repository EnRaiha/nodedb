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
//! In-transaction `UPDATE ... FROM` never reaches here: it is buffered for
//! COMMIT replay. In-transaction `MERGE`, by contrast, is now expanded at
//! COMMIT into concrete point ops by
//! `control::merge_orchestrator::expand_staged_merges`; the analogous
//! COMMIT-time expansion for `UPDATE ... FROM` is not yet wired, so its
//! in-transaction form still replays the buffered `UpdateFromJoin` plan.

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{DocumentOp, ReturningSpec, UpdateValue};

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
}

/// Drive an autocommit `UPDATE ... FROM <source>` from the Control Plane.
///
/// Returns the `{"affected": N}` (or RETURNING-rows) response the Data-Plane
/// handler produces, so the dispatch loops render the same command tag as a
/// co-resident single-shard update.
pub async fn run_update_from_join(
    state: &SharedState,
    args: UpdateFromJoinArgs<'_>,
) -> crate::Result<Response> {
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
    )
    .await?;

    let plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection: args.target_collection.to_string(),
        source_collection: args.source_collection.to_string(),
        source_alias: args.source_alias.to_string(),
        target_join_col: args.target_join_col.to_string(),
        source_join_col: args.source_join_col.to_string(),
        updates: args.updates.to_vec(),
        target_filters: args.target_filters.to_vec(),
        returning: args.returning.cloned(),
        source_rows: Some(source_rows),
    });

    // Dispatch to the TARGET's core: the join-map is now built from the shipped
    // source rows, so the update lands correctly regardless of where the source
    // collection's vShard lives.
    let resp = dispatch_local(
        state,
        args.tenant_id,
        args.database_id,
        args.target_collection,
        plan,
    )
    .await?;

    // `dispatch_local` bypasses the pgwire autocommit funnel's post-apply redo
    // minting, so an update landing on a vector-indexed target carries its
    // per-row `Put` write-set (surrogate + post-image) back here unconsumed.
    // Mint it now: without this durable redo, a WAL-only restart rebuilds the
    // HNSW from the pre-update `Put` records and resurrects the stale
    // embeddings (`sparse.put` reconciled storage + overlays but minted no WAL
    // redo carrying the new body). Empty on non-vector targets, so this is a
    // no-op there.
    crate::control::server::wal_dispatch::mint_dispatch_local_redo(
        &state.wal,
        args.tenant_id,
        args.database_id,
        args.target_collection,
        &resp,
    )?;

    Ok(resp)
}
