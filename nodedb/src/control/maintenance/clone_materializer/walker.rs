// SPDX-License-Identifier: BUSL-1.1

//! Clone materializer walker.
//!
//! For every cloned collection in `Shadowed | Materializing { .. }` state,
//! routes to the per-engine row-copy implementation, which copies source
//! rows into target storage and then flips `clone_status` to `Materialized`
//! via the reaper.
//!
//! ## Engine support matrix
//!
//! - **KV** — implemented.
//! - **Document** — implemented.
//! - **Columnar / Timeseries / Spatial** — implemented (all three share the
//!   same columnar materializer path via `ColumnarOp::MaterializeScan`).
//!
//! ## Cluster
//!
//! Source scans run on the source shard's owner, and target writes go
//! through the target shard's replicated write path, so every replica holds
//! the copied rows. Status flips are replicated `PutCollection` entries. The
//! scheduled sweep runs on one node cluster-wide.
//!
//! ## Async end to end
//!
//! Every entry point is async and awaits the per-engine copies, which
//! dispatch through the SPSC bridge. Nothing blocks a runtime thread on a
//! future, so the DDL handlers that await it and the maintenance sweep run
//! on any runtime flavor.

use std::sync::atomic::{AtomicBool, Ordering};

use nodedb_types::{CloneStatus, CollectionType, DatabaseId};

use crate::control::maintenance::wrapper::{MaintenanceOutcome, with_budget_async};
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;

use super::columnar::materialize_columnar_collection;
use super::document::materialize_document_collection;
use super::kv::materialize_kv_collection;
use super::progress::CloneMaterializerHandle;
use super::rls_gate::refuse_if_rls_policy_applies;
use super::source_drain::with_source_drain;

/// Result of a single `materialize_database` call.
#[derive(Debug)]
pub enum MaterializeOutcome {
    /// Every clone collection in the database is `Materialized`.
    AllComplete,
    /// `n` collections still need work and the caller reschedules.
    Incomplete { collections_remaining: usize },
    /// The maintenance budget was exhausted before any work was done.
    BudgetDeferred,
    /// Cooperative shutdown signal received.
    Cancelled,
}

/// Parameters for one materialization sweep over a database.
pub struct MaterializeParams<'a> {
    pub db_id: DatabaseId,
    pub state: &'a SharedState,
    pub catalog: &'a SystemCatalog,
    /// Cooperative cancellation flag set by the shutdown handler.
    pub cancel: &'a AtomicBool,
    /// Optional completion handle; `notify_collection_done()` is called for
    /// each collection that finishes.
    pub handle: Option<&'a CloneMaterializerHandle>,
    /// Estimated seconds passed to `with_budget`. Set high (or 0.0) on the
    /// blocking DDL paths so the budget check always passes; the background
    /// sweep uses a realistic estimate.
    pub estimated_secs: f64,
}

/// Drive one materialization sweep for `db_id`.
pub async fn materialize_database(
    params: MaterializeParams<'_>,
) -> crate::Result<MaterializeOutcome> {
    if params.cancel.load(Ordering::Relaxed) {
        return Ok(MaterializeOutcome::Cancelled);
    }

    let outcome = with_budget_async(
        &params.state.maintenance_budget,
        params.db_id,
        params.estimated_secs,
        do_materialize_database(&params),
    )
    .await;

    match outcome {
        MaintenanceOutcome::Deferred => Ok(MaterializeOutcome::BudgetDeferred),
        MaintenanceOutcome::Ran(inner) => inner,
    }
}

/// Inner sweep, runs inside the budget window.
///
/// Document and columnar sources are read as of the clone point through
/// `system_as_of_ms`, and a KV source is drained cluster-wide in
/// `materialize_one`.
async fn do_materialize_database(
    params: &MaterializeParams<'_>,
) -> crate::Result<MaterializeOutcome> {
    if params.cancel.load(Ordering::Relaxed) {
        return Ok(MaterializeOutcome::Cancelled);
    }

    let pending = pending_clone_collections(params.catalog, params.db_id)?;

    if let Some(h) = params.handle {
        h.notify_start(pending.len());
    }

    if pending.is_empty() {
        return Ok(MaterializeOutcome::AllComplete);
    }

    let mut remaining = 0usize;
    for coll in &pending {
        if params.cancel.load(Ordering::Relaxed) {
            return Ok(MaterializeOutcome::Cancelled);
        }
        match materialize_one(params, coll).await {
            Ok(()) => {
                if let Some(h) = params.handle {
                    h.notify_collection_done();
                }
            }
            Err(e) => {
                // A per-collection failure does not abort the whole sweep —
                // surface it after the loop so partial progress is not lost.
                tracing::warn!(
                    db_id = params.db_id.as_u64(),
                    collection = %coll.name,
                    error = %e,
                    "clone materialize: per-collection error",
                );
                remaining += 1;
                // For unsupported-engine errors, propagate immediately so
                // DDL handlers can map to `0A000`. Other errors continue so
                // the surviving collections still progress.
                if matches!(&e, crate::Error::BadRequest { .. }) {
                    return Err(e);
                }
            }
        }
    }

    if remaining == 0 {
        Ok(MaterializeOutcome::AllComplete)
    } else {
        Ok(MaterializeOutcome::Incomplete {
            collections_remaining: remaining,
        })
    }
}

/// Load all clone collections in `db_id` that still need materialization.
fn pending_clone_collections(
    catalog: &SystemCatalog,
    db_id: DatabaseId,
) -> crate::Result<Vec<StoredCollection>> {
    let all = catalog.load_all_collections(db_id)?;
    Ok(all
        .into_iter()
        .filter(|c| {
            c.cloned_from.is_some()
                && matches!(
                    c.clone_status,
                    CloneStatus::Shadowed | CloneStatus::Materializing { .. }
                )
        })
        .collect())
}

/// Route one collection to its per-engine materializer and await it.
///
/// Every engine's materialization flows through this function, so the RLS
/// policy-existence gate is checked once here, before any scan or write plan
/// is built for any of the four write sites (KV `Put`, Document
/// `PointInsert`, Columnar `Insert`, Timeseries `Ingest`) or their matching
/// source-side `MaterializeScan`s.
async fn materialize_one(
    params: &MaterializeParams<'_>,
    coll: &StoredCollection,
) -> crate::Result<()> {
    refuse_if_rls_policy_applies(params.state, params.db_id, coll)?;

    match &coll.collection_type {
        // KV keeps no row versions, so its source takes no write for the copy.
        CollectionType::KeyValue(_) => {
            with_source_drain(
                params.state,
                coll,
                materialize_kv_collection(params.state, params.catalog, params.db_id, coll),
            )
            .await
        }
        CollectionType::Document(_) => {
            materialize_document_collection(params.state, params.catalog, params.db_id, coll).await
        }
        CollectionType::Columnar(_) => {
            materialize_columnar_collection(params.state, params.catalog, params.db_id, coll).await
        }
    }
}

/// Drive materialization of `db_id` to completion.
///
/// Used by `ALTER DATABASE … MATERIALIZE` and `DROP DATABASE … FORCE`. Returns
/// `Err(Error::BadRequest)` for unsupported engines (mapped to SQLSTATE
/// `0A000` by the DDL handlers); returns `Ok(())` on success or after the
/// budget defers.
pub async fn force_materialize(
    db_id: DatabaseId,
    state: &SharedState,
    catalog: &SystemCatalog,
    handle: Option<&CloneMaterializerHandle>,
) -> crate::Result<()> {
    let cancel = AtomicBool::new(false);
    let params = MaterializeParams {
        db_id,
        state,
        catalog,
        cancel: &cancel,
        handle,
        estimated_secs: 0.0,
    };

    match do_materialize_database(&params).await? {
        MaterializeOutcome::AllComplete => Ok(()),
        MaterializeOutcome::Incomplete {
            collections_remaining,
        } => Err(crate::Error::Storage {
            engine: "clone_materializer".into(),
            detail: format!(
                "{collections_remaining} collection(s) in database {} did not \
                 finish materializing; check logs for per-collection errors",
                db_id.as_u64()
            ),
        }),
        MaterializeOutcome::BudgetDeferred => Ok(()),
        MaterializeOutcome::Cancelled => Ok(()),
    }
}

/// Entry point called by the maintenance scheduler on each tick.
pub async fn run_scheduled_sweep(
    state: &SharedState,
    catalog: &SystemCatalog,
    cancel: &AtomicBool,
) -> crate::Result<()> {
    // Materializer writes route to each shard's owner and replicate, so one
    // node sweeps for the whole cluster.
    if !state.is_singleton_worker() {
        return Ok(());
    }
    // A copy that crashed on any node, or whose clone went away, leaves its
    // source drain to this node. Settled before the sweep re-drives copies.
    super::source_drain::recover_orphaned_source_drains(state).await?;
    let database_ids: Vec<DatabaseId> = catalog
        .list_databases()?
        .into_iter()
        .map(|d| d.id)
        .collect();

    for db_id in database_ids {
        if cancel.load(Ordering::Relaxed) {
            break;
        }

        let params = MaterializeParams {
            db_id,
            state,
            catalog,
            cancel,
            handle: None,
            estimated_secs: 5.0,
        };

        match materialize_database(params).await {
            Ok(MaterializeOutcome::AllComplete) => {}
            Ok(MaterializeOutcome::Incomplete {
                collections_remaining,
            }) => {
                tracing::info!(
                    db_id = db_id.as_u64(),
                    collections_remaining,
                    "clone sweep partial: per-collection errors logged separately",
                );
            }
            Ok(MaterializeOutcome::BudgetDeferred) => {}
            Ok(MaterializeOutcome::Cancelled) => break,
            Err(crate::Error::BadRequest { detail }) => {
                // Unsupported engine — log once and move on so the sweep
                // does not spam every tick.
                tracing::info!(db_id = db_id.as_u64(), %detail, "clone sweep skipped");
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}
