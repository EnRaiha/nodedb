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
//! ## Sync wrapper
//!
//! The public API is sync because it is invoked from `spawn_blocking` on
//! both the DDL hot path and the maintenance scheduler. Internally we call
//! [`tokio::runtime::Handle::block_on`] so the per-engine async helpers can
//! use the SPSC bridge.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};

use nodedb_types::{CloneStatus, CollectionType, DatabaseId};

use crate::control::maintenance::wrapper::{MaintenanceOutcome, with_budget};
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;

use super::columnar::materialize_columnar_collection;
use super::document::materialize_document_collection;
use super::kv::materialize_kv_collection;
use super::progress::CloneMaterializerHandle;
use super::rls_gate::refuse_if_rls_policy_applies;

/// Result of a single `materialize_database` call.
#[derive(Debug)]
pub enum MaterializeOutcome {
    /// Every clone collection in the database is `Materialized`.
    AllComplete,
    /// `n` collections still need work and the caller should reschedule.
    Incomplete { collections_remaining: usize },
    /// The maintenance budget was exhausted before any work could be done.
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
pub fn materialize_database(params: MaterializeParams<'_>) -> crate::Result<MaterializeOutcome> {
    if params.cancel.load(Ordering::Relaxed) {
        return Ok(MaterializeOutcome::Cancelled);
    }

    let outcome = with_budget(
        &params.state.maintenance_budget,
        params.db_id,
        params.estimated_secs,
        || do_materialize_database(&params),
    );

    match outcome {
        MaintenanceOutcome::Deferred => Ok(MaterializeOutcome::BudgetDeferred),
        MaintenanceOutcome::Ran(inner) => inner,
    }
}

/// Inner sweep, runs inside the budget window.
fn do_materialize_database(params: &MaterializeParams<'_>) -> crate::Result<MaterializeOutcome> {
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

    // Freeze every distinct source database referenced by the pending
    // collections for the duration of this sweep.  This prevents concurrent
    // user writes from leaking into the KV materializer copy path (KV has no
    // MVCC).  Guards are held until `_freeze_guards` drops at end of scope.
    let source_db_ids: HashSet<DatabaseId> = pending
        .iter()
        .filter_map(|c| c.cloned_from.as_ref().map(|o| o.source_database))
        .collect();
    let _freeze_guards: Vec<crate::control::clone::FreezeGuard> = source_db_ids
        .iter()
        .map(|db_id| params.state.materialize_freeze.freeze(*db_id))
        .collect();

    let runtime_handle = tokio::runtime::Handle::try_current().map_err(|_| {
        // Materializer must run inside a Tokio runtime so SPSC dispatch
        // futures can drive. The DDL hot path runs sync handlers on runtime
        // worker threads; the background sweep runs sync handlers on
        // `spawn_blocking` threads. Both share the runtime.
        crate::Error::Dispatch {
            detail: "clone materializer requires a Tokio runtime context".into(),
        }
    })?;

    let mut remaining = 0usize;
    for coll in &pending {
        if params.cancel.load(Ordering::Relaxed) {
            return Ok(MaterializeOutcome::Cancelled);
        }
        match materialize_one(&runtime_handle, params, coll) {
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

/// Route one collection to its per-engine materializer.
///
/// The per-engine implementations are async (they dispatch through the SPSC
/// bridge). We bridge sync↔async with [`tokio::task::block_in_place`] so the
/// runtime worker stays usable while we drive the future on this thread. This
/// requires a multi-threaded runtime — the production server uses
/// `#[tokio::main]` (multi-thread by default) and tests must annotate with
/// `#[tokio::test(flavor = "multi_thread")]`.
///
/// Every engine's materialization flows through this function, so the RLS
/// policy-existence gate is checked once here, before any scan or write plan
/// is built for any of the four write sites (KV `Put`, Document
/// `PointInsert`, Columnar `Insert`, Timeseries `Ingest`) or their matching
/// source-side `MaterializeScan`s.
fn materialize_one(
    runtime: &tokio::runtime::Handle,
    params: &MaterializeParams<'_>,
    coll: &StoredCollection,
) -> crate::Result<()> {
    refuse_if_rls_policy_applies(params.state, params.db_id, coll)?;

    match &coll.collection_type {
        CollectionType::KeyValue(_) => tokio::task::block_in_place(|| {
            runtime.block_on(materialize_kv_collection(
                params.state,
                params.catalog,
                params.db_id,
                coll,
            ))
        }),
        CollectionType::Document(_) => tokio::task::block_in_place(|| {
            runtime.block_on(materialize_document_collection(
                params.state,
                params.catalog,
                params.db_id,
                coll,
            ))
        }),
        CollectionType::Columnar(_) => tokio::task::block_in_place(|| {
            runtime.block_on(materialize_columnar_collection(
                params.state,
                params.catalog,
                params.db_id,
                coll,
            ))
        }),
    }
}

/// Drive materialization to completion synchronously.
///
/// Used by `ALTER DATABASE … MATERIALIZE` and `DROP DATABASE … FORCE`. Returns
/// `Err(Error::BadRequest)` for unsupported engines (mapped to SQLSTATE
/// `0A000` by the DDL handlers); returns `Ok(())` on success or after the
/// budget defers.
pub fn force_materialize_blocking(
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

    match do_materialize_database(&params)? {
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
pub fn run_scheduled_sweep(
    state: &SharedState,
    catalog: &SystemCatalog,
    cancel: &AtomicBool,
) -> crate::Result<()> {
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

        match materialize_database(params) {
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
