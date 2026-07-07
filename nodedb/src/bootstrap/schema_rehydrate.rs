// SPDX-License-Identifier: BUSL-1.1

//! Boot-time authoritative rehydration of the Data Plane per-core schema
//! registry from the durable catalog.
//!
//! The per-core `doc_configs` registry (populated by `DocumentOp::Register`)
//! is in-memory only. On restart it starts empty; nothing in single-node
//! mode ever re-populates it, and in cluster mode it is only populated as
//! an unreliable fire-and-forget side effect of raft log replay. This
//! leaves strict-mode collections unable to decode their schema after a
//! restart, degrading `SELECT *` to a raw `(id, data)` tuple.
//!
//! [`rehydrate_schema_registry`] closes this gap: it enumerates every
//! active collection in every database from the durable catalog and
//! re-registers each one to all Data Plane cores, awaited and
//! fail-closed, in both single-node and cluster mode.

use std::collections::HashSet;
use std::sync::Arc;

use tracing::info;

use crate::bootstrap::constraint_reconcile::load_collections;
use crate::control::server::shared::ddl::neutral::collection::dispatch_register_from_stored;
use crate::control::state::SharedState;

/// Re-register every active stored collection to all Data Plane cores.
///
/// Returns `Ok(())` immediately if the catalog is not yet initialized
/// (fresh boot before catalog init — nothing persisted yet). On the first
/// registration failure, returns `Err` immediately without attempting the
/// remaining collections: an unregistered strict schema after restart is a
/// data-loss-shaped bug, so this path is fail-closed rather than
/// warn-and-continue.
///
/// Database enumeration (including the implicit `DatabaseId::DEFAULT`,
/// which carries no descriptor row in `_system.databases`) is shared with
/// the constraint-reconcile loop via [`load_collections`].
pub async fn rehydrate_schema_registry(shared: &Arc<SharedState>) -> anyhow::Result<()> {
    let catalog = shared.credentials.catalog();

    let all = load_collections(catalog)
        .map_err(|e| anyhow::anyhow!("schema rehydration: failed to load collections: {e}"))?;

    let mut databases = HashSet::new();
    let mut rehydrated = 0usize;
    for (db_id, coll) in &all {
        databases.insert(*db_id);
        if !coll.is_active {
            continue;
        }
        dispatch_register_from_stored(shared, coll)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "schema rehydration: failed to re-register collection \
                     (database {db_id:?}): {e}"
                )
            })?;
        rehydrated += 1;
    }

    info!(
        collections = rehydrated,
        databases = databases.len(),
        "schema registry rehydrated from durable catalog"
    );
    Ok(())
}

// No unit test here: even the `None`-catalog early-return path requires
// constructing a `SharedState`, which needs a live Data Plane / redb
// catalog wiring that this module cannot build in isolation. Coverage for
// both the early-return and populated-catalog paths belongs in an
// integration test exercising a full restart cycle.
