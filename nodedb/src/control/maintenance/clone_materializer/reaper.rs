// SPDX-License-Identifier: BUSL-1.1

//! Post-copy cleanup. Reaps `clone_copyups` and `clone_tombstones` (both
//! surrogate-keyed and KV-key-keyed) catalog rows for a collection that has
//! just been fully materialized, then flips the collection's `clone_status`
//! to `Materialized` and clears `cloned_from`.
//!
//! Idempotent: calling on a collection already in `Materialized` is a no-op.

use nodedb_types::{CloneStatus, DatabaseId};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

/// Parameters for reaping a single fully-copied clone collection.
pub struct ReapParams<'a> {
    /// `db_qualified(target_db_id, name)` — same key the CoW write helpers
    /// use when stamping copyups/tombstones (see `control::clone::tombstone`).
    pub target_collection_qualified: &'a str,
    pub db_id: DatabaseId,
    pub tenant_id: u64,
    pub name: &'a str,
    pub state: &'a SharedState,
    pub catalog: &'a SystemCatalog,
}

/// Reap CoW auxiliary rows and flip the collection to `Materialized`.
///
/// Order matters for crash safety: aux rows are deleted first (idempotent).
/// A crash after aux delete but before status flip is recovered on the
/// next sweep — re-deletion is a no-op, and the status flip then proceeds.
/// A crash before aux delete leaves both the rows and the pre-flip status
/// in place — the next sweep redoes the whole step. The status-flip + clear
/// `cloned_from` is one Raft proposal, so it commits atomically.
pub fn reap_materialized_collection(params: ReapParams<'_>) -> crate::Result<()> {
    let ReapParams {
        target_collection_qualified,
        db_id,
        tenant_id,
        name,
        state,
        catalog,
    } = params;

    catalog.delete_all_clone_copyups_for_collection(target_collection_qualified)?;
    catalog.delete_all_clone_tombstones_for_collection(target_collection_qualified)?;

    let Some(mut desc) = catalog.get_collection(db_id, tenant_id, name)? else {
        // Collection was concurrently dropped — nothing to reap.
        return Ok(());
    };

    if desc.clone_status == CloneStatus::Materialized {
        return Ok(());
    }

    desc.clone_status = CloneStatus::Materialized;
    // Clearing `cloned_from` is belt-and-suspenders: `clone::resolver` already
    // short-circuits on `Materialized`, but a `None` origin lets the read-path
    // fast path skip the `cloned_from` lookup entirely.
    desc.cloned_from = None;

    let outcome =
        propose_catalog_entry(state, &CatalogEntry::PutCollection(Box::new(desc.clone())))?;
    if outcome.needs_local_apply() {
        catalog.put_collection(db_id, &desc)?;
    }

    Ok(())
}
