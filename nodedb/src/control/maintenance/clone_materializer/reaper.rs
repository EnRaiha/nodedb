// SPDX-License-Identifier: BUSL-1.1

//! Post-copy cleanup: flip a fully copied clone collection to `Materialized`
//! and clear `cloned_from`.
//!
//! The flip is one replicated `PutCollection`. Its apply drops each node's
//! copy-up and tombstone rows of the collection before it writes the row, so
//! every node reaps its own copy-on-write state at the same log position.
//!
//! Idempotent: calling on a collection already in `Materialized` is a no-op.

use nodedb_types::{CloneStatus, DatabaseId};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

/// Parameters for reaping a single fully-copied clone collection.
pub struct ReapParams<'a> {
    pub db_id: DatabaseId,
    pub tenant_id: u64,
    pub name: &'a str,
    pub state: &'a SharedState,
    pub catalog: &'a SystemCatalog,
}

/// Flip the collection to `Materialized` and reap its CoW rows on every node.
///
/// A crash before the flip commits leaves the pre-flip status, and the next
/// sweep redoes the step.
pub async fn reap_materialized_collection(params: ReapParams<'_>) -> crate::Result<()> {
    let ReapParams {
        db_id,
        tenant_id,
        name,
        state,
        catalog,
    } = params;

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

    propose_catalog_entry_async(state, &CatalogEntry::PutCollection(Box::new(desc))).await?;
    Ok(())
}
