// SPDX-License-Identifier: BUSL-1.1

//! Replicate one copy-on-write catalog row through the metadata log.
//!
//! Every node applies the entry, so a clone read on any node sees the same
//! copy-ups and tombstones, and the materializer on one node sees them all.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::state::SharedState;

/// Propose `entry` and wait until this node applied it, on any runtime
/// flavor. Inside an open transaction the entry commits with it.
pub(crate) async fn replicate_async(
    state: &SharedState,
    entry: &CatalogEntry,
) -> crate::Result<()> {
    propose_catalog_entry_async(state, entry).await?;
    Ok(())
}
