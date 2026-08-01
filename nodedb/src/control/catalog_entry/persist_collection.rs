// SPDX-License-Identifier: BUSL-1.1

//! The one way a mutated `StoredCollection` reaches durable storage.

use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::CatalogEntry;

/// Persist a mutated `StoredCollection` through the replicated metadata path.
///
/// A bare `catalog.put_collection` is never correct for a descriptor the
/// replicated catalog also owns, for two independent reasons:
///
/// * **Divergence** — the write lands only on the node that made it, so any
///   node coordinating a later operation reads a different descriptor.
/// * **A wedged apply loop** — the propose path stamps `descriptor_version` as
///   `prior + 1` and the apply path enforces that a given version always
///   carries the same bytes. Mutating the persisted record in place leaves the
///   local copy at version N no longer byte-equal to the replicated entry at
///   version N, so replaying that entry after a restart raises
///   `DescriptorVersionAnomaly`. The metadata applier treats that as a durable
///   apply failure and stops advancing its watermark — permanently. Every
///   later metadata operation on the node, descriptor leases included, then
///   times out, which presents as a database that starts cleanly and fails
///   every query.
///
/// `log_index == 0` means no metadata raft handle (single-node or
/// mixed-version compat mode); the applier is bypassed there, so the caller's
/// record is written through locally — mirroring the DDL handlers.
pub fn persist_collection_replicated(
    state: &SharedState,
    database_id: DatabaseId,
    coll: &StoredCollection,
) -> crate::Result<()> {
    let entry = CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)?;
    if log_index == 0 {
        state
            .credentials
            .catalog()
            .put_collection(database_id, coll)?;
    }
    Ok(())
}
