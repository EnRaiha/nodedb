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

/// Union ingest-inferred fields into a collection's schema projection and
/// persist the result through the replicated metadata path.
///
/// Returns `true` when the projection changed and a new descriptor version was
/// proposed, `false` when the collection is absent or already carries every
/// inferred field (the overwhelmingly common case on a steady ingest stream —
/// no proposal is made).
///
/// The schema projection is rebuildable control-plane state, but the record it
/// lives in is not: it is the replicated collection descriptor. Writing the
/// merged fields straight to local redb would satisfy the projection and break
/// the descriptor — see the divergence and wedged-apply-loop reasoning on
/// [`persist_collection_replicated`]. Going through the proposer instead makes
/// the merge a real descriptor version, which is both replicated to every node
/// and replay-safe.
///
/// Read-modify-propose is not atomic the way a single redb transaction was, so
/// two ingest flushes inferring different new fields can race and one union can
/// be lost. That is self-healing rather than a durability hole: every ILP batch
/// re-supplies the full field set for its measurement, so the next batch
/// carrying the dropped field merges it again. Trading that for a descriptor
/// that is byte-stable at a given version is the right side of the deal — the
/// alternative loses the whole node.
pub fn merge_collection_fields_replicated(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
    inferred_fields: &[(String, String)],
) -> crate::Result<bool> {
    let catalog = state.credentials.catalog();
    let Some(mut coll) = catalog.get_collection(database_id, tenant_id, name)? else {
        return Ok(false);
    };
    if !crate::control::security::catalog::merge_inferred_fields(&mut coll, inferred_fields) {
        return Ok(false);
    }
    persist_collection_replicated(state, database_id, &coll)?;
    Ok(true)
}
