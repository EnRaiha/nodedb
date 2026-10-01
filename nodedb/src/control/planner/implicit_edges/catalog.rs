// SPDX-License-Identifier: BUSL-1.1

//! Edge-bearing collection flag bookkeeping in the system catalog.

use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Mark a collection as edge-bearing in the system catalog (idempotent).
///
/// Sets [`StoredCollection::has_implicit_edges`] to `true` the first time an
/// edge (implicit `_from`/`_to` document, or explicit `GRAPH INSERT EDGE`) is
/// written into `collection`. This is the routing gate for implicit-edge
/// DELETE / UPDATE cleanup — see `has_implicit_edges`'s doc comment.
///
/// Read-then-conditional-write: if the collection is already flagged the write
/// is SKIPPED, so the common steady-state insert path issues zero catalog
/// proposals (only the very first edge into a fresh collection pays the cost).
/// If the catalog is unavailable or the collection row is absent, this is a
/// no-op `Ok(())` — flag bookkeeping must never fail a write that otherwise
/// succeeds. A genuine propose/put error IS propagated (not swallowed).
///
/// The flag is committed via the REPLICATED metadata path
/// (`propose_catalog_entry` → `CatalogEntry::PutCollection`), exactly like
/// CREATE/ALTER COLLECTION. A bare local `put_collection` will only update the
/// proposing node's catalog, so a DELETE coordinated on a different node will
/// not observe the flag and will skip implicit-edge cleanup. The replicated
/// path prevents that.
pub async fn mark_collection_edge_bearing(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> crate::Result<()> {
    // Callers pass the plan's database-qualified name or the bare DDL name.
    // The catalog keys collections by the bare name.
    let bare =
        crate::control::target_identity::naming::bare_collection_name(database_id, collection);
    let catalog = state.credentials.catalog();
    let Some(mut coll) = catalog.get_collection(database_id, tenant_id.as_u64(), &bare)? else {
        // Collection row absent — don't fail the write over flag bookkeeping.
        return Ok(());
    };
    if coll.has_implicit_edges {
        // Already flagged — skip the proposal entirely.
        return Ok(());
    }
    coll.has_implicit_edges = true;

    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll));
    crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry).await?;
    Ok(())
}
