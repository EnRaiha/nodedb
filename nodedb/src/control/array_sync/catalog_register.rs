// SPDX-License-Identifier: BUSL-1.1

//! Shared `array_catalog` registration helper.
//!
//! Split out of `raft_apply.rs` (which was pushing the 500-line file-size
//! limit) so both array-schema-import codepaths — the Raft-apply path
//! ([`crate::control::array_sync::raft_apply::apply_array_schema`], run on
//! every replica after Raft commit) and the single-node direct-import path
//! (`OriginArrayInbound::handle_schema`'s no-cluster branch, which never
//! goes through Raft) — converge on one registration routine instead of
//! duplicating (and risking drift between) the catalog-entry construction.

use std::sync::Arc;

use crate::control::array_catalog::entry::ArrayCatalogEntry;
use crate::control::state::SharedState;

/// Register (or no-op if already present) an [`ArrayCatalogEntry`] for
/// `array` by reading back its just-imported schema from
/// `state.array_sync_schemas`.
///
/// Without this call, a synced array's schema lands in `array_sync_schemas`
/// but the array never becomes openable by the Data Plane
/// (`ensure_array_open` looks it up in `array_catalog`) and never becomes
/// visible to system-catalog introspection (`SHOW COLLECTIONS` merges in
/// `array_catalog::all_entries()`).
///
/// Returns `Ok(())` when an entry already exists (benign no-op) or was
/// freshly registered. Returns `Err` on a genuine registration failure
/// (schema not readable back, encode failure, or catalog write error) —
/// the caller decides whether to propagate (single-node direct-import path,
/// which can still fail the request back to the sync sender) or to
/// warn-and-continue (Raft-apply path, which has already committed the
/// schema import durably and runs in a fire-and-forget apply loop that
/// cannot fail back; a missing entry there is caught by the next
/// `ensure_array_open` lookup failure or by drift detection instead).
pub(crate) fn register_array_catalog_entry(
    state: &Arc<SharedState>,
    array: &str,
) -> crate::Result<()> {
    use nodedb_types::TenantId as NdTenantId;

    let schema = state.array_sync_schemas.to_array_schema(array).ok_or_else(|| {
        crate::Error::Internal {
            detail: format!(
                "register_array_catalog_entry: to_array_schema returned None for '{array}' after import"
            ),
        }
    })?;
    let schema_msgpack = zerompk::to_msgpack_vec(&schema).map_err(|e| crate::Error::Internal {
        detail: format!("register_array_catalog_entry: schema_msgpack encode failed: {e}"),
    })?;

    let array_id = nodedb_array::types::ArrayId::new(NdTenantId::new(0), array);
    let entry = ArrayCatalogEntry {
        array_id,
        name: array.to_string(),
        schema_msgpack,
        schema_hash: 0,
        created_at_ms: 0,
        prefix_bits: 8,
        audit_retain_ms: None,
        minimum_audit_retain_ms: None,
    };
    let mut cat = state
        .array_catalog
        .write()
        .unwrap_or_else(|p| p.into_inner());
    if cat.lookup_by_name(array).is_none() {
        cat.register(entry).map_err(|e| crate::Error::Internal {
            detail: format!("register_array_catalog_entry: catalog register failed: {e}"),
        })?;
    }
    Ok(())
}
