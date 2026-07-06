// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructor and catalog gating for the protocol-neutral
//! graph-ops handlers (and `match_ops`, a sibling of this module's parent).

use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// Preserves the exact SQLSTATE / message the pgwire graph-ops handlers
/// produced (via `sqlstate_error`), so error parity stays byte-identical after
/// the migration off the pgwire router.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Gate a named collection on catalog `is_active`: a plain `DROP COLLECTION`
/// (no PURGE) only flips `is_active=false` in the catalog and does not
/// reclaim edges/CSR, so graph reads must independently hide it until UNDROP
/// or a hard purge. Mirrors base-engine `SELECT ... FROM c` behavior on a
/// soft-dropped collection (not-found/deactivated).
///
/// Shared by `SHOW GRAPH STATS '<collection>'`, `GRAPH RAG FUSION`, and
/// `MATCH ... IN c` so the not-found / deactivated semantics can't drift
/// between the three call sites.
pub(crate) fn ensure_collection_active(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> Result<(), DdlError> {
    let catalog = match state.credentials.catalog() {
        Some(c) => c,
        None => return Err(ddl_err("XX000", "catalog not available")),
    };
    match catalog.get_collection(database_id, tenant_id, name) {
        Ok(Some(c)) if c.is_active => Ok(()),
        Ok(Some(_)) => Err(ddl_err(
            "42P01",
            format!("collection '{name}' is deactivated"),
        )),
        _ => Err(ddl_err("42P01", format!("collection '{name}' not found"))),
    }
}
