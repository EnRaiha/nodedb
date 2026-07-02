// SPDX-License-Identifier: BUSL-1.1

//! Existence-check helpers used by IF EXISTS / IF NOT EXISTS guards.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use nodedb_types::DatabaseId;

pub(super) fn collection_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database_id: DatabaseId,
) -> bool {
    let Some(catalog) = state.credentials.catalog() else {
        return false;
    };
    let tid = identity.tenant_id.as_u64();
    matches!(catalog.get_collection(database_id, tid, name), Ok(Some(_)))
}
