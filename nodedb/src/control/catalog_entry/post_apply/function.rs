// SPDX-License-Identifier: BUSL-1.1

//! Function post-apply side effects — clear the parsed block
//! cache so the next call re-parses the new body.

use crate::control::security::catalog::function_types::StoredFunction;
use crate::control::state::SharedState;

pub fn put(func: StoredFunction, shared: &SharedState) {
    // The block cache is keyed by body-SQL hash, not (tenant,
    // name), so point invalidation isn't possible. Clearing the
    // whole cache mirrors PostgreSQL's "any DDL invalidates
    // prepared plans" behavior — cache is small, reparse is cheap.
    shared.block_cache.clear();
    super::owner::install_from_parent_in_database(
        "function",
        func.database_id.as_u64(),
        func.tenant_id,
        &func.name,
        &func.owner,
        shared,
    );
}

pub fn delete(
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    name: String,
    shared: &SharedState,
) {
    shared.block_cache.clear();
    shared
        .permissions
        .install_replicated_remove_owner_in_database(
            "function",
            database_id.as_u64(),
            tenant_id,
            &name,
        );
}
