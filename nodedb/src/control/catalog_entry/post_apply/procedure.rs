// SPDX-License-Identifier: BUSL-1.1

//! Procedure post-apply side effects — same block-cache
//! invalidation pattern as Function.

use std::sync::Arc;

use crate::control::security::catalog::procedure_types::StoredProcedure;
use crate::control::state::SharedState;

pub fn put(proc: StoredProcedure, shared: Arc<SharedState>) {
    shared.block_cache.clear();
    super::owner::install_from_parent_in_database(
        "procedure",
        proc.database_id.as_u64(),
        proc.tenant_id,
        &proc.name,
        &proc.owner,
        &shared,
    );
}

pub fn delete(
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    name: String,
    shared: Arc<SharedState>,
) {
    shared.block_cache.clear();
    shared
        .permissions
        .install_replicated_remove_owner_in_database(
            "procedure",
            database_id.as_u64(),
            tenant_id,
            &name,
        );
}
