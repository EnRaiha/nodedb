// SPDX-License-Identifier: BUSL-1.1

//! MaterializedView post-apply — no in-memory registry to sync.
//! The refresh loop reads the view definition straight from
//! `SystemCatalog` on its next tick.

use std::sync::Arc;

use tracing::debug;

use crate::control::security::catalog::StoredMaterializedView;
use crate::control::state::SharedState;

pub fn put(stored: StoredMaterializedView, shared: Arc<SharedState>) {
    debug!(
        view = %stored.name,
        database = stored.database_id,
        tenant = stored.tenant_id,
        "catalog_entry: materialized view upserted (refresh loop will pick it up)"
    );
    super::owner::install_from_parent_in_database(
        "materialized_view",
        stored.database_id,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
}

pub fn delete(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    debug!(
        view = %name,
        database = database_id,
        tenant = tenant_id,
        "catalog_entry: materialized view removed"
    );
    shared
        .permissions
        .install_replicated_remove_owner_in_database(
            "materialized_view",
            database_id,
            tenant_id,
            &name,
        );
}
