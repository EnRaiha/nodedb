// SPDX-License-Identifier: BUSL-1.1

//! Streaming materialized-view post-apply registry cleanup.

use std::sync::Arc;

use crate::control::security::catalog::auth_types::object_type;
use crate::control::state::SharedState;
use crate::event::streaming_mv::StreamingMvDef;
use crate::types::DatabaseId;

pub fn put(definition: StreamingMvDef, shared: Arc<SharedState>) {
    shared
        .permissions
        .install_replicated_owner(&crate::control::security::catalog::StoredOwner {
            database_id: definition.database_id.as_u64(),
            object_type: object_type::STREAMING_MATERIALIZED_VIEW.to_string(),
            object_name: definition.name.clone(),
            tenant_id: definition.tenant_id,
            owner_username: definition.owner.clone(),
        });
    shared.mv_registry.register(definition);
}

pub fn delete(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    let database_id = DatabaseId::new(database_id);
    shared.mv_registry.unregister(database_id, tenant_id, &name);
    shared
        .permissions
        .install_replicated_remove_owner_in_database(
            object_type::STREAMING_MATERIALIZED_VIEW,
            database_id.as_u64(),
            tenant_id,
            &name,
        );
}
