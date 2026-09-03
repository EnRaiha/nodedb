// SPDX-License-Identifier: BUSL-1.1

//! Custom type post-apply side effects — sync the in-memory registry.

use std::sync::Arc;

use crate::control::security::catalog::StoredCustomType;
use crate::control::state::SharedState;

pub fn put(stored: StoredCustomType, shared: Arc<SharedState>) {
    shared.custom_type_registry.register(stored);
}

pub fn delete(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    shared
        .custom_type_registry
        .unregister(database_id, tenant_id, &name);
}
