// SPDX-License-Identifier: BUSL-1.1

//! Synonym group post-apply side effects — sync the in-memory registry.

use std::sync::Arc;

use crate::control::security::catalog::StoredSynonymGroup;
use crate::control::state::SharedState;

pub fn put(stored: StoredSynonymGroup, shared: Arc<SharedState>) {
    shared.synonym_registry.register(stored);
}

pub fn delete(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    shared
        .synonym_registry
        .unregister(database_id, tenant_id, &name);
}
