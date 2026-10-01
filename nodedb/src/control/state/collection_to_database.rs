// SPDX-License-Identifier: BUSL-1.1

//! Collection-to-database reverse mapping.
//!
//! Maps `(TenantId, collection_name)` → `DatabaseId` so the Event Plane
//! consumer can look up which database a collection belongs to without a
//! catalog round-trip on every write event.
//!
//! Updated synchronously when collections are created or dropped.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_types::DatabaseId;

use crate::types::TenantId;

type Key = (TenantId, Arc<str>);

/// In-memory reverse mapping from collection to owning database.
///
/// Control Plane only (`Send + Sync`).
pub struct CollectionToDatabase {
    inner: RwLock<HashMap<Key, DatabaseId>>,
}

impl CollectionToDatabase {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Look up which database a `(tenant_id, collection_name)` pair belongs to.
    ///
    /// Returns `None` if the collection is not registered (e.g., was created
    /// before this cache was wired in, or belongs to a database without active
    /// DML auditing — either is safe to skip).
    pub fn lookup(&self, tenant_id: TenantId, collection: &str) -> Option<DatabaseId> {
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(tenant_id, Arc::from(collection)))
            .copied()
    }

    /// Register a collection-to-database mapping.
    pub fn insert(&self, tenant_id: TenantId, collection: Arc<str>, db_id: DatabaseId) {
        self.inner
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert((tenant_id, collection), db_id);
    }

    /// Remove a collection-to-database mapping on collection drop.
    pub fn remove(&self, tenant_id: TenantId, collection: &str) {
        self.inner
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(tenant_id, Arc::from(collection)));
    }

    /// Replace the cache with the reverse map of every database's
    /// collections in the catalog.
    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::SystemCatalog,
    ) -> crate::Result<()> {
        let mut fresh = HashMap::new();
        for descriptor in catalog.list_databases()? {
            for collection in catalog.load_all_collections(descriptor.id)? {
                let tenant_id = TenantId::new(collection.tenant_id);
                fresh.insert(
                    (tenant_id, Arc::from(collection.name.as_str())),
                    descriptor.id,
                );
            }
        }
        *self.inner.write().unwrap_or_else(|p| p.into_inner()) = fresh;
        Ok(())
    }
}

impl Default for CollectionToDatabase {
    fn default() -> Self {
        Self::new()
    }
}
